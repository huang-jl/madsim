use super::rpc::{Bytes, Request};
use crate::{task, time::Duration};
use bytes::{BufMut, BytesMut};
use erased_serde::Serialize;
use futures::{Future, FutureExt};
use log::{info, warn};
use mad_rpc::{
    transport::{self, Transport},
    ud::VerbsTransport,
};
use std::{
    collections::HashMap,
    io::{self, IoSlice, Write},
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs},
    sync::{oneshot, Mutex as AsyncMutex},
};

// The origin rpc implementation in mad_rpc is not suitable for porting to madsim directly.
// The main logic is inspired by [`mad_rpc::rpc`] and [`madsim::net::tcp`]

#[derive(Clone)]
pub struct Endpoint {
    inner: Option<Arc<Inner>>,
    init_lock: Arc<AsyncMutex<Option<SocketAddr>>>,
}

struct Inner {
    addr: SocketAddr,
    transport: Mutex<VerbsTransport<Endpoint>>,
    /// mappings from SocketAddr to EndpointID in erpc
    mappings: AsyncMutex<HashMap<SocketAddr, u32>>,
    tasks: Mutex<Vec<task::JoinHandle<()>>>,
    // RPC ID -> Handler
    handlers: RwLock<HashMap<u64, RpcHandler>>,
    // RequestId -> oneshot::Sender
    waiters: Mutex<HashMap<u64, oneshot::Sender<RecvMsg>>>,
    req_id: AtomicU64,
}

type RpcHandler = Box<dyn Fn(Arc<Inner>, RecvMsg) + Send + Sync>;

#[derive(Debug)]
struct MsgHeader {
    // whether request or response => use to determine give it to handler or just send to waiter
    is_req: bool,
    tag: u64,    // RPC ID
    req_id: u64, // Request ID, match a specific send and recv
    arg_len: u32,
    data_len: u32,
}

pub struct SendMsg<'a> {
    header: MsgHeader,
    // Option here is used for detecting whether unpack msg has started
    arg: Option<&'a (dyn Serialize + Sync)>,
    data: &'a [u8],
}

#[derive(Debug)]
pub struct RecvMsg {
    #[allow(dead_code)]
    ep_id: u32,
    header: MsgHeader,
    // Option here is used for detecting whether unpack msg has started
    data: Option<BytesMut>,
}

fn mad_rpc_err_to_io_err(err: mad_rpc::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{err:?}"))
}

impl MsgHeader {
    // format: [is_req: u8, tag: u64, req_id: u64, arg_len:u32, data_len:u32]
    fn serialize(&self, mut buf: &mut [u8]) -> usize {
        let mut len = 0;
        len += buf.write(&[self.is_req as u8]).unwrap();
        len += buf.write(&self.tag.to_be_bytes()).unwrap();
        len += buf.write(&self.req_id.to_be_bytes()).unwrap();
        len += buf.write(&self.arg_len.to_be_bytes()).unwrap();
        len += buf.write(&self.data_len.to_be_bytes()).unwrap();
        assert_eq!(len, 25);
        len
    }

    fn deserialize(data: &[u8]) -> (Self, usize) {
        let is_req = data[0] != 0;
        let tag = u64::from_be_bytes(data[1..9].try_into().unwrap());
        let req_id = u64::from_be_bytes(data[9..17].try_into().unwrap());
        let arg_len = u32::from_be_bytes(data[17..21].try_into().unwrap());
        let data_len = u32::from_be_bytes(data[21..25].try_into().unwrap());
        (
            MsgHeader {
                is_req,
                tag,
                req_id,
                arg_len,
                data_len,
            },
            25,
        )
    }
}

impl<'a> SendMsg<'a> {
    fn new<R: Serialize + Sync>(
        is_req: bool,
        tag: u64,
        req_id: u64,
        arg: &'a R,
        data: &'a [u8],
    ) -> Self {
        Self {
            header: MsgHeader {
                is_req,
                tag,
                req_id,
                arg_len: 0,
                data_len: 0,
            },
            arg: Some(arg),
            data,
        }
    }
}

impl RecvMsg {
    pub fn new(ep_id: u32) -> Self {
        RecvMsg {
            ep_id,
            header: MsgHeader {
                is_req: false,
                tag: 0,
                req_id: 0,
                arg_len: 0,
                data_len: 0,
            },
            data: None,
        }
    }

    #[inline]
    fn take(mut self) -> (Bytes, Bytes) {
        let data = self.data.take().unwrap().freeze();
        let arg_len = self.header.arg_len as usize;
        let resp_arg = data.slice(..arg_len);
        let resp_data = data.slice(arg_len..);
        assert_eq!(resp_data.len(), self.header.data_len as usize);
        (resp_arg, resp_data)
    }
}

impl<'a> transport::SendMsg for SendMsg<'a> {
    #[inline]
    fn pack(&mut self, mut buf: &mut [u8]) -> (usize, bool) {
        struct SliceWriter<'a> {
            slice: &'a mut [u8],
            len: usize,
        }

        impl<'a> SliceWriter<'a> {
            pub fn new(slice: &'a mut [u8]) -> Self {
                SliceWriter { slice, len: 0 }
            }

            pub fn len(&self) -> usize {
                self.len
            }
        }

        impl<'a> Write for SliceWriter<'a> {
            #[inline]
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let len = self.slice.write(buf)?;
                self.len += len;
                Ok(len)
            }

            #[inline]
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut header_len = 0;
        if let Some(arg) = self.arg.take() {
            let mut writer = SliceWriter::new(&mut buf[25..]);
            bincode::serialize_into(&mut writer, arg).unwrap();

            self.header.arg_len = writer.len() as _;
            self.header.data_len = self.data.len() as _;
            header_len += writer.len();
            header_len += self.header.serialize(&mut buf[..25]);
            buf = &mut buf[header_len..];
        }

        let data_len = buf.len().min(self.data.len());
        buf[..data_len].copy_from_slice(&self.data[..data_len]);
        self.data = &self.data[data_len..];

        (data_len + header_len, !self.data.is_empty())
    }
}

impl transport::RecvMsg for RecvMsg {
    #[inline]
    fn unpack(&mut self, mut pkt: &[u8]) -> bool {
        if self.data.is_none() {
            let (header, len) = MsgHeader::deserialize(pkt);
            self.header = header;
            let capacity = self.header.data_len as usize + self.header.arg_len as usize;
            self.data = Some(BytesMut::with_capacity(capacity));
            pkt = &pkt[len..];
        }
        let buf = unsafe { self.data.as_mut().unwrap_unchecked() };
        buf.put(pkt);

        false
    }
}

impl transport::Context for Endpoint {
    type SendMsg = SendMsg<'static>;

    type RecvMsg = RecvMsg;

    #[inline]
    fn accept(&mut self, _addr: &str, _ep_id: u32) {
        // todo: notify upper layer
    }

    fn msg_begin(&mut self, ep_id: u32) -> Self::RecvMsg {
        RecvMsg::new(ep_id)
    }

    fn msg_end(&mut self, msg: Self::RecvMsg) {
        if msg.header.is_req {
            // Recv a request, transfer to handlers
            self.inner().handle_request(self.inner().clone(), msg)
        } else {
            // Recv a response, transfer to waiters
            self.inner().handle_response(msg)
        }
    }
}

impl Inner {
    /// Create a RpcInner bind to `addr` and `dev`
    fn new(addr: SocketAddr, dev: &str) -> io::Result<Self> {
        Ok(Self {
            addr,
            transport: VerbsTransport::new_verbs(dev).map_err(mad_rpc_err_to_io_err)?,
            mappings: AsyncMutex::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
            handlers: RwLock::new(HashMap::new()),
            waiters: Mutex::new(HashMap::new()),
            req_id: AtomicU64::new(0),
        })
    }

    fn url(&self) -> String {
        self.transport.addr()
    }

    async fn rpc_request<'a, R: Request>(
        &self,
        dst: impl ToSocketAddrs,
        req: &R,
        data: Option<&[u8]>,
    ) -> io::Result<RecvMsg> {
        struct WaiterGuard<'a>(&'a Inner, u64);
        impl<'a> Drop for WaiterGuard<'a> {
            fn drop(&mut self) {
                self.0.waiters.lock().unwrap().remove(&self.1);
            }
        }

        let dst = lookup_host(dst).await?.next().unwrap();
        let dst_ep_id = self.get_ep_id_or_connect(dst).await?;
        let req_id = self
            .req_id
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let (tx, rx) = oneshot::channel();
        let guard = WaiterGuard(self, req_id);
        let old_waiter = self.waiters.lock().unwrap().insert(req_id, tx);
        assert!(old_waiter.is_none());

        let msg = SendMsg::new(true, R::ID, req_id, req, data.unwrap_or_default());
        //Safety: data must live until send msg return
        self.transport
            .send(dst_ep_id, unsafe { std::mem::transmute(msg) })
            .await
            .map_err(mad_rpc_err_to_io_err)?;
        let msg = rx.await.unwrap();
        std::mem::forget(guard);

        Ok(msg)
    }

    async fn send_response<R: Request>(
        &self,
        ep_id: u32,
        req_id: u64,
        arg: &R::Response,
        data: Option<&[u8]>,
    ) -> io::Result<()> {
        let msg = SendMsg::new(false, R::ID, req_id, arg, data.unwrap_or_default());
        //Safety: data must live until send msg return
        self.transport
            .send(ep_id, unsafe { std::mem::transmute(msg) })
            .await
            .map_err(mad_rpc_err_to_io_err)?;
        Ok(())
    }

    fn handle_request(&self, inner: Arc<Inner>, msg: RecvMsg) {
        self.handlers
            .read()
            .unwrap()
            .get(&msg.header.tag)
            .expect("RPC handler not found")(inner, msg)
    }

    fn handle_response(&self, msg: RecvMsg) {
        if let Some(sender) = self.waiters.lock().unwrap().remove(&msg.header.req_id) {
            sender.send(msg).expect("send to waiters failed");
        } else {
            warn!(
                "request {} do not has waiters waiting for response",
                msg.header.req_id
            );
        }
    }

    fn register<H>(&self, rpc_id: u64, handler: H)
    where
        H: Fn(Arc<Inner>, RecvMsg) + Send + Sync + 'static,
    {
        let handler = Box::new(handler);
        self.handlers.write().unwrap().insert(rpc_id, handler);
    }

    /// Get the Endpoint Id of the remote rdma peer.
    /// If there has been no connection, try to establish one.
    async fn get_ep_id_or_connect(&self, peer: SocketAddr) -> io::Result<u32> {
        let mut mapping = self.mappings.lock().await;
        if let Some(ep_id) = mapping.get(&peer) {
            return Ok(*ep_id);
        }
        let mut stream = TcpStream::connect(peer).await?;
        let len = stream.read_u32().await? as usize;
        let mut buf = vec![0u8; len];
        let size = stream.read_exact(&mut buf).await?;
        assert_eq!(size, len);
        // url is the address of peer at the level of rdma
        let url = std::str::from_utf8(&buf).expect("Invalid utf-8 bytes receive from peers");
        let ep_id = self
            .transport
            .connect(url)
            .await
            .map_err(mad_rpc_err_to_io_err)?;
        mapping.insert(peer, ep_id);
        Ok(ep_id)
    }
}

impl Endpoint {
    /// Creates a [`Endpoint`] from the given address.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let addr = lookup_host(addr).await?.next().unwrap();
        let ep = Endpoint {
            inner: None,
            init_lock: Arc::new(AsyncMutex::new(Some(addr))),
        };
        Ok(ep)
    }

    pub async fn init(mut self, dev: &str) -> io::Result<Self> {
        let listener = {
            let mut guard = self.init_lock.lock().await;
            let addr = guard.take().expect("Duplicate Initialization");
            let listener = TcpListener::bind(addr).await?;
            let addr = listener.local_addr()?;
            self.inner = Some(Arc::new(Inner::new(addr, dev)?));
            listener
        };

        let ep_clone = self.clone();
        //polling
        // todo spwan blocking ?
        let polling_task = task::spawn(async move {
            // `ep_clone` accounts for 1 strong count
            // so if strong count of ep is > 1, means Endpoint still in use
            while ep_clone.strong_count() > 1 {
                ep_clone.progress();
                task::yield_now().await;
            }
        });

        let url = self.inner().url();
        // Connection Helper
        let connect_task = task::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_u32(url.as_bytes().len() as _).await.unwrap();
                stream.write_all(url.as_bytes()).await.unwrap();
            }
        });

        self.inner()
            .tasks
            .lock()
            .unwrap()
            .extend([polling_task, connect_task]);

        Ok(self)
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.inner().addr)
    }

    #[inline]
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.inner())
    }

    #[inline]
    pub fn progress(&self) {
        self.inner().transport.progress(&mut self.clone());
    }

    pub async fn send_to(
        &self,
        _dst: impl ToSocketAddrs,
        _tag: u64,
        _data: &[u8],
    ) -> io::Result<()> {
        unimplemented!("erpc do not support send raw data, try using rpc API")
    }

    pub async fn send_to_vectored<'a>(
        &self,
        _dst: impl ToSocketAddrs,
        _tag: u64,
        _bufs: &'a mut [IoSlice<'a>],
    ) -> io::Result<()> {
        unimplemented!("erpc do not support send raw data, try using rpc API")
    }

    pub async fn recv_from(&self, _tag: u64, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        unimplemented!("erpc do not support recv raw data, try using rpc API")
    }

    /// Receives a raw message.
    pub async fn recv_from_raw(&self, _tag: u64) -> io::Result<(Bytes, SocketAddr)> {
        unimplemented!("erpc do not support recv raw data, try using rpc API")
    }

    pub fn url(&self) -> String {
        self.inner().url()
    }

    #[inline]
    fn inner(&self) -> Arc<Inner> {
        self.inner
            .as_ref()
            .expect("Endpoint has not been init")
            .clone()
    }
}

impl Endpoint {
    /// Call function on a remote node with timeout.
    pub async fn call_timeout<R: Request>(
        &self,
        dst: SocketAddr,
        request: R,
        timeout: Duration,
    ) -> io::Result<R::Response> {
        crate::time::timeout(timeout, self.call(dst, request))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RPC timeout"))?
    }

    /// Call function on a remote node.
    pub async fn call<R: Request>(&self, dst: SocketAddr, request: R) -> io::Result<R::Response> {
        let (rsp, _data) = self.call_with_data(dst, request, &[]).await?;
        Ok(rsp)
    }

    /// Call function on a remote node.
    pub async fn call_with_data<R: Request>(
        &self,
        dst: SocketAddr,
        request: R,
        data: &[u8],
    ) -> io::Result<(R::Response, Bytes)> {
        let msg = self.inner().rpc_request(dst, &request, Some(data)).await?;
        let (resp, resp_data) = msg.take();
        let resp = bincode::deserialize(&resp).unwrap();
        Ok((resp, resp_data))
    }

    /// Add a RPC handler.
    pub fn add_rpc_handler<R: Request, AsyncFn, Fut>(self: &Arc<Self>, f: AsyncFn)
    where
        AsyncFn: Fn(R) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R::Response> + Send + 'static,
    {
        self.add_rpc_handler_with_data(move |req, _data| f(req).map(|rsp| (rsp, vec![])))
    }

    /// Add a RPC handler that send and receive data.
    pub fn add_rpc_handler_with_data<R: Request, AsyncFn, Fut>(self: &Arc<Self>, f: AsyncFn)
    where
        AsyncFn: Fn(R, Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = (R::Response, Vec<u8>)> + Send + 'static,
    {
        let handler = move |inner: Arc<Inner>, msg: RecvMsg| {
            let ep_id = msg.ep_id;
            let req_id = msg.header.req_id;
            let (req, req_data) = msg.take();
            let req = bincode::deserialize(&req).unwrap();
            let fut = f(req, req_data);
            task::spawn(async move {
                let (resp, data) = fut.await;
                inner
                    .send_response::<R>(ep_id, req_id, &resp, Some(&data))
                    .await
                    .ok();
            });
        };
        self.inner().register(R::ID, handler);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        for task in self.tasks.lock().unwrap().iter() {
            task.abort();
        }
    }
}
