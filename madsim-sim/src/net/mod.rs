//! Asynchronous network endpoint and a controlled network simulator.
//!
//! # Examples
//!
//! ```
//! # use madsim_sim as madsim;
//! use madsim::{Runtime, net::NetLocalHandle};
//! use std::net::SocketAddr;
//!
//! let runtime = Runtime::new();
//! let addr1 = "0.0.0.1:1".parse::<SocketAddr>().unwrap();
//! let addr2 = "0.0.0.2:1".parse::<SocketAddr>().unwrap();
//! let host1 = runtime.create_host(addr1).build().unwrap();
//! let host2 = runtime.create_host(addr2).build().unwrap();
//!
//! host1
//!     .spawn(async move {
//!         let net = NetLocalHandle::current();
//!         net.send_to(addr2, 1, &[1]).await.unwrap();
//!     })
//!     .detach();
//!
//! let f = host2.spawn(async move {
//!     let net = NetLocalHandle::current();
//!     let mut buf = vec![0; 0x10];
//!     let (len, from) = net.recv_from(1, &mut buf).await.unwrap();
//!     assert_eq!(from, addr1);
//!     assert_eq!(&buf[..len], &[1]);
//! });
//!
//! runtime.block_on(f);
//! ```

use log::*;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};

pub use self::network::{Config, Stat};
use self::network::{Network, Payload};
use crate::{
    plugin::{addr, simulator, Simulator},
    rand::{RandHandle, Rng},
    time::{Duration, TimeHandle},
};

mod network;
#[cfg(feature = "rpc")]
pub mod rpc;

/// Network simulator.
pub struct NetSim {
    network: Mutex<Network>,
    rand: RandHandle,
    time: TimeHandle,
}

impl Simulator for NetSim {
    fn new(rand: &RandHandle, time: &TimeHandle, config: &crate::Config) -> Self {
        NetSim {
            network: Mutex::new(Network::new(rand.clone(), time.clone(), config.net.clone())),
            rand: rand.clone(),
            time: time.clone(),
        }
    }

    fn create(&self, addr: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.insert(addr);
    }

    fn reset(&self, addr: SocketAddr) {
        self.reset(addr);
    }
}

impl NetSim {
    /// Return a handle of the specified host.
    pub(crate) fn get_host(self: &Arc<Self>, addr: SocketAddr) -> NetLocalHandle {
        NetLocalHandle {
            net: self.clone(),
            addr,
        }
    }

    /// Get the statistics.
    pub fn stat(&self) -> Stat {
        self.network.lock().unwrap().stat().clone()
    }

    /// Update network configurations.
    pub fn update_config(&self, f: impl FnOnce(&mut Config)) {
        let mut network = self.network.lock().unwrap();
        network.update_config(f);
    }

    /// Reset an endpoint.
    ///
    /// All connections will be closed.
    pub fn reset(&self, addr: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.reset(addr);
    }

    /// Connect a host to the network.
    pub fn connect(&self, addr: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.unclog(addr);
    }

    /// Disconnect a host from the network.
    pub fn disconnect(&self, addr: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.clog(addr);
    }

    /// Connect a pair of hosts.
    pub fn connect2(&self, addr1: SocketAddr, addr2: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.unclog_link(addr1, addr2);
        network.unclog_link(addr2, addr1);
    }

    /// Disconnect a pair of hosts.
    pub fn disconnect2(&self, addr1: SocketAddr, addr2: SocketAddr) {
        let mut network = self.network.lock().unwrap();
        network.clog_link(addr1, addr2);
        network.clog_link(addr2, addr1);
    }
}

/// Local host network handle to the runtime.
#[derive(Clone)]
pub struct NetLocalHandle {
    net: Arc<NetSim>,
    addr: SocketAddr,
}

impl NetLocalHandle {
    /// Returns a [`NetLocalHandle`] view over the currently running [`Runtime`].
    ///
    /// [`Runtime`]: crate::Runtime
    pub fn current() -> Self {
        simulator::<NetSim>().get_host(addr())
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Sends data with tag on the socket to the given address.
    ///
    /// # Example
    /// ```
    /// # use madsim_sim as madsim;
    /// use madsim::{Runtime, net::NetLocalHandle};
    ///
    /// Runtime::new().block_on(async {
    ///     let net = NetLocalHandle::current();
    ///     net.send_to("127.0.0.1:4242", 0, &[0; 10]).await.expect("couldn't send data");
    /// });
    /// ```
    pub async fn send_to(&self, dst: impl ToSocketAddrs, tag: u64, data: &[u8]) -> io::Result<()> {
        let dst = dst.to_socket_addrs()?.next().unwrap();
        self.send_to_raw(dst, tag, Box::new(Vec::from(data))).await
    }

    /// Receives a single message with given tag on the socket.
    /// On success, returns the number of bytes read and the origin.
    ///
    /// # Example
    /// ```no_run
    /// # use madsim_sim as madsim;
    /// use madsim::{Runtime, net::NetLocalHandle};
    ///
    /// Runtime::new().block_on(async {
    ///     let net = NetLocalHandle::current();
    ///     let mut buf = [0; 10];
    ///     let (len, src) = net.recv_from(0, &mut buf).await.expect("couldn't receive data");
    /// });
    /// ```
    pub async fn recv_from(&self, tag: u64, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (data, from) = self.recv_from_raw(tag).await?;
        // copy to buffer
        let data = data.downcast::<Vec<u8>>().expect("message is not data");
        let len = buf.len().min(data.len());
        buf[..len].copy_from_slice(&data[..len]);
        Ok((len, from))
    }

    /// Sends a raw message.
    async fn send_to_raw(&self, dst: SocketAddr, tag: u64, data: Payload) -> io::Result<()> {
        self.net
            .network
            .lock()
            .unwrap()
            .send(self.addr, dst, tag, data);
        // random delay
        let delay = Duration::from_micros(self.net.rand.with(|rng| rng.gen_range(0..5)));
        self.net.time.sleep(delay).await;
        Ok(())
    }

    /// Receives a raw message.
    async fn recv_from_raw(&self, tag: u64) -> io::Result<(Payload, SocketAddr)> {
        let recver = self.net.network.lock().unwrap().recv(self.addr, tag);
        let msg = recver
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "network is down"))?;
        // random delay
        let delay = Duration::from_micros(self.net.rand.with(|rng| rng.gen_range(0..5)));
        self.net.time.sleep(delay).await;

        trace!("recv: {} <- {}, tag={}", self.addr, msg.from, msg.tag);
        Ok((msg.data, msg.from))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::{NetLocalHandle, NetSim};
    use crate::{plugin::simulator, time::*, Runtime};

    #[test]
    fn send_recv() {
        let runtime = Runtime::new();
        let addr1 = "0.0.0.1:1".parse::<SocketAddr>().unwrap();
        let addr2 = "0.0.0.2:1".parse::<SocketAddr>().unwrap();
        let host1 = runtime.create_host(addr1).build().unwrap();
        let host2 = runtime.create_host(addr2).build().unwrap();

        host1
            .spawn(async move {
                let net = NetLocalHandle::current();
                net.send_to(addr2, 1, &[1]).await.unwrap();

                sleep(Duration::from_secs(1)).await;
                net.send_to(addr2, 2, &[2]).await.unwrap();
            })
            .detach();

        let f = host2.spawn(async move {
            let net = NetLocalHandle::current();
            let mut buf = vec![0; 0x10];
            let (len, from) = net.recv_from(2, &mut buf).await.unwrap();
            assert_eq!(len, 1);
            assert_eq!(from, addr1);
            assert_eq!(buf[0], 2);
            let (len, from) = net.recv_from(1, &mut buf).await.unwrap();
            assert_eq!(len, 1);
            assert_eq!(from, addr1);
            assert_eq!(buf[0], 1);
        });

        runtime.block_on(f);
    }

    #[test]
    fn receiver_drop() {
        let runtime = Runtime::new();
        let addr1 = "0.0.0.1:1".parse::<SocketAddr>().unwrap();
        let addr2 = "0.0.0.2:1".parse::<SocketAddr>().unwrap();
        let host1 = runtime.create_host(addr1).build().unwrap();
        let host2 = runtime.create_host(addr2).build().unwrap();

        host1
            .spawn(async move {
                let net = NetLocalHandle::current();
                sleep(Duration::from_secs(2)).await;
                net.send_to(addr2, 1, &[1]).await.unwrap();
            })
            .detach();

        let f = host2.spawn(async move {
            let net = NetLocalHandle::current();
            let mut buf = vec![0; 0x10];
            timeout(Duration::from_secs(1), net.recv_from(1, &mut buf))
                .await
                .err()
                .unwrap();
            // timeout and receiver dropped here
            sleep(Duration::from_secs(2)).await;
            // receive again should success
            let (len, from) = net.recv_from(1, &mut buf).await.unwrap();
            assert_eq!(len, 1);
            assert_eq!(from, addr1);
        });

        runtime.block_on(f);
    }

    #[test]
    fn reset() {
        let runtime = Runtime::new();
        let addr1 = "0.0.0.1:1".parse::<SocketAddr>().unwrap();
        let host1 = runtime.create_host(addr1).build().unwrap();

        let f = host1.spawn(async move {
            let net = NetLocalHandle::current();
            let err = net.recv_from(1, &mut []).await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        });

        runtime.block_on(async move {
            sleep(Duration::from_secs(1)).await;
            simulator::<NetSim>().reset(addr1);
            f.await;
        });
    }
}
