use bytes::Buf;
use madsim::{
    net::{rpc::Request, Endpoint},
    task, Request,
};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::{io::IoSlice, net::SocketAddr};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "erpc example")]
struct Opt {
    #[structopt(about = "The local address", short, long)]
    listen: SocketAddr,

    #[structopt(
        about = "The device descriptor, needs to match /dev/infiniband/",
        long,
        default_value = "uverbs0"
    )]
    dev: String,

    #[structopt(
        about = "The address of remote server, required for client",
        short,
        long
    )]
    server: Option<SocketAddr>,

    #[structopt(about = "Data size of each rpc", short, default_value = "64")]
    data_size: usize,

    #[structopt(about = "Total test time (second)", short, default_value = "10")]
    test_time: u64,
}

#[derive(Serialize, Deserialize, Request)]
#[rtype(u64)]
struct Echo(u64);

#[derive(Clone)]
struct Server;

#[madsim::service]
impl Server {
    // send the raw bytes back
    async fn echo(&self, ep: Endpoint) {
        loop {
            let (mut data, from) = ep.recv_from_raw(Echo::ID).await.unwrap();
            let rsp_tag = data.get_u64();
            let req_len = data.get_u32() as usize;
            let req_bytes = data.split_to(req_len);
            let req: Echo = bincode::deserialize(&req_bytes).unwrap();
            let rsp = Echo(req.0);
            let ep = ep.clone();
            task::spawn(async move {
                let rsp = bincode::serialize(&rsp).unwrap();
                let rsp_len_buf = (rsp.len() as u32).to_be_bytes();
                let mut iov = [
                    IoSlice::new(&rsp_len_buf[..]),
                    IoSlice::new(&rsp),
                    IoSlice::new(&data),
                ];
                ep.send_to_vectored(from, rsp_tag, &mut iov).await.unwrap();
            });
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::time::Duration;

    use madsim::time::Instant;

    let opt = Opt::from_args();
    let ep = Endpoint::bind(opt.listen)
        .await
        .unwrap()
        .init(&opt.dev)
        .await
        .unwrap();

    let main = task::spawn(async move {
        if let Some(addr) = opt.server {
            // client
            let mut rpc_cnt: u32 = 0;
            let mut rpc_size: u64 = 0;
            let buf: Vec<_> = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(opt.data_size)
                .collect();

            let start = Instant::now();
            let mut tick = Instant::now();
            let mut id = 0;
            while start.elapsed() < Duration::from_secs(opt.test_time) {
                for _ in 0..1000 {
                    let (reply, bytes) = ep.call_with_data(addr, Echo(id), &buf).await.unwrap();
                    assert_eq!(reply, id);
                    assert_eq!(bytes.len(), opt.data_size);

                    id += 1;
                    rpc_cnt += 1;
                    rpc_size += 2 * (opt.data_size as u64);
                }

                if tick.elapsed() > Duration::from_secs(1) {
                    let dur = tick.elapsed();
                    let iops = (rpc_cnt as f64 / dur.as_secs_f64()) as u64;
                    let lat = dur / rpc_cnt;
                    let band = rpc_size as f64 / (1 << 20) as f64 / dur.as_secs_f64();
                    println!("iops {}, latency {:?}, bandwidth {}", iops, lat, band);

                    tick = Instant::now();
                    rpc_cnt = 0;
                    rpc_size = 0;
                }
            }
        } else {
            // server
            println!("listening on {}", ep.local_addr().unwrap());
            let server = Server;
            // rpc handler
            task::spawn(async move {
                server.echo(ep).await;
            });
            std::future::pending::<()>().await;
        }
    });
    main.await.unwrap();
}
