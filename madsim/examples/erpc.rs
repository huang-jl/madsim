use bytes::Bytes;
use madsim::{net::Endpoint, task, Request};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
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

// #[derive(Serialize, Deserialize, Request)]
// #[rtype(u64)]
// struct EchoW(u64);

// #[derive(Serialize, Deserialize, Request)]
// #[rtype(u64)]
// struct EchoR(u64);

#[derive(Serialize, Deserialize, Request)]
#[rtype("Echo")]
struct Echo(u64);

#[derive(Clone)]
struct Server {
    reply_bytes: Arc<Vec<u8>>,
}

#[madsim::service]
impl Server {
    pub fn init(&self, ep: Arc<Endpoint>) {
        // ep.add_rpc_handler_with_data(|req: Echo, data: Bytes| async move { (Echo(req.0), data) });
        let this = self.clone();
        ep.add_rpc_handler_with_data(move |req: Echo, data: Bytes| {
            assert_eq!(this.reply_bytes.len(), data.len());
            // to_vec will introduce a copy, which will affect the maximum bandwidth
            // change the `add_rpc_handler_with_data` API to return Bytes instead of Vec to test the maximum bandwidth
            async move { (Echo(req.0), data.to_vec()) }
        });
    }
}

#[cfg(feature = "erpc")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::time::{Duration, Instant};

    let opt = Opt::from_args();
    println!(
        "dev: {}, data_size: {}, test_time: {}, server_addr: {:?}",
        opt.dev, opt.data_size, opt.test_time, opt.server
    );

    let ep = Endpoint::bind(opt.listen)
        .await
        .unwrap()
        .init(&opt.dev)
        .await
        .unwrap();

    let main = task::spawn(async move {
        if let Some(addr) = opt.server {
            // client
            // let ep = Endpoint::bind(opt.listen).await.unwrap();
            let mut rpc_cnt: u32 = 0;
            let mut rpc_size: u64 = 0;
            let buf: Vec<_> = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(opt.data_size)
                .collect();

            let start = Instant::now();
            let mut tick = Instant::now();
            while start.elapsed() < Duration::from_secs(opt.test_time) {
                for _ in 0..1000 {
                    let (reply, bytes) = ep.call_with_data(addr, Echo(123), &buf).await.unwrap();
                    assert_eq!(reply.0, 123);
                    assert_eq!(bytes.len(), opt.data_size);

                    rpc_cnt += 1;
                    rpc_size += 2 * (opt.data_size as u64);
                    // rpc_size += opt.data_size as u64;
                }

                let dur = tick.elapsed();
                if dur > Duration::from_secs(1) {
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
            // let ep = Endpoint::bind(opt.listen).await.unwrap();
            // let ep = Arc::new(ep.init(&opt.dev).await.unwrap());
            let ep = Arc::new(ep);
            println!("listening on {}, {}", ep.local_addr().unwrap(), ep.url());
            let server = Server {
                reply_bytes: Arc::new(
                    rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(opt.data_size)
                        .collect(),
                ),
            };
            server.init(ep.clone());
            std::future::pending::<()>().await;
        }
    });

    main.await.unwrap();
}
