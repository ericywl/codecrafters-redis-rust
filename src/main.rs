use clap::Parser;

use redis_starter_rust::redis::{Redis, RedisConfig};
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to listen to
    #[arg(short, long, default_value = "6379")]
    port: String,

    /// Run as replica of master host and port
    #[arg(name = "replicaof", short, long, value_delimiter = ' ', num_args = 2, value_names=["master_host", "master_port"])]
    replica_of: Option<Vec<String>>,
}

impl Args {
    fn replica_addr(&self) -> Option<String> {
        self.replica_of.clone().map(|v| v.join(":"))
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    info!("Logs from your program will appear here!");

    let addr = format!("127.0.0.1:{}", args.port);
    info!("Listening to {addr}...");

    let redis = match Redis::new(
        addr,
        RedisConfig {
            replica_addr: args.replica_addr(),
        },
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Initialize redis error: {e}");
            return;
        }
    };

    match redis.start().await {
        Ok(()) => (),
        Err(e) => error!("Start redis error: {e}"),
    }
}
