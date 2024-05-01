use clap::Parser;

use redis_starter_rust::redis::Redis;
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to listen to
    #[arg(short, long, default_value = "6379")]
    port: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    info!("Logs from your program will appear here!");

    let addr = format!("127.0.0.1:{}", args.port);
    info!("Listening to {addr}...");

    let redis = match Redis::new(addr).await {
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
