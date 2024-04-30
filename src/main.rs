use redis_starter_rust::redis::Redis;
use tracing::error;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let redis = match Redis::new("127.0.0.1:6379").await {
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
