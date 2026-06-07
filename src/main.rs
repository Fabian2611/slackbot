mod api;

use log::{error, info};
use crate::api::Bot;

#[tokio::main]
async fn main() {
    colog::init();

    let bot = Bot::init();
    info!("Starting bot...");
    if let Err(e) = bot.start().await {
        error!("Fatal runtime error: {}", e);
        std::process::exit(1);
    }
}
