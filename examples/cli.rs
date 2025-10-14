use clap::{Parser, Subcommand};
use std::net::SocketAddrV4;
use std::time::Instant;

use barge::Barge;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: SubCommand,
}

#[derive(Subcommand)]
enum SubCommand {
    New { port: u16 },
    Join { port: u16, join_addr: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();

    match args.command {
        SubCommand::New { port } => {
            tracing::info!("Starting new cluster on {}", port);
            let _barge = Barge::new(1, port);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        }
        SubCommand::Join { port, join_addr } => {
            let join_addr: SocketAddrV4 = join_addr.parse()?;
            tracing::info!("Joining cluster at {} from {}", join_addr, port);
            let barge = Barge::join(1, port, join_addr);
            let num = 100_0;
            let start = Instant::now();
            for _ in 0..num {
                let _ = barge.propose(b"Hello, world!".to_vec()).await;
            }
            let elapsed = start.elapsed();
            tracing::info!("Sent {} proposals in {:?}", num, elapsed);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        }
    }
}
