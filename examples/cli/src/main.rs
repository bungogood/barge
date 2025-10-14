use clap::{Parser, Subcommand};
use futures::future::join_all;
use std::net::SocketAddrV4;
use std::time::Instant;
use tracing::info;

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
            info!("Starting new cluster on {}", port);
            let _barge = Barge::new(1, port);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        }
        SubCommand::Join { port, join_addr } => {
            let join_addr: SocketAddrV4 = join_addr.parse()?;
            info!("Joining cluster at {} from {}", join_addr, port);
            let barge = Barge::join(1, port, join_addr);
            let num = 100_000;
            let proposals = (0..num).map(|_| barge.propose(b"Hello, world!".to_vec()));
            let start = Instant::now();
            join_all(proposals).await;
            let elapsed = start.elapsed();
            let tps = elapsed / num;
            let rps = num as f64 / elapsed.as_secs_f64();
            info!(
                "Proposed {} entries in {:.2?} ({:.2?} per entry, {:.2} entries/sec)",
                num, elapsed, tps, rps
            );
            loop {}
        }
    }
}
