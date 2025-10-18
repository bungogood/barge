use clap::{Parser, Subcommand};
use futures::future::join_all;
use std::{path::PathBuf, time::Instant};
use tracing::info;

use barge::Barge;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: SubCommand,
}

#[derive(Subcommand)]
enum SubCommand {
    New { addr: String },
    Join { addr: String, join_addr: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();
    let dir = PathBuf::from("data");

    match args.command {
        SubCommand::New { addr } => {
            let addr = addr.parse()?;
            info!("Starting new cluster at {}", addr);
            let _barge = Barge::new(None, dir, addr);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        }
        SubCommand::Join { addr, join_addr } => {
            let addr = addr.parse()?;
            let join_addr = join_addr.parse()?;
            info!("Joining cluster at {} from {}", join_addr, addr);
            let barge = Barge::join(None, dir, addr, vec![join_addr]);
            // let num = 10_000;
            let num = 10;
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
