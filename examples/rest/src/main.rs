use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::sync::Arc;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use tracing::info;
use warp::Filter;
use warp::Reply;

use barge::Barge;
use serde::Serialize;
use warp::http::StatusCode;

#[derive(Parser)]
struct Args {
    #[arg(short = 'p', default_value = "3030")]
    port: u16,
    #[command(subcommand)]
    command: SubCommand,
}

#[derive(Subcommand)]
enum SubCommand {
    New { addr: String },
    Join { addr: String, join_addr: String },
}

#[derive(Serialize)]
struct Success<T> {
    data: T,
}

#[derive(Serialize)]
struct Problem {
    leader: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().init();

    let barge = match args.command {
        SubCommand::New { addr } => Barge::new(addr.parse()?),
        SubCommand::Join { addr, join_addr } => {
            Barge::join(addr.parse()?, vec![join_addr.parse()?])
        }
    };
    let barge = Arc::new(barge);

    let barge_filter = warp::any().map(move || barge.clone());

    let propose = warp::path("propose")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(barge_filter.clone())
        .and_then(|body: Bytes, barge: Arc<Barge>| async move {
            let data = body.to_vec();
            match barge.propose(data.clone()).await {
                Ok(()) => {
                    let resp = Success { data };
                    info!("Proposal accepted");
                    let json = warp::reply::json(&resp);
                    Ok::<_, warp::Rejection>(
                        warp::reply::with_status(json, StatusCode::OK).into_response(),
                    )
                }
                Err(err) => {
                    // TODO: query actual leader from `barge`; placeholder for now
                    let leader_hint = "http://127.0.0.1:3030/ping".to_string();
                    let prob = Problem {
                        leader: Some(leader_hint.clone()),
                    };
                    info!("Proposal rejected: {}", err);
                    let reply = warp::reply::json(&prob);
                    let reply = warp::reply::with_status(reply, StatusCode::CONFLICT);
                    let reply = warp::reply::with_header(reply, "x-leader", leader_hint);
                    Ok::<_, warp::Rejection>(reply.into_response())
                }
            }
        });

    let routes = propose;

    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, args.port);
    warp::serve(routes).run(addr).await;
    Ok(())
}
