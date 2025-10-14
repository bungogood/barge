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
    #[command(subcommand)]
    command: SubCommand,
}

#[derive(Subcommand)]
enum SubCommand {
    New { port: u16 },
    Join { port: u16, join_addr: String },
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
    tracing_subscriber::fmt().init();

    // Create a local single-node barge cluster on port 7000
    let barge = Arc::new(Barge::new(1, 7000));

    let barge_filter = warp::any().map(move || barge.clone());

    let propose = warp::path("propose")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(barge_filter.clone())
        .and_then(|body: Bytes, barge: Arc<Barge>| async move {
            let data = body.to_vec();
            match barge.propose(data).await {
                Ok(()) => {
                    let resp = Success {
                        data: "ok".to_string(),
                    };
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

    println!("Listening on http://127.0.0.1:3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
    Ok(())
}
