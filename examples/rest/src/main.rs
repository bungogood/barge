use std::sync::Arc;

use bytes::Bytes;
use warp::Filter;

use barge::Barge;

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
                Ok(()) => Ok::<_, warp::Rejection>(warp::reply::with_status(
                    "ok",
                    warp::http::StatusCode::OK,
                )),
                Err(_) => Ok(warp::reply::with_status(
                    "error",
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                )),
            }
        });

    let routes = propose;

    println!("Listening on http://127.0.0.1:3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
    Ok(())
}
