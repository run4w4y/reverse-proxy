use axum::{routing::get, Router};

async fn root() -> &'static str {
    "Hello, World!"
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(root));

    let port = std::env::var("PORT")
        .unwrap_or(String::from("8000"))
        .parse::<u16>()
        .unwrap();
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
