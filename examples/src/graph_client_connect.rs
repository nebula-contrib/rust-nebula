use rust_nebula::{graph::query::GraphQuery as _, GraphClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9669";
    let username = "root";
    let password = "password";

    println!(
        "graph_client addr:{} username:{} password:{}",
        addr, username, password
    );

    //
    let client = GraphClient::new(&addr).await?;

    let mut session = client.authenticate(&username, &password).await?;

    let res = session.show_hosts().await?;
    println!("{:?}", res);
    let res = session.show_spaces().await?;
    println!("{:?}", res);

    session.execute("USE basketballplayer;").await?;

    let output = session
        .query(
            "WITH [NULL, 1, 1, 2, 2] As a \
        UNWIND a AS b \
        RETURN count(b), COUNT(*), couNT(DISTINCT b);",
        )
        .await?;
    if let Some(dataset) = output.dataset() {
        println!("{}", dataset);
    }
    let output = session
        .query("MATCH (v:player)-[:follow]->() RETURN v;")
        .await?;
    if let Some(dataset) = output.dataset() {
        println!("{}", dataset);
    }

    Ok(())
}
