use rust_nebula::{
    graph::query::GraphQuery as _, HostAddress, SingleConnSessionConf, SingleConnSessionManager,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = SingleConnSessionConf::new(
        vec![HostAddress::new("127.0.0.1", 9669)],
        "root".to_owned(),
        "password".to_owned(),
        Some("basketballplayer".to_string()),
    );

    //
    let manager = SingleConnSessionManager::new(config);
    let pool = bb8::Pool::builder().max_size(10).build(manager).await?;

    //
    {
        let mut session = pool.get().await?;
        let res = session
            .query("MATCH (v:player)-[:follow]->() RETURN v LIMIT 10;")
            .await?;
        println!("{:?}", res.dataset());
    }

    //
    {
        let mut session = pool.get().await?;
        let res = session.show_spaces().await?;
        println!("{res:?}");
    }

    Ok(())
}
