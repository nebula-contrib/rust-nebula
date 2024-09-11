use rust_nebula::{
    graph::query::GraphQuery as _, HostAddress, SingleConnSessionConf, SingleConnSessionManager,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = SingleConnSessionConf::new(
        vec![HostAddress::new("127.0.0.1", 9669)],
        "root".to_owned(),
        "password".to_owned(),
        Some("basketballplayer".to_string()),
    );
    // Set fbThrift to a larger cache size, otherwise if the query result is too large,
    // e.g. `MATCH (v:player)-[:follow]->() RETURN v;`, it will report an error of `Reach max buffer size`
    config.set_max_buf_size(1024 * 1024);
    config.set_buf_size(10 * 1024);

    let client = SingleConnSessionManager::new(config);

    let mut session = client.get_session().await?;

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
        // Todo: datasetwrapper doesn't support displaying `path` data type now.
        println!("{:?}", dataset);
    }

    let output = session.query("SHOW HOSTS META;").await?;
    if let Some(dataset) = output.dataset() {
        println!("{}", dataset);
    }
    for i in 0..output.get_row_size() {
        match output.get_row_values_by_index(i) {
            Ok(record) => {
                let host = record.get_value_by_col_name("Host").unwrap();
                let port = record.get_value_by_col_name("Port").unwrap();
                let host_addr = format!("{}:{}", host.as_string()?, port.as_int()?);
                println!("{}", host_addr);
            }
            _ => {}
        }
    }
    Ok(())
}
