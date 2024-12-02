#[tokio::test]
async fn publish() {
    let nc = async_nats::connect("demo.nats.io").await.unwrap();

    let sub = nc.subscribe("subject").await.unwrap();

    let publisher = nc.publisher();
}
