//! Client-streaming `PublishStream` over a real tonic client.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use common::{client, config, next, open, publish};
use thadam_core::{AckOutcome, Source, SourceBatch};
use serde_json::json;
use tonic::{Code, Request};

fn payloads(n: usize) -> Vec<thadam_grpc::proto::PublishRequest> {
    (0..n)
        .map(|i| publish("s", format!("m{i:04}").as_bytes()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_acks_in_order_and_reports_the_count_after_all_commit() {
    let (mut source, addr) = open(config(json!({}))).await;
    let mut c = client(addr).await;
    let messages = payloads(40);
    let call = tokio::spawn(async move { c.publish_stream(tokio_stream::iter(messages)).await });

    let mut batches: Vec<SourceBatch> = Vec::new();
    let mut seen = 0usize;
    while seen < 40 {
        let batch = next(&mut source, 8).await;
        seen += batch.batch.messages.len();
        batches.push(batch);
    }
    let received: Vec<Vec<u8>> = batches
        .iter()
        .flat_map(|b| b.batch.messages.iter().map(|m| m.payload.to_vec()))
        .collect();
    let expected: Vec<Vec<u8>> = (0..40).map(|i| format!("m{i:04}").into_bytes()).collect();
    assert_eq!(received, expected, "messages arrive in stream order");

    // Batches are released in receive order: nothing is reported until the
    // first batch commits, even if later batches commit first.
    let head = batches.remove(0);
    for batch in batches.into_iter().rev() {
        batch.ack.complete(AckOutcome::Committed);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !call.is_finished(),
        "response sent before the first batch committed"
    );

    head.ack.complete(AckOutcome::Committed);
    let reply = call.await.unwrap().expect("stream succeeds");
    assert_eq!(reply.into_inner().accepted, 40);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_stream_commits_as_it_goes_and_counts_every_message() {
    let (mut source, addr) = open(config(json!({}))).await;
    let mut c = client(addr).await;
    let messages = payloads(300);
    let call = tokio::spawn(async move { c.publish_stream(tokio_stream::iter(messages)).await });

    let mut seen = 0usize;
    while seen < 300 {
        let batch = next(&mut source, 16).await;
        seen += batch.batch.messages.len();
        batch.ack.complete(AckOutcome::Committed);
    }
    let reply = call.await.unwrap().expect("stream succeeds");
    assert_eq!(reply.into_inner().accepted, 300);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_requires_the_bearer_token() {
    let (mut source, addr) = open(config(json!({ "bearer_token": "s3cret" }))).await;
    let mut c = client(addr).await;
    let status = c
        .publish_stream(tokio_stream::iter(payloads(3)))
        .await
        .expect_err("no token");
    assert_eq!(status.code(), Code::Unauthenticated);

    let mut request = Request::new(tokio_stream::iter(payloads(1)));
    request
        .metadata_mut()
        .insert("authorization", "Bearer s3cret".parse().unwrap());
    let call = tokio::spawn(async move { c.publish_stream(request).await });
    let batch = next(&mut source, 4).await;
    batch.ack.complete(AckOutcome::Committed);
    assert_eq!(
        call.await.unwrap().expect("accepted").into_inner().accepted,
        1
    );
    source.close().await.unwrap();
}
