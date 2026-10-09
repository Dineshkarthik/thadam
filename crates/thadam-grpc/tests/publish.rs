//! Unary `Publish` over a real tonic client.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{client, config, next, open, publish};
use thadam_core::{AckOutcome, BatchLimits, Source};
use thadam_grpc::proto::PublishRequest;
use serde_json::json;
use tonic::{Code, Request};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_is_sent_only_after_the_batch_is_committed() {
    let (mut source, addr) = open(config(json!({}))).await;
    let mut c = client(addr).await;

    let mut request = PublishRequest {
        stream: "orders".into(),
        payload: b"hello".to_vec(),
        content_type: "application/json".into(),
        attributes: HashMap::from([("tenant".to_owned(), "acme".to_owned())]),
    };
    request.attributes.insert("region".into(), "eu".into());
    let call = tokio::spawn(async move { c.publish(Request::new(request)).await });

    let batch = next(&mut source, 8).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !call.is_finished(),
        "response sent before the batch committed"
    );

    let message = &batch.batch.messages[0];
    assert_eq!(message.payload.as_ref(), b"hello");
    assert_eq!(message.attrs.get("grpc.method"), Some("Publish"));
    assert_eq!(message.attrs.get("grpc.stream"), Some("orders"));
    assert_eq!(
        message.attrs.get("grpc.content_type"),
        Some("application/json")
    );
    assert_eq!(message.attrs.get("grpc.attr.tenant"), Some("acme"));
    assert_eq!(message.attrs.get("grpc.attr.region"), Some("eu"));
    assert!(
        message
            .attrs
            .get("grpc.peer")
            .is_some_and(|p| p.starts_with("127.0.0.1:"))
    );

    batch.ack.complete(AckOutcome::Committed);
    call.await
        .unwrap()
        .expect("publish succeeds once committed");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_batch_answers_unavailable() {
    let (mut source, addr) = open(config(json!({}))).await;
    let mut c = client(addr).await;
    let call = tokio::spawn(async move { c.publish(Request::new(publish("s", b"doomed"))).await });

    let batch = next(&mut source, 8).await;
    batch.ack.complete(AckOutcome::Abandoned);
    let status = call.await.unwrap().expect_err("abandoned must fail");
    assert_eq!(status.code(), Code::Unavailable);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_queue_answers_resource_exhausted_without_blocking() {
    let (mut source, addr) = open(config(json!({ "queue_capacity": 16 }))).await;

    let mut held = Vec::new();
    for i in 0..16u8 {
        let mut c = client(addr).await;
        held.push(tokio::spawn(async move {
            c.publish(Request::new(publish("s", &[i]))).await
        }));
    }
    // Waiting for 16 messages with a long linger means all of them are queued
    // before the batch is drained, so the queue is at capacity afterwards.
    let batch = tokio::time::timeout(
        Duration::from_secs(5),
        source.next_batch(BatchLimits {
            max_messages: 16,
            linger: Duration::from_secs(10),
            ..BatchLimits::default()
        }),
    )
    .await
    .expect("timed out")
    .expect("next_batch failed")
    .expect("stream ended");
    assert_eq!(batch.batch.messages.len(), 16);

    let mut c = client(addr).await;
    let refused = tokio::time::timeout(
        Duration::from_secs(2),
        c.publish(Request::new(publish("s", b"overflow"))),
    )
    .await
    .expect("a full queue must not block the call");
    assert_eq!(
        refused.expect_err("queue is full").code(),
        Code::ResourceExhausted
    );

    batch.ack.complete(AckOutcome::Committed);
    for call in held {
        call.await.unwrap().expect("held publishes commit");
    }
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversize_payload_is_invalid_argument() {
    let (mut source, addr) = open(config(json!({ "max_message_bytes": 1024 }))).await;
    let mut c = client(addr).await;

    let status = c
        .publish(Request::new(publish("s", &vec![0u8; 1025])))
        .await
        .expect_err("oversize must be refused");
    assert_eq!(status.code(), Code::InvalidArgument);

    let call = tokio::spawn(async move {
        c.publish(Request::new(publish("s", &vec![0u8; 1024])))
            .await
    });
    let batch = next(&mut source, 4).await;
    batch.ack.complete(AckOutcome::Committed);
    call.await
        .unwrap()
        .expect("a payload at the limit is accepted");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_token_is_required_when_configured() {
    let (mut source, addr) = open(config(json!({ "bearer_token": "s3cret" }))).await;
    let mut c = client(addr).await;

    let missing = c.publish(Request::new(publish("s", b"x"))).await;
    assert_eq!(missing.expect_err("no token").code(), Code::Unauthenticated);

    let mut wrong = Request::new(publish("s", b"x"));
    wrong
        .metadata_mut()
        .insert("authorization", "Bearer nope".parse().unwrap());
    let wrong = c.publish(wrong).await;
    assert_eq!(
        wrong.expect_err("wrong token").code(),
        Code::Unauthenticated
    );

    let mut good = Request::new(publish("s", b"accepted"));
    good.metadata_mut()
        .insert("authorization", "Bearer s3cret".parse().unwrap());
    let call = tokio::spawn(async move { c.publish(good).await });
    let batch = next(&mut source, 4).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"accepted");
    batch.ack.complete(AckOutcome::Committed);
    call.await.unwrap().expect("correct token is accepted");
    source.close().await.unwrap();
}
