//! Tests for the `thadam status` command.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exited normally")
}

#[test]
fn status_unreachable_server_exits_1() {
    // Use port 1 which should never have anything listening
    let out = thadam(&["status", "--admin-url", "http://127.0.0.1:1"]);
    assert_eq!(
        code(&out),
        1,
        "expected exit 1 for unreachable server, got {}\nstdout: {}\nstderr: {}",
        code(&out),
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("failed to connect") || stderr.contains("Connection refused"),
        "expected connection error in stderr: {stderr}"
    );
}

/// A minimal HTTP/1.1 server that responds to GET /status with a fixed JSON body.
fn spawn_test_server(response_body: &'static str) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    let port = listener.local_addr().expect("get local addr").port();

    thread::spawn(move || {
        // Accept one connection
        if let Ok((mut stream, _)) = listener.accept() {
            // Read the request (we don't care about the content, just drain it)
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);

            // Send HTTP/1.1 response
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    // Give the server a moment to be ready
    thread::sleep(Duration::from_millis(50));
    port
}

#[test]
fn status_with_local_server_prints_json_and_exits_0() {
    let json_response = r#"{"status":"ok","version":"1.0.0"}"#;
    let port = spawn_test_server(json_response);

    let url = format!("http://127.0.0.1:{}", port);
    let out = thadam(&["status", "--admin-url", &url]);

    assert_eq!(
        code(&out),
        0,
        "expected exit 0, got {}\nstdout: {}\nstderr: {}",
        code(&out),
        text(&out.stdout),
        text(&out.stderr)
    );

    let stdout = text(&out.stdout);
    // Check that the pretty-printed JSON is in stdout
    assert!(
        stdout.contains("status"),
        "expected 'status' in output: {stdout}"
    );
    assert!(stdout.contains("ok"), "expected 'ok' in output: {stdout}");
    assert!(
        stdout.contains("version"),
        "expected 'version' in output: {stdout}"
    );
    assert!(
        stdout.contains("1.0.0"),
        "expected '1.0.0' in output: {stdout}"
    );

    let stderr = text(&out.stderr);
    assert!(stderr.is_empty(), "expected empty stderr: {stderr}");
}

#[test]
fn status_help_lists_flags() {
    let out = thadam(&["status", "--help"]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("thadam status"), "{stdout}");
    assert!(stdout.contains("--admin-url"), "{stdout}");
    assert!(stdout.contains("--token-file"), "{stdout}");
}
