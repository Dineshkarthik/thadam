//! `thadam status`: check the admin server status.

use std::io::Write;
use std::path::Path;

use crate::exit::Exit;

const DEFAULT_ADMIN_URL: &str = "http://127.0.0.1:9464";

/// Check the admin server status endpoint.
///
/// Fetches `<admin_url>/status` and pretty-prints the JSON response.
/// Exits 0 on success, 1 if the server is unreachable or returns non-200.
pub fn run(
    admin_url: Option<&str>,
    token_file: Option<&Path>,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Exit {
    let url = admin_url.unwrap_or(DEFAULT_ADMIN_URL);
    let status_url = format!("{}/status", url.trim_end_matches('/'));

    // Read token if provided
    let token = if let Some(token_path) = token_file {
        match std::fs::read_to_string(token_path) {
            Ok(content) => Some(content.trim().to_string()),
            Err(e) => {
                let _ = writeln!(
                    err,
                    "cannot read token file `{}`: {e}",
                    token_path.display()
                );
                return Exit::RuntimeFailure;
            }
        }
    } else {
        None
    };

    // Build the request with optional Authorization header
    let request = ureq::get(&status_url);
    let request_with_auth = if let Some(t) = &token {
        request.header("Authorization", &format!("Bearer {t}"))
    } else {
        request
    };

    // Make the request
    match request_with_auth.call() {
        Ok(response) => {
            let status = response.status();

            // Read the body using the Read trait
            let mut body_reader = response.into_body();
            let body = match body_reader.read_to_string() {
                Ok(b) => b,
                Err(e) => {
                    let _ = writeln!(err, "failed to read response body: {e}");
                    return Exit::RuntimeFailure;
                }
            };

            // Pretty-print the JSON
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => match serde_json::to_string_pretty(&json) {
                    Ok(pretty) => {
                        let _ = writeln!(out, "{pretty}");
                        if status == 200 {
                            Exit::Ok
                        } else {
                            let _ = writeln!(err, "server returned non-200 status: {}", status);
                            Exit::RuntimeFailure
                        }
                    }
                    Err(e) => {
                        let _ = writeln!(err, "failed to format JSON: {e}");
                        Exit::RuntimeFailure
                    }
                },
                Err(e) => {
                    let _ = writeln!(err, "response is not valid JSON: {e}");
                    Exit::RuntimeFailure
                }
            }
        }
        Err(e) => {
            let _ = writeln!(err, "failed to connect to admin server at {url}: {e}");
            Exit::RuntimeFailure
        }
    }
}
