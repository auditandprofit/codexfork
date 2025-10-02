use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::SecondsFormat;
use chrono::Utc;
use codex_protocol::ConversationId;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde_json::Value;
use serde_json::json;
use tracing::warn;

pub const REQUEST_LOG_DIR_ENV: &str = "CODEX_REQUEST_LOG_DIR";

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Clone, Debug)]
pub struct RequestAttemptLogger {
    inner: Arc<RequestAttemptLogInner>,
}

impl RequestAttemptLogger {
    pub fn log_response_start(&self, status: StatusCode, headers: &HeaderMap) {
        let mut headers_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, value) in headers.iter() {
            headers_map
                .entry(name.as_str().to_string())
                .or_default()
                .push(value.to_str().unwrap_or_default().to_string());
        }

        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "response_started",
            "status": status.as_u16(),
            "headers": headers_map,
        }));
    }

    pub fn log_stream_event(&self, event: Option<&str>, data: &str) {
        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "sse_event",
            "event": event,
            "data": data,
        }));
    }

    pub fn log_stream_closed(&self, reason: &str) {
        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "sse_closed",
            "reason": reason,
        }));
    }

    pub fn log_error(&self, message: &str) {
        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "error",
            "message": message,
        }));
    }

    pub fn log_error_response(&self, status: StatusCode, body: &str) {
        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "error_response",
            "status": status.as_u16(),
            "body": body,
        }));
    }

    pub fn log_message(&self, message: &str) {
        self.write_json_line(json!({
            "timestamp": timestamp(),
            "type": "info",
            "message": message,
        }));
    }

    fn write_json_line(&self, value: Value) {
        let mut guard = match self.inner.file.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(e) = serde_json::to_writer(&mut *guard, &value) {
            warn!("request log serialization error: {}", e);
            return;
        }
        if let Err(e) = guard.write_all(b"\n") {
            warn!("request log write error: {}", e);
            return;
        }
        if let Err(e) = guard.flush() {
            warn!("request log flush error: {}", e);
        }
    }
}

#[derive(Debug)]
struct RequestAttemptLogInner {
    file: Mutex<std::fs::File>,
}

#[derive(Debug)]
pub struct RequestLogger {
    conversation_dir: PathBuf,
}

impl RequestLogger {
    pub fn from_env(conversation_id: &ConversationId) -> Option<Arc<Self>> {
        let base_dir = std::env::var_os(REQUEST_LOG_DIR_ENV)?;
        let conversation_dir = Path::new(&base_dir).join(conversation_id.to_string());
        if let Err(e) = std::fs::create_dir_all(&conversation_dir) {
            warn!(
                "failed to create request log directory {:?}: {}",
                conversation_dir, e
            );
            return None;
        }
        Some(Arc::new(Self { conversation_dir }))
    }

    pub fn log_request(
        &self,
        attempt: u64,
        url: &str,
        payload: &Value,
    ) -> Option<RequestAttemptLogger> {
        let attempt_id = format!("attempt-{attempt:03}");
        let request_path = self
            .conversation_dir
            .join(format!("{attempt_id}-request.json"));
        let response_path = self
            .conversation_dir
            .join(format!("{attempt_id}-response.jsonl"));

        if let Err(e) = write_request_file(&request_path, attempt, url, payload) {
            warn!("failed to write request log {:?}: {}", request_path, e);
            return None;
        }

        match create_response_file(&response_path) {
            Ok(file) => Some(RequestAttemptLogger {
                inner: Arc::new(RequestAttemptLogInner {
                    file: Mutex::new(file),
                }),
            }),
            Err(e) => {
                warn!("failed to prepare response log {:?}: {}", response_path, e);
                None
            }
        }
    }
}

fn write_request_file(
    path: &Path,
    attempt: u64,
    url: &str,
    payload: &Value,
) -> std::io::Result<()> {
    let mut opts = OpenOptions::new();
    opts.create(true).write(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    let mut file = opts.open(path)?;

    let record = json!({
        "timestamp": timestamp(),
        "attempt": attempt,
        "url": url,
        "payload": payload,
    });
    serde_json::to_writer_pretty(&mut file, &record)?;
    file.write_all(b"\n")
}

fn create_response_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = OpenOptions::new();
    opts.create(true).write(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    opts.open(path)
}
