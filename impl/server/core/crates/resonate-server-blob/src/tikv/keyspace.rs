//! Creating an API v2 keyspace through PD's HTTP API, if it is missing.
//!
//! PD pre-allocates keyspaces (`keyspace.pre-alloc`) only when a cluster is
//! first bootstrapped. A cluster that predates the durable keyspace has none,
//! so the server creates it at startup, the way Loams 0.1.0 did. It is
//! idempotent, and a concurrent create by another instance is fine.

use std::time::Duration;

/// Makes sure keyspace `name` exists, creating it through PD's HTTP API at
/// `pd_http` (`http://host:port`). Returns its id.
pub async fn ensure_keyspace(pd_http: &str, name: &str) -> Result<u32, String> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(format!(
            "keyspace name {name:?} must be 1-64 characters of [A-Za-z0-9_-]"
        ));
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("PD HTTP client: {e}"))?;
    let base = pd_http.trim_end_matches('/');
    let url = format!("{base}/pd/api/v2/keyspaces/{name}");
    if let Some(id) = get(&http, &url).await? {
        return Ok(id);
    }
    let created = http
        .post(format!("{base}/pd/api/v2/keyspaces"))
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await
        .map_err(|e| format!("POST keyspace {name}: {e}"))?;
    let status = created.status();
    let body = created.text().await.unwrap_or_default();
    if status.is_success() {
        tracing::info!(keyspace = name, "created the TiKV keyspace");
        return id_of(&body);
    }
    // Lost a race with another instance: read what it created.
    get(&http, &url)
        .await?
        .ok_or_else(|| format!("POST keyspace {name}: {status} {body}"))
}

async fn get(http: &reqwest::Client, url: &str) -> Result<Option<u32>, String> {
    let answer = http
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    let status = answer.status();
    let body = answer.text().await.unwrap_or_default();
    if status.is_success() {
        return id_of(&body).map(Some);
    }
    // PD answers a missing keyspace with 404, or with 500 and a "not found"
    // or "does not exist" message, depending on the version.
    let lower = body.to_ascii_lowercase();
    if status.as_u16() == 404 || lower.contains("not found") || lower.contains("not exist") {
        return Ok(None);
    }
    Err(format!("GET {url}: {status} {body}"))
}

fn id_of(body: &str) -> Result<u32, String> {
    let meta: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("PD keyspace answer: {e}: {body}"))?;
    meta["id"]
        .as_u64()
        .and_then(|id| u32::try_from(id).ok())
        .ok_or_else(|| format!("PD keyspace answer has no id: {body}"))
}

/// The PD HTTP base for a PD endpoint as the client takes it (`host:port`).
pub fn pd_http_url(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", endpoint.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pd_http_base_defaults_to_plain_http() {
        assert_eq!(pd_http_url("127.0.0.1:2379"), "http://127.0.0.1:2379");
        assert_eq!(pd_http_url("https://pd:2379/"), "https://pd:2379");
    }

    #[test]
    fn a_keyspace_answer_gives_its_id() {
        assert_eq!(id_of(r#"{"id":7,"name":"k","state":"ENABLED"}"#), Ok(7));
        assert!(id_of("{}").is_err());
    }
}
