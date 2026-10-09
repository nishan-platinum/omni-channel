//! Where the platform fixture comes from: a JSON file (bake-off default) or an HTTP(S) URL (the
//! hybrid candidate's platform endpoint, e.g. `GET /platform/config`).

use std::path::PathBuf;
use std::time::Duration;

use super::super::domain::Fixture;

#[derive(Debug, Clone)]
pub enum FixtureSource {
    File(PathBuf),
    Url(String),
}

impl FixtureSource {
    pub fn parse(s: &str) -> Self {
        if s.starts_with("http://") || s.starts_with("https://") {
            FixtureSource::Url(s.to_string())
        } else {
            FixtureSource::File(PathBuf::from(s))
        }
    }

    pub async fn load(&self) -> anyhow::Result<Fixture> {
        let bytes = match self {
            FixtureSource::File(p) => tokio::fs::read(p).await.map_err(|e| anyhow::anyhow!("{}: {e}", p.display()))?,
            FixtureSource::Url(u) => {
                let resp = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?
                    .get(u)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e.without_url()))?
                    .error_for_status()
                    .map_err(|e| anyhow::anyhow!("{}", e.without_url()))?;
                resp.bytes().await?.to_vec()
            }
        };
        Ok(serde_json::from_slice(&bytes)?)
    }
}
