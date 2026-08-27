use std::sync::Arc;

use anyhow::Result;
use strata_sdk::config;
use http_client::{HttpClient, OAuth2};
use schema::HasSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use strata_sdk::page::ListStrategy;
use strata_sdk::router::{Route, Router};
use strata_sdk::{Cursor, Page, Params};

const API_BASE: &str = "https://api.spotify.com/v1";
const TOKEN_URL: &str = "https://accounts.spotify.com/api/token";

#[config]
pub struct Config {
    #[config(env = "SPOTIFY_CLIENT_ID", description = "Spotify app client ID")]
    client_id: String,
    #[config(
        env = "SPOTIFY_CLIENT_SECRET",
        description = "Spotify app client secret",
        secret
    )]
    client_secret: String,
}

impl Config {
    fn auth_source(&self) -> OAuth2 {
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();

        OAuth2::new(TOKEN_URL, move || {
            Ok(vec![
                ("grant_type".into(), "client_credentials".into()),
                ("client_id".into(), client_id.clone()),
                ("client_secret".into(), client_secret.clone()),
            ])
        })
    }
}

const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 49; // 50 - end check

#[derive(strata_sdk::Provider)]
#[config(Config)]
pub struct Spotify {
    http: HttpClient,
}

impl Spotify {
    fn new(config: Config) -> Result<Self> {
        let http = HttpClient::builder().auth(config.auth_source()).build()?;
        Ok(Spotify { http })
    }

    async fn api_get<T: DeserializeOwned>(&self, path: &str, query: &[(&str, &str)]) -> Result<T> {
        let request = self.http.get(format!("{API_BASE}{path}")).query(query);
        Ok(self.http.send_json::<T>(request).await?)
    }

    fn routes(r: &mut Router<Self>) {
        r.add(Route::new().path("/podcasts/:id").get(get_show));
        r.add(
            Route::new()
                .path("/podcasts/:id/episodes")
                .list(list_episodes)
                .strategy(ListStrategy::Offset),
        );
        r.add(Route::new().path("/albums/:id").get(get_album));
        r.add(
            Route::new()
                .path("/albums/:id/tracks")
                .list(list_tracks)
                .strategy(ListStrategy::Offset),
        );
        r.add(Route::new().path("/artists/:id").get(get_artist));
        r.add(
            Route::new()
                .path("/artists/:id/albums")
                .list(list_albums)
                .strategy(ListStrategy::Offset),
        );
    }
}

#[derive(Deserialize, Serialize)]
struct SpotifyCursor {
    #[serde(default)]
    offset: u32,
}

async fn page<T>(
    http: &HttpClient,
    p: Params,
    url: String,
    query: &[(&str, &str)],
    envelope: Option<String>,
) -> Result<Page<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let cursor: SpotifyCursor = p.cursor()?;
    let mut limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT);

    // HACK: /artists/{id}/albums rejects `limit` above 10; clamp to 9.
    if url.ends_with("/albums") {
        limit = limit.min(8);
    }

    // Fetch one extra row: if it comes back there's a next page.
    let request = http.get(url).query(query).query(&[
        ("limit", (limit + 1).to_string()),
        ("offset", cursor.offset.to_string()),
    ]);
    let body = http.send_json::<Value>(request).await?;

    let raw = match &envelope {
        Some(key) => body
            .get(key)
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
        None => body,
    };
    let mut entries: Vec<Option<T>> = serde_json::from_value(raw)?;

    let next = if entries.len() as u32 > limit {
        entries.truncate(limit as usize);
        Cursor::new(&SpotifyCursor {
            offset: cursor.offset + limit,
        })?
    } else {
        Cursor::empty()
    };
    let items: Vec<T> = entries.into_iter().flatten().collect();
    Ok(Page::new(items, next))
}

async fn get_artist(s: Arc<Spotify>, p: Params) -> Result<Artist> {
    s.api_get(&format!("/artists/{}", p.get("id")?), &[]).await
}

async fn list_albums(s: Arc<Spotify>, p: Params) -> Result<Page<AlbumRef>> {
    let url = format!("{API_BASE}/artists/{}/albums", p.get("id")?);

    page(&s.http, p, url, &[], Some("items".to_string())).await
}

async fn get_show(s: Arc<Spotify>, p: Params) -> Result<Show> {
    let market = market();

    s.api_get(
        &format!("/shows/{}", p.get("id")?),
        &[("market", market.as_str())],
    )
    .await
}

async fn list_episodes(s: Arc<Spotify>, p: Params) -> Result<Page<Episode>> {
    let market = market();
    let url = format!("{API_BASE}/shows/{}/episodes", p.get("id")?);

    page(
        &s.http,
        p,
        url,
        &[("market", market.as_str())],
        Some("items".to_string()),
    )
    .await
}

async fn get_album(s: Arc<Spotify>, p: Params) -> Result<Album> {
    s.api_get(&format!("/albums/{}", p.get("id")?), &[]).await
}

async fn list_tracks(s: Arc<Spotify>, p: Params) -> Result<Page<Track>> {
    let url = format!("{API_BASE}/albums/{}/tracks", p.get("id")?);

    page(&s.http, p, url, &[], Some("items".to_string())).await
}

/// Default market for endpoints that require one (top-tracks, shows, episodes).
/// Override with `SPOTIFY_MARKET` (ISO 3166-1 alpha-2); defaults to `US`.
fn market() -> String {
    std::env::var("SPOTIFY_MARKET").unwrap_or_else(|_| "US".to_string())
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Album {
    id: String,
    name: String,
    album_type: String,
    release_date: String,
    total_tracks: u64,
    label: Option<String>,
    popularity: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Track {
    id: String,
    name: String,
    track_number: u64,
    duration_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Artist {
    id: String,
    name: String,
    #[serde(default)]
    genres: Vec<String>,
    popularity: Option<u64>,
    followers: Option<Followers>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Followers {
    total: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct AlbumRef {
    id: String,
    name: String,
    album_type: String,
    release_date: String,
    total_tracks: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Show {
    id: String,
    name: String,
    publisher: Option<String>,
    description: String,
    total_episodes: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Episode {
    id: String,
    name: String,
    description: String,
    release_date: String,
    duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_sdk::testkit::Client;

    // Stable IDs (Radiohead / an album / a show) matching the recorded cassettes.
    const ARTIST_ID: &str = "4Z8W4fKeB5YxbusRsdQVPb";
    const ALBUM_ID: &str = "4LH4d3cOWNNsVw41Gqt2kv";
    const SHOW_ID: &str = "2MAi0BvDc6GTFvKFPXnkCL";

    fn client() -> Result<Client<Spotify>> {
        let config: strata_sdk::config::ProviderConfig =
            serde_json::from_value(serde_json::json!({
                "backend": "spotify",
                "client_id": "test",
                "client_secret": "test",
            }))?;
        Client::<Spotify>::mount(&config)
    }

    #[tokio::test]
    async fn get_artist() -> Result<()> {
        let artist: Artist = client()?.get(&format!("/artists/{ARTIST_ID}")).await?;
        assert!(!artist.name.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn get_album() -> Result<()> {
        let album: Album = client()?.get(&format!("/albums/{ALBUM_ID}")).await?;
        assert!(!album.name.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn get_show() -> Result<()> {
        let show: Show = client()?.get(&format!("/podcasts/{SHOW_ID}")).await?;
        assert!(!show.name.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_albums() -> Result<()> {
        let mut stream = client()?
            .list(&format!("/artists/{ARTIST_ID}/albums"))
            .await?;
        let albums: Vec<AlbumRef> = stream.next().await?;
        assert!(!albums.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_tracks() -> Result<()> {
        let mut stream = client()?
            .list(&format!("/albums/{ALBUM_ID}/tracks"))
            .await?;
        let tracks: Vec<Track> = stream.next().await?;
        assert!(!tracks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_episodes() -> Result<()> {
        let mut stream = client()?
            .list(&format!("/podcasts/{SHOW_ID}/episodes"))
            .await?;
        let episodes: Vec<Episode> = stream.next().await?;
        assert!(!episodes.is_empty());
        Ok(())
    }
}
