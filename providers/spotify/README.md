# Spotify provider — credential setup

The `spotify` provider uses the **Client Credentials** flow: app-only auth, fully
headless. It reaches Spotify's **public catalog** (search, artists, albums,
tracks) — not user data like playlists or listening history (that would need the
Authorization Code flow).

It needs two environment variables (a `.env` at the repo root is loaded
automatically and is git-ignored):

```
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
```

The provider trades these for a 1-hour access token and re-requests one when it
expires — there is no refresh token in this flow.

## 1. Create the app

1. [Spotify Developer Dashboard](https://developer.spotify.com/dashboard) → log in
   → accept the Developer Terms.
2. **Create app**: give it a name + description.
   - **Redirect URI**: any valid value is fine (it's a required field but unused
     by this flow), e.g. `http://127.0.0.1:8888/callback`.
     > ⚠️ Spotify rejects `http://localhost`; use the loopback IP `127.0.0.1` or
     > an `https://` URL.
   - **APIs**: tick **Web API**.
3. Open the app → **Settings** → copy **Client ID** and (via *View client secret*)
   the **Client secret** into `.env`.

## 2. Verify

```
curl -X POST https://accounts.spotify.com/api/token \
  -d grant_type=client_credentials \
  -d client_id=$SPOTIFY_CLIENT_ID \
  -d client_secret=$SPOTIFY_CLIENT_SECRET
```

Should return `{ "access_token": "...", "token_type": "Bearer", "expires_in": 3600 }`.

## Example call

Once the credentials are in `.env`, call an endpoint by its show ID — e.g. *The
Joe Rogan Experience* (`4rOoJ6Egrf8K2IrywzwOMk`):

```
cargo run -- call spotify /podcasts/4rOoJ6Egrf8K2IrywzwOMk
```
```json
{
  "id": "4rOoJ6Egrf8K2IrywzwOMk",
  "name": "The Joe Rogan Experience",
  "publisher": null,
  "description": "...",
  "total_episodes": 2709
}
```

List its episodes, then read one (the show ID is path context; the episode ID
does the lookup):

```
cargo run -- call spotify /podcasts/4rOoJ6Egrf8K2IrywzwOMk/episodes
cargo run -- call spotify /podcasts/4rOoJ6Egrf8K2IrywzwOMk/episodes/<episode_id>
```
