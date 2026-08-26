# Google provider — credential setup

The `google` provider reads Calendar, Drive/Docs, and Gmail (read-only) via
**OAuth2 with a refresh token**. Google has no static "API key + secret" for
private user data — reading your own data means consenting once in a browser,
after which the server uses the resulting refresh token headlessly forever.

It needs three environment variables (a `.env` at the repo root is loaded
automatically and is git-ignored):

```
GOOGLE_CLIENT_ID=...apps.googleusercontent.com
GOOGLE_SECRET=GOCSPX-...
GOOGLE_REFRESH_TOKEN=1//...
```

These are read lazily — only *calling* a `google` endpoint requires them, so the
rest of the CLI works without them.

The walkthrough below is fiddly, so the common errors are called out inline.

## 1. Project + APIs

[Google Cloud Console](https://console.cloud.google.com) → select/create a
project → **APIs & Services → Enable APIs and Services** → enable:

- **Google Calendar API**
- **Google Drive API**
- **Gmail API** (only if you'll use the gmail endpoints)

## 2. OAuth consent screen

APIs & Services → **OAuth consent screen** (newer consoles: **Google Auth
Platform**):

- **User type: External** (or **Internal** on Workspace).
- Add the read-only scopes you need:
  - `https://www.googleapis.com/auth/calendar.readonly`
  - `https://www.googleapis.com/auth/drive.readonly`
  - `https://www.googleapis.com/auth/gmail.readonly`
- **Audience → Test users**: add your own Google account.

> ⚠️ Skipping the test user gives `Error 403: access_denied` ("app is being
> tested, can only be accessed by developer-approved testers").

## 3. OAuth client → `GOOGLE_CLIENT_ID` + `GOOGLE_SECRET`

APIs & Services → **Credentials → Create Credentials → OAuth client ID**:

- **Application type: Web application** — *not* Desktop.
- **Authorized redirect URIs → Add URI** (exact, no trailing slash):
  ```
  https://developers.google.com/oauthplayground
  ```
- Create → copy the **client ID** (→ `GOOGLE_CLIENT_ID`) and **client secret**
  (→ `GOOGLE_SECRET`).

> ⚠️ A Desktop-type client, or omitting this redirect URI, gives
> `Error 400: redirect_uri_mismatch` in the next step. Credential changes can
> take a few minutes to propagate.

## 4. Refresh token → `GOOGLE_REFRESH_TOKEN`

[OAuth 2.0 Playground](https://developers.google.com/oauthplayground):

1. **⚙️ gear** (top right) → check **"Use your own OAuth credentials"** → paste
   the client id/secret from step 3.
2. **Step 1**: in "Input your own scopes", paste (space-separated):
   ```
   https://www.googleapis.com/auth/calendar.readonly https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/gmail.readonly
   ```
   → **Authorize APIs** → sign in as your test user → allow.
3. **Step 2** → **Exchange authorization code for tokens** → copy the
   **`refresh_token`**.

> ⚠️ **No refresh token returned?** Google issues one only on the *first*
> consent. [Revoke the app's access](https://myaccount.google.com/permissions)
> and redo this step.
>
> ⚠️ **7-day expiry:** while the consent screen is in **Testing** status
> (External user type), refresh tokens expire after 7 days. To make it durable,
> **Publish** the app ("In production"), or use **Internal** on Workspace.
> Publishing an app that requests the Gmail scope triggers Google's security
> assessment.

## 5. Verify

With the three values in `.env`:

```
cargo run -- call google /calendar
cargo run -- call google /calendar/primary/events
cargo run -- call google /drive
cargo run -- call google /gmail/profile
```
