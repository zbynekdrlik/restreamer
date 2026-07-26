# Cloudflare Tunnel Setup for stream.lan

Domain: `streamsnv.newlevel.media`
Machine: stream.lan (Windows 11 IoT Enterprise LTSC)

> ## READ THIS BEFORE YOU BRING UP A TUNNEL ON ANY BOX
>
> **A tunnel with no Cloudflare Access application in front of it publishes the
> whole dashboard — including every control action — to the entire internet.**
> That is not hypothetical: it is exactly what this document used to describe,
> and `streamsnv.newlevel.media` sat like that from the first day until
> 2026-07-26. Anyone who knew the hostname could stop the church live stream
> (#70, filed 2026-04-02; #273; #337; #339).
>
> So the order is **Access application FIRST, tunnel SECOND** — see
> [Cloudflare Access](#cloudflare-access-required) below. Reviving the dormant
> tunnel on `streampp` without doing this (#344) is the single most likely way
> this whole class of bug comes back.

## Prerequisites

- Cloudflare account with `newlevel.media` zone
- API Token: stored in password manager (same token as iem.newlevel.media)
- Account ID: `8f3efbc0edbe05bd6fdcab10cd63876a`
- Zone ID: `b9019ca528e573e62c2a110a45f45c74`

## Install cloudflared

```powershell
Invoke-WebRequest -Uri "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.msi" -OutFile "$env:TEMP\cloudflared.msi"
msiexec /i "$env:TEMP\cloudflared.msi" /quiet
```

## Create Tunnel

```powershell
cloudflared tunnel login
cloudflared tunnel create restreamer
cloudflared tunnel route dns restreamer streamsnv.newlevel.media
```

## Configure

Create `C:\Users\newlevel\.cloudflared\config.yml`:

```yaml
tunnel: restreamer
credentials-file: C:\Users\newlevel\.cloudflared\<tunnel-id>.json

ingress:
  - hostname: streamsnv.newlevel.media
    service: http://localhost:8910
  - service: http_status:404
```

## Install as Service

```powershell
cloudflared service install
cloudflared service start
```

## Cloudflare Access (REQUIRED)

Two layers protect the box. Both are required; neither alone is enough.

### Layer 1 — the Access application at the edge

A Zero Trust **self-hosted application** covering the whole hostname. An
unauthenticated request is redirected to the login screen and never reaches the
box at all — and because the application covers `hostname/*`, "we forgot to
protect one route" is impossible at this layer.

The Zero Trust org already existed before restreamer and guards several other
dashboards; follow that pattern rather than inventing a second mechanism.

| setting | value |
|---|---|
| org / team domain | `newlevelchurch.cloudflareaccess.com` |
| identity provider | one-time PIN (a code e-mailed to the operator) |
| session | 730 h, matching the other dashboards |
| policy | Allow, specific e-mail addresses |
| application `restreamer-snv` | `streamsnv.newlevel.media` |
| application `restreamer-pp` | `streampp.newlevel.media` |

Adding an operator is one field in the Zero Trust dashboard's policy.

### Layer 2 — JWT verification inside the app

`crates/rs-api/src/access.rs` re-verifies the signed Access assertion on every
request. This is what survives a second ingress rule, a port-forward on the
router, a second `cloudflared`, or a tunnel revived on the other box with no
Access application attached.

Configured under `api.access` in `C:\ProgramData\Restreamer\config.json`, and
the built-in defaults already carry these values, so an untouched config is
correct:

```jsonc
"api": { "access": {
  "mode": "enforce",
  "team_domain": "newlevelchurch.cloudflareaccess.com",
  "aud": ["<restreamer-snv application audience tag>",
          "<restreamer-pp application audience tag>"]
}}
```

**None of these are secrets** — the team domain and the application audience
tags are public identifiers. That is deliberate: the box stores no credential
for this mechanism, so there is nothing on it to steal, leak through
`GET /api/v1/config`, or overwrite through `PATCH /api/v1/config`.

Both AUDs are listed on both boxes so `streamsnv` and `streampp` run a
byte-identical config.

### What stays open

The **church LAN is never authenticated** — loopback, RFC1918 and Tailscale
addresses reach everything with no login, exactly as before. When Cloudflare is
down, identity is down or the building's internet is out, the operator opens
`http://stream.lan:8910` and works normally; the local path performs no network
I/O at all, so it has nothing to hang on. Remote access is gone in that
situation with or without this feature, since it only ever ran over the tunnel.

CI is unaffected and holds no credentials: every call in `ci.yml` goes to
`http://127.0.0.1:8910` and `scripts/soak-mini.ps1` to the LAN address.

### Rollback, fastest first

1. `api.access.mode = "log_only"` — one config value, no rebuild, behaviour
   identical to before this feature. Restart Restreamer to apply.
2. Stop the `cloudflared` service — the hostname disappears from the internet,
   LAN untouched.
3. `api.access.mode = "lan_only"` — the opposite lever: refuses everything
   internet-sourced even with a valid token (a phone with a stolen session).
4. Delete the Access policy in the Zero Trust dashboard.

### Verifying

```powershell
# From the box itself — must be 200, no login anywhere.
Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status

# Simulate an internet-sourced request (this is what cloudflared looks like).
# Must be 403 from layer 2, with the reason in the app log.
curl.exe -s -o NUL -w "%{http_code}" -H "cf-connecting-ip: 203.0.113.7" `
  http://127.0.0.1:8910/api/v1/status
```

From a browser off-site, `https://streamsnv.newlevel.media/` must redirect to
the Cloudflare login. **Gotcha:** right after creating an application, the edge
may still serve a cached pre-Access `200` for a plain URL. Append a cache-busting
query string (`?x=1`) to see the real behaviour — do not conclude a path is
ungated from the plain URL alone.

## TLS Certificates (Let's Encrypt)

Generate on any Linux machine with certbot:

```bash
pip install certbot-dns-cloudflare

# Create cloudflare.ini with API token
echo "dns_cloudflare_api_token = YOUR_TOKEN" > cloudflare.ini
chmod 600 cloudflare.ini

certbot certonly \
  --dns-cloudflare \
  --dns-cloudflare-credentials cloudflare.ini \
  -d streamsnv.newlevel.media

# Certs at /etc/letsencrypt/live/streamsnv.newlevel.media/
```

Upload to GitHub Secrets:
- `TLS_CERT_PEM` <- contents of `fullchain.pem`
- `TLS_KEY_PEM` <- contents of `privkey.pem`

CI deploys these to `C:\ProgramData\Restreamer\cert.pem` and `key.pem`.

## Verify

```
curl -I https://streamsnv.newlevel.media/api/v1/status
```
