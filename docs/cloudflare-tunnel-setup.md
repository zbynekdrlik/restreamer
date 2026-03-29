# Cloudflare Tunnel Setup for stream.lan

Domain: `streamsnv.newlevel.media`
Machine: stream.lan (Windows 11 IoT Enterprise LTSC)

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
