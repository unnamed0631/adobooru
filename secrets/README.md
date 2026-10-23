# X / Twitter cookies

The X pipeline needs an authenticated browser session. Export your logged-in
x.com cookies as **JSON** to:

```
secrets/cookies.txt
```

Ways to do that:

1. Sign in to x.com in a browser.
2. Use a cookie export extension that supports JSON (for example
   _Cookie-Editor_ -> Export -> JSON, or _EditThisCookie_).
3. Replace `secrets/cookies.txt` with the downloaded file.

The file should be a JSON array of cookie objects, for example:

```json
[
  {
    "name": "auth_token",
    "value": "...",
    "domain": ".x.com",
    "path": "/",
    "secure": true,
    "httpOnly": true,
    "sameSite": "no_restriction",
    "session": false,
    "expirationDate": 1824186227.266
  }
]
```

The Playwright fallback uses the JSON directly. Because `gallery-dl` only
understands Netscape jars, the orchestrator converts the JSON to a
short-lived Netscape file for each gallery-dl run. Legacy Netscape
`cookies.txt` files are still accepted.

## Notes

- Treat the file like a password: it grants access to your X account.
  `secrets/*` is git-ignored; keep the host file `chmod 600`.
- The container runs as UID/GID 1000 (`app`). On Linux, if your host user has
  a different UID, make the file readable by 1000 (for example
  `sudo chown 1000:1000 secrets/cookies.txt`) or adjust the UID build args.
- Cookies expire. When ingestion starts returning login redirects or 401/403,
  export a fresh jar. `python ingest_orchestrator.py doctor` reports the file's
  age.
- Use a dedicated account if possible, and keep request rates low
  (`--sleep-request` defaults are set by the orchestrator). Respect X's Terms
  of Service and the artists' wishes; this project is for personal archiving,
  not redistribution.
- Set `TWITTER_ALLOW_ANONYMOUS=true` only for public profile scraping that
  works without a session. Hashtag/search timelines normally require login.
