# X / Twitter cookies

The X pipeline needs an authenticated browser session. Export your logged-in
x.com cookies in **Netscape** format to:

```
secrets/cookies.txt
```

Ways to do that:

1. Sign in to x.com in a browser.
2. Use a "cookies.txt" export extension (for example _Get cookies.txt
   LOCALLY_ or _Open Cookies.txt_) and export for `x.com`.
3. Replace `secrets/cookies.txt` with the downloaded file.

The file must start with a line like:

```
# Netscape HTTP Cookie File
```

`gallery-dl` reads it directly; the Playwright fallback converts it to
browser-context cookies (including `#HttpOnly_` entries).

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
