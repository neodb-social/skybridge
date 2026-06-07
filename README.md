# NeoDB Sky Bridge

NeoDB Sky Bridge relays public AT Protocol activity (e.g. popfeed) into the Fediverse
as NeoDB-compatible ActivityPub. 

Any AT Protocol user may optout by themselves.

## How it works

```
Jetstream (atproto firehose)  ─┐
   or replayed fixtures        │   ┌───────────── translate ─────────────┐
                               ▼   │ popfeed record → NeoDB AP object    │
  filter popfeed collections ──►   │  (Note + relatedWith Status/Review/ │
  resolve DID → bridged Person │   │   Comment/Shelf, withRegardTo work) │
  (mint RSA keypair on sight)  │   │  wrapped in Create/Update/Delete    │
                               │   └─────────────────┬───────────────────┘
                               ▼                     ▼
                      persist to SQLite        enqueue delivery
                      (archive + dedup)              └─ relay: Announce → subscribers
                                                        (signed HTTP, retry/backoff)
```

### NeoDB compatibility contract

The activity object is a Mastodon-compatible `Note` so generic servers render
it. NeoDB catalog semantics ride alongside in a `relatedWith` array of typed
objects — `Status` (shelf mark), `Review`, `Rating`, `Comment`, `Shelf` — each
carrying a `withRegardTo` pointing at a dereferenceable catalog item that we
mint at `https://<domain>/catalog/<type>/<id>`. Deletes emit a `Delete`
referencing a `Tombstone`.

| popfeed record | becomes |
|---|---|
| `social.popfeed.feed.post` | `Note` + `Comment` `relatedWith` the work, tagged with a `Link` + category `Hashtag` |
| `social.popfeed.feed.list` | `Note` carrying a `Shelf` (name, summary, tags) |
| `social.popfeed.feed.listItem` | `Note` + a `Status` mark (`status` derived from `listType`) `withRegardTo` the work |

`creativeWorkType` maps to NeoDB categories: `movie`→movie, `tv_show`→tv,
`video_game`→game, `book`→book, `music`→music.

---

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SKYBRIDGE_DOMAIN` | `localhost:8000` | Public host of this relay (the single source of identity) |
| `SKYBRIDGE_SCHEME` | `https` (`http` for localhost) | URL scheme |
| `SKYBRIDGE_DB` | `skybridge.db` | SQLite path (`:memory:` for ephemeral) |
| `SKYBRIDGE_JETSTREAM` | public jetstream2 us-east | Jetstream WebSocket endpoint |
| `SKYBRIDGE_INGEST` | unset | set to `1` to start live ingestion inside `serve` |
| `SKYBRIDGE_LOG` | `INFO` | log level |

---

## Install & run

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync            # create .venv and install runtime + dev deps from uv.lock
```

The CLI has four subcommands (run them via `uv run`):

```bash
# Serve the ActivityPub endpoints + dashboard (set SKYBRIDGE_INGEST=1 to also
# stream live popfeed activity in the same process).
SKYBRIDGE_DOMAIN=bridge.example.social uv run python -m skybridge serve --port 8000

# Stream live popfeed activity from Jetstream (Ctrl-C to stop; --limit N to bound).
SKYBRIDGE_DOMAIN=bridge.example.social uv run python -m skybridge ingest

# Replay a captured JSONL fixture through the full pipeline (offline).
uv run python -m skybridge replay fixtures/jetstream_sample.jsonl --reset

# Seed from a single DID's existing popfeed records.
uv run python -m skybridge backfill did:plc:i6k6scfcdaup4e2va33nkprb
```

### Endpoints

- Discovery: `/.well-known/webfinger`, `/.well-known/nodeinfo`, `/nodeinfo/2.1`
- Relay actor: `GET /actor`, shared inbox `POST /inbox`, `POST /actor/inbox`
- Bridged actors: `GET /users/{handle}` (+ `/inbox` `/outbox` `/followers`)
- Objects: `GET /users/{handle}/posts/{rkey}` (Note / Tombstone),
  `GET /catalog/{type}/{id}` (catalog work)
- UI / stats: `GET /` (dashboard), `GET /archive`, `GET /archive/{at_uri}`
  (original record vs. translated AP side-by-side), `GET /catalog`, `GET /stats`
- Opt-out: `GET /optout` (form), `POST /optout` (authenticated; HTML or JSON)

## Development

make sure CI is clean before commit

```bash
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
```
