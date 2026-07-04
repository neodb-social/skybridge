# NeoDB Sky Bridge

NeoDB Sky Bridge relays public AT Protocol activity (e.g. popfeed) into the Fediverse
as NeoDB-compatible ActivityPub. 

Any AT Protocol user may optout by themselves.

## How it works

```
Jetstream (atproto firehose)  ─┐
   or replayed fixtures        │   ┌───────────── translate ─────────────┐
                               ▼   │ popfeed record → NeoDB AP object    │
  filter popfeed collections ──►   │  (one Note per author+work, with    │
  resolve DID → bridged Person │   │   Status/Rating/Comment relatedWith │
  (mint RSA keypair on sight)  │   │   the work) in Create/Update/Delete │
                               │   └─────────────────┬───────────────────┘
                               ▼                     ▼
                      persist to SQLite        enqueue delivery
                      (archive + dedup)              └─ relay: Announce → subscribers
                                                        (signed HTTP, retry/backoff)
```

### NeoDB compatibility contract

The activity object is a Mastodon-compatible `Note` so generic servers render
it. NeoDB catalog semantics ride alongside in a `relatedWith` array of typed
objects — `Status` (shelf mark), `Rating`, `Comment` — each carrying a
`withRegardTo` pointing at a dereferenceable catalog item that we mint at
`https://<domain>/catalog/<type>/<id>`. Deletes emit a `Delete` referencing a
`Tombstone`. We emit `Note`s only — never `Article`/titled `Review` objects.

| popfeed record | becomes |
|---|---|
| `social.popfeed.feed.review` | `Note` + `Rating` (0-10) and, when there is review text, an untitled `Comment` `withRegardTo` the work (never a titled `Review`/`Article`); `containsSpoilers` sets `sensitive` + a CW `summary` |
| `social.popfeed.feed.list` | archived only (no AP emission): stored for `listUri` resolution and future NeoDB `Collection` mapping |
| `social.popfeed.feed.listItem` | on a shelf-type list: `Note` + a `Status` mark (`status` derived from `listType`, including compound types like `watched_movies`) `withRegardTo` the work — folded into the review's Note when the same author reviewed the same work (see below). On a status-less list: archived only (collection membership is not bridged) |

One popfeed action ("watched + rated") writes a review *and* a listItem; the
bridge emits ONE AP `Note` per (author, work) carrying `Status` + `Rating` +
`Comment` together. The Note id is anchored on whichever record publishes
first (`/users/<handle>/posts/<rkey>` — rkeys are immutable, unlike work
identifiers); any later change to either record re-derives the combined Note
and sends an `Update` with the same id (rewatches included). Deleting the
anchoring record `Delete`s the Note (the surviving partner re-publishes under
its own rkey on its next event); deleting the partner just re-derives the
Note.

`listType` verbs map to the NeoDB shelf statuses wishlist / progress /
complete / dropped (do / doing / done / dropped per media type: watch, play,
read, listen); unrecognized listTypes fall back to plain list membership.

`creativeWorkType` maps to NeoDB categories: `movie`→movie, `tv_show`/
`tv_season`→tv, `video_game`→game, `book`→book, `music`/`album`/`ep`→music,
`podcast`→podcast (anything else falls back to `item`).
Works are deduplicated across records by *any* shared external identifier
(imdb/tmdb/igdb/steam/isbn/musicbrainz), so a review and a listItem carrying
different identifier subsets point at the same catalog entry.

Known but not bridged: `social.popfeed.feed.post` (legacy free-text posts,
superseded by `feed.review` in 2025), `social.popfeed.feed.reaction` (emoji
reactions; would become AP `Like`/`EmojiReact`), and the per-episode
`watchedEpisodes` array on tv listItems.

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
