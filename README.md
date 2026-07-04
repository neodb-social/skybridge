# 🌁 NeoDB Sky Bridge

NeoDB Sky Bridge relays public AT Protocol records (e.g. popfeed) into the Fediverse
as NeoDB-compatible ActivityPub activities. 

Any AT Protocol user may opt out by themselves (verified via atproto OAuth).

Any NeoDB server may subscribe to the relay service (`https://SKYBRIDGE_DOMAIN/actor`) to receive the activities.

## How it works

```
Jetstream (atproto firehose)  ─┐
   or replayed fixtures        │   ┌───────────── translate ─────────────┐
                               ▼   │ popfeed record → NeoDB AP object    │
  filter popfeed collections   ├──►│  (one Note per author+work, with    │
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
it. NeoDB catalog semantics ride alongside, matching NeoDB's wire format
(verified against its `takahe/ap_handlers.py` + `catalog/sites/fedi.py`
ingest code):

- `tag` carries exactly one typed catalog ref (`Movie`/`TVShow`/`TVSeason`/
  `Edition`/`Game`/`Album`/`Podcast`) with `href`/`name`/`image` — this is how
  a NeoDB peer locates the work; posts without one are dropped.
- `relatedWith` holds the mark facets — `Status` (shelf mark), `Rating`,
  `Comment` — each with the required `id`/`href`/`attributedTo`/`published`/
  `updated` envelope and a `withRegardTo` link to the catalog item.
- The catalog item at `https://<domain>/catalog/<type>/<id>` is served in
  NeoDB's ItemSchema shape (`type` = catalog type, `id` = the URL itself,
  `display_title`, `cover_image_url`) with `external_resources` (imdb / tmdb /
  igdb-slug / steam / musicbrainz URLs) plus top-level `imdb`/`isbn`, so peers
  merge it with items they already know instead of minting duplicates.

Deletes emit a `Delete` referencing a `Tombstone`. We emit `Note`s only —
never `Article`/titled `Review` objects.

| popfeed record | becomes |
|---|---|
| `social.popfeed.feed.review` | `Note` + `Rating` (0-10) and, when there is review text, an untitled `Comment` `withRegardTo` the work (never a titled `Review`/`Article`); `containsSpoilers` sets `sensitive` + a CW `summary` |
| `social.popfeed.feed.list` | archived only (no AP emission): stored for `listUri` resolution and future NeoDB `Collection` mapping |
| `social.popfeed.feed.listItem` | on a shelf-type list: `Note` + a `Status` mark (`status` derived from `listType`, including compound types like `watched_movies`) `withRegardTo` the work — folded into the review's Note when the same author reviewed the same work (see below). On a status-less list: archived only (collection membership is not bridged) |
| `social.popfeed.actor.profile` | no `Note`: refreshes the bridged actor's display name/avatar and emits an `Update(Person)` directly to that author's followers (never relayed as an `Announce`); not archived — it's identity metadata, not content |

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

Known but not bridged: 
- `social.popfeed.feed.post` (legacy)
- `social.popfeed.feed.reaction` (emoji reactions; maybe later `Like`/`EmojiReact` in AP)
- per-episode `watchedEpisodes` array on tv listItems.
- `app.bsky.actor.profile` on Jetstream (deliberately not watched — that would
  stream every profile edit network-wide); it's instead re-fetched as a
  fallback whenever a `social.popfeed.actor.profile` event arrives.

---

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SKYBRIDGE_DOMAIN` | `localhost:8000` | Public host of this relay (the single source of identity) |
| `SKYBRIDGE_SCHEME` | `https` (`http` for localhost) | URL scheme |
| `SKYBRIDGE_DATA` | `./data` | Folder for all mutable state (`skybridge.db`, `relay_key.pem`); under compose it is the host folder bind-mounted to the container's `/data` |
| `SKYBRIDGE_PORT` | `8000` | Host port docker compose publishes the server on (compose-only) |
| `SKYBRIDGE_JETSTREAM` | public jetstream2 us-east | Jetstream WebSocket endpoint |
| `SKYBRIDGE_RELAY_KEY` | **required** | Relay actor private key (PEM); alternatively place a PEM at `$SKYBRIDGE_DATA/relay_key.pem` |

The relay actor signs outbound activities with an RSA key that **you must
provide** — either as `SKYBRIDGE_RELAY_KEY` in `.env` (compose supports
quoted multi-line values) or as a PEM file at `$SKYBRIDGE_DATA/relay_key.pem`.
Startup fails if neither is present. To generate one:

```bash
printf 'SKYBRIDGE_RELAY_KEY="%s"\n' \
  "$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048)" >> .env
```

Keep the key safe and back it up — losing it changes the relay's ActivityPub
identity, and peers that cached the old public key will reject signatures
until they re-fetch the actor.
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

# Seed from a single DID's existing popfeed records.
uv run python -m skybridge backfill did:plc:i6k6scfcdaup4e2va33nkprb

# Replay a captured JSONL fixture through the full pipeline (offline).
uv run python -m skybridge replay fixtures/jetstream_sample.jsonl --reset
```

### Docker

```bash
cp .env.example .env   # set SKYBRIDGE_DOMAIN (+ SKYBRIDGE_INGEST=1 to go live)
printf 'SKYBRIDGE_RELAY_KEY="%s"\n' \
  "$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048)" >> .env
docker compose up -d   # serves on :8000; state lives in ./data (bind mount)
```

The container runs as uid 1000; on Linux make sure the data folder is
writable by it (`mkdir -p data && chown 1000 data`).

Every push to `main` runs the checks and publishes multi-arch
(amd64/arm64) images to Docker Hub as `neodb/skybridge` (`latest` + commit sha tags) 

### Endpoints

- Discovery: `/.well-known/webfinger`, `/.well-known/nodeinfo`, `/nodeinfo/2.1`
- Relay actor: `GET /actor`, shared inbox `POST /inbox`, `POST /actor/inbox`
- Bridged actors: `GET /users/{handle}` (+ `/inbox` `/outbox` `/followers`)
- Objects: `GET /users/{handle}/posts/{rkey}` (Note / Tombstone),
  `GET /catalog/{type}/{id}` (catalog work)
- UI / stats: `GET /` (dashboard), `GET /archive`, `GET /archive/{at_uri}`
  (original record vs. translated AP side-by-side), `GET /catalog`, `GET /stats`
- Opt-out: `GET /optout` (status form), `POST /optout` (starts the sign-in),
  `GET /oauth/client-metadata.json`, `GET /oauth/callback` — users prove
  control of their account via **AT Protocol OAuth** against their own
  authorization server (PAR + PKCE + DPoP); no passwords ever touch the relay
  and tokens are discarded right after the identity check

## Development

make sure CI is clean before commit

```bash
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
```
