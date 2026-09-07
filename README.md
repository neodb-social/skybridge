# 🌁 NeoDB Sky Bridge

NeoDB Sky Bridge relays public AT Protocol records (e.g. popfeed,
[BookHive](https://github.com/nperez0111/bookhive) and
[teal.fm](https://github.com/teal-fm/teal)) into the Fediverse
as NeoDB-compatible ActivityPub activities. 

Any AT Protocol user may opt out by themselves (verified via atproto OAuth).

Skybridge is an ActivityPub server: any Fediverse (or NeoDB) account can
follow a bridged user directly at `https://SKYBRIDGE_DOMAIN/users/{handle}`
and receive their activities as regular followers. It additionally publishes
every public post through external relays admin configures with
`SKYBRIDGE_RELAYS`, so peers connected to same relay see bridged content without 
following individually.

## How it works

```
Jetstream v2 (atproto firehose)┐
  live tail / archive replay   │
   or replayed fixtures        │   ┌───────────── translate ─────────────┐
                               ▼   │ popfeed record → NeoDB AP object    │
  filter popfeed collections   ├──►│  (one Note per author+work, with    │
  resolve DID → bridged Person │   │   Status/Rating/Comment relatedWith │
  (mint RSA keypair on sight)  │   │   the work) in Create/Update/Delete │
                               │   └─────────────────┬───────────────────┘
                               ▼                     ▼
                      persist to SQLite        enqueue delivery
                      (archive + dedup)              └─ fanout: author-signed activity →
                                                          configured relays + followers
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

Jetstream `identity` events (handle changes) take the same path: the actor's
handle is updated and an `Update(Person)` goes to its followers. Neither an
identity event nor a profile edit ever *mints* an actor — we bridge people
because of what they post.

Because a handle is also the actor URL (`/users/<handle>`), a rename would
strand every id already federated. Two rules keep them alive:

- The retired handle is kept as an alias. `/users/<old>` (plus its inbox,
  outbox, followers and WebFinger record) resolves to the same account and
  `301`s to the live name; already-published post URLs under it keep
  dereferencing.
- Object ids are never recomputed from the current handle. An `Update` or
  `Delete` names the id peers actually received, whatever handle minted it.

`handle.invalid` (reported when a handle stops resolving back to its DID) is
not treated as a rename: every account in that state reports the same value,
so the actor keeps the last name we know it by until a real one arrives.

A handle points at one DID at a time, so when a name moves to another account
the previous holder is pushed onto its synthetic `<did-tail>.did` handle.
Leaving two rows on one name would let one account's URL, WebFinger record and
HTTP signature `key_id` resolve to the other's.

Known limit: a renamed actor is not announced with a `Move`, so remote servers
keep following the old id (which still works) instead of migrating to the new
one.

### Visibility preferences

Bluesky publishes two "speech is not reach" toggles that a bridge is expected
to honour. Both are read off the atproto account and never set on this side.

**Hide from algorithmic recommendations.** The
`app.bsky.actor.contentVisibilityDeclaration` record (rkey `self`, field
`hideFromAlgorithmicRecommendations`; a missing record means false, per the
lexicon). It becomes `discoverable: false` on the bridged `Person` — which on
Mastodon drops the account from the profile directory, from follow
recommendations and from trends, and on NeoDB also withholds consent to being
featured in someone's collection (FEP-7aa9). Watched on Jetstream, so a toggle
publishes an `Update(Person)` to that author's followers straight away.

The flag is written under *both* `discoverable` and `toot:discoverable`,
because the two receivers read it in incompatible ways: Mastodon never compacts
a fetched actor and reads the raw key, while NeoDB/takahe compacts against the
document's own context and then reads `toot:discoverable`. Our `@context`
declares the `toot` prefix but not the `discoverable` alias, which is what
keeps the two keys from folding into one array. Nothing is emitted when the
preference is unset: both receivers already default to permissive, and
volunteering `discoverable: true` would opt every bridged author into
directories nobody asked for.

**Hide from logged-out users.** The `!no-unauthenticated` self-label on the
`app.bsky.actor.profile` record. ActivityPub cannot say "signed-in readers
only" while staying publicly federated, so this is honoured as far as it can
be, and no further:

- Our own web pages carry `noindex, nofollow`, drop their link-preview tags
  and their schema.org JSON-LD, and show identity only — no post list on the
  profile, no source record on the post page. `/archive`, its detail pages and
  the catalog item listings hide the author's records entirely. The one thing
  that still counts them is a catalog item's rating average, which names and
  quotes nobody (see Endpoints). The AP representation at the same URLs is
  untouched — a peer that follows the author is exactly the audience the label
  still allows.
- Posts are addressed unlisted (`to: [followers]`, `cc: [as:Public]`), which
  keeps them out of the public, local, federated and hashtag timelines and out
  of trends. Delivery is unaffected: neodb-relay redistributes on `to` *or*
  `cc`, and takahe files a `cc`-public post as unlisted rather than dropping
  it.
- The actor also gets `indexable: false`.

Not honoured: a permalink on a Mastodon-family peer stays readable by anyone,
signed in or not. Note that NeoDB peers already hide every remote author's
marks from logged-out visitors, since they create remote identities with
`anonymous_viewable=False`.

This label is not watched on Jetstream (it rides on `app.bsky.actor.profile`,
which we deliberately do not tail). It is read when an actor is minted and
re-read whenever the actor is refreshed, so a toggle lands on the next refresh
rather than at once. Neither preference is ever *cleared* by a refresh — a
failed fetch and a preference turned off both arrive as an empty record, and
the two must not be confused when one of them means "publish this person more
widely again". Clearing has exact signals of its own: the declaration's own
Jetstream commit (a delete and a `false` both mean false), and a profile record
that comes back without the label.

**Signing in resyncs the account.** Completing the atproto OAuth flow on
`/manage` proves control of the DID, so the callback re-reads the whole account
from the network before showing the status page: handle, display name, avatar
and both preferences. Anything that moved is published to the author's
followers as an `Update(Person)`, and the status card shows which preferences
are currently being carried.

This is the one path allowed to turn a preference *off*, because the account
holder is present and expects the settings they hold right now to apply. It
still refuses to confuse an absent record with a failed fetch, so each flag is
lowered only when the read it came from answered — its own read, not a
sibling's. The three reads are three separate requests, and one succeeding
says nothing about another that timed out. atproto reports a missing record as
`400 {"error": "RecordNotFound"}`, which is an answer; nothing coming back at
all is not. Two things it will never do — mint an actor for a DID we do not bridge
(signing in is not activity), and touch an account that opted out (its records
were retracted, so nobody should hear about its actor again). A handle is
applied only when the DID document actually resolved one, so an unreachable
PLC cannot rename a live actor onto its `<did-tail>.did` placeholder.

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
  stream every profile edit network-wide); it's instead re-fetched whenever a
  `social.popfeed.actor.profile` event arrives, both as a name/avatar fallback
  and to re-read the `!no-unauthenticated` label.

`app.bsky.actor.contentVisibilityDeclaration` *is* watched network-wide (see
Visibility preferences above): unlike a profile edit it is a rare, low-volume
record, and like one it only ever updates an actor we already bridge.

`uv run python -m skybridge discover` keeps this list honest: it subscribes to
`social.popfeed.*` / `buzz.bookhive.*` / `fm.teal.*` (Jetstream v2 accepts
namespace wildcards) and reports every collection seen, flagging the ones we
don't bridge. Ingestion itself still asks for the explicit list — a wildcard
subscription would also pull in `buzz.bookhive.catalogBook`, whose records
carry multi-KB author biographies we have no use for.

### BookHive

[BookHive](https://github.com/nperez0111/bookhive) is a separate atproto app —
a decentralized Goodreads. Unlike popfeed, which splits a user action across a
`review` and a `listItem`, one BookHive action lives in a *single*
`buzz.bookhive.book` record that already carries the shelf status, star rating
and review together, so it bridges to ONE AP `Note` through the same
non-paired path (no pair-merging). The work type is `book` (AP `Edition`), so a
BookHive book and a popfeed book that share an ISBN merge into one catalog
entry.

| BookHive record | becomes |
|---|---|
| `buzz.bookhive.book` | a single `Note` carrying, as available: a `Status` mark (`status` → wishlist / progress / complete / dropped, from `wantToRead` / `reading` / `finished` / `abandoned`), a `Rating` (`stars`, 1-10), and — when there is review text — an untitled `Comment` `withRegardTo` the work. A status-only shelf-add (no stars/review) still bridges, leading with a reading verb ("Wants to read", "Reading", …) |

Book identity comes from the record's `identifiers` (isbn13 → isbn10 →
goodreadsId → hiveId; `hiveId` is always present, so a work always mints). The
catalog item exposes the `isbn` and a Goodreads `external_resource` so NeoDB
peers merge it with editions they already know.

Known but not bridged:
- `buzz.bookhive.buzz` (comments/replies on a book; like popfeed reactions)
- `buzz.bookhive.hiveBook` / `buzz.bookhive.catalogBook` (the app's own catalog
  entries, not user activity)
- the `cover` blob (a PDS blob, not a URL): no poster is derived yet, so the
  Note relies on the catalog-item tag for imagery

### teal.fm

[teal.fm](https://github.com/teal-fm/teal) is a music scrobbler on atproto:
one `fm.teal.feed.play` record is written for every track a user listens to,
naming the track, the artists and the release (album), with MusicBrainz ids
(`mbid:<uuid>`) when the tracker could match them. Two NSIDs are live on the
network and both are bridged: `fm.teal.feed.play` and the pre-July-2026
`fm.teal.alpha.feed.play`, which older trackers still write; the record shapes
agree on every field used here.

The bridged work is the **release**, as a NeoDB `Album` (category `music`).
Its identity is `releaseMbId`, exposed as a `https://musicbrainz.org/release/…`
external resource that NeoDB resolves; when a play carries no MusicBrainz id
but its `originUri` is an Apple Music track URL, the album id in that URL
identifies the release instead (`https://music.apple.com/album/…`, also
resolved by NeoDB). A play that names no release — only a recording id, or a
Last.fm track page — mints no work and is archived without AP emission: NeoDB
has no track item to mark, and a scrobbler makes many of these.

A scrobbler writes one record per track, so bridging each play as its own Note
would post a 12-track album twelve times. Plays are instead bridged as **ONE
`Note` per (author, release)**:

- The first play of a release publishes the Note (`Create`), anchored on that
  play's rkey. Its content names only the album and the artists of that play
  (never a track or a play count) and carries a `Status` of `complete`
  (NeoDB's "listened"); there is no `Rating` and no `Comment`.
- Every further play of the same release is archived, grouped on the same
  work, and **sends nothing**: the Note is re-derived and compared with the
  stored one (ignoring `updated` stamps), and an `Update` goes out only when
  it actually differs — a release title that only a later play supplied, or a
  visibility preference that changed since (re-derivation happens on the next
  event for that release, not when the preference flips).
- Deleting the anchoring play `Delete`s the Note; the newest surviving play of
  the release re-publishes it under its own rkey. Deleting any other play just
  re-derives the Note, which almost always changes nothing.

| teal.fm record | becomes |
|---|---|
| `fm.teal.feed.play` / `fm.teal.alpha.feed.play` | one `Note` per (author, release): "Listened to *Album* by *Artists*" with a `Status` of `complete` `withRegardTo` the album; later plays of the same release are archived silently |

`published` is the anchoring play's `playedTime` (a play has no `createdAt`).

Known but not bridged:
- `fm.teal.actor.status` / `fm.teal.alpha.actor.status` ("now playing": rkey
  `self`, rewritten on every track, expiring ten minutes later)
- `fm.teal.actor.profile` (display name + avatar blob; the bridged actor's
  identity comes from the bsky profile fallback) and
  `fm.teal.actor.profileStatus` (onboarding progress)
- `recordingMbId` / `trackMbId` / `isrc`: track-level ids with no NeoDB item
  type to land on

### Account lifecycle

Jetstream v2 reports atproto account state, which the bridge acts on. Only a
`deleted` account is permanent: everything bridged from it is retracted, the
same way an opt-out is, but *without* recording an opt-out — the DID is gone,
not making a standing choice, and a stored opt-out would wrongly suppress it
if it ever returned.

Every other inactive status (`deactivated`, `suspended`, `takendown`) is
treated as reversible: ingestion stops for that account and **nothing is
retracted**, so someone who deactivates for a week and comes back finds their
federated history intact. Ingestion resumes when the account goes active
again.

### Importing history

The live socket only reaches back a bounded window (36 hours on Bluesky's
instances). Jetstream v2 additionally serves its whole archive over HTTP, so
history can be imported without crawling every PDS. This is what
`SKYBRIDGE_JETSTREAM_API_KEY` is for — nothing else needs it.

An import is designed to run on a live server:

- **It cannot overwrite newer data.** Each record keeps the highest `seq`
  applied to it, and an import is bounded above by the live ingest cursor, so
  an archived event can never regress a record the live tail has moved past.
  The same mark makes Jetstream's at-least-once redelivery idempotent.
- **It honours opt-out.** Opted-out DIDs are skipped, and because the import
  runs *inside the server process* an opt-out mid-import can cancel it. An
  out-of-process import could not be stopped.
- **It does not deliver by default.** Replaying history would flood every
  subscriber with `Create` activities, so imported records are archived
  silently unless delivery is explicitly requested.
- **It is resumable.** Progress is persisted per segment, so a metering `429`
  or a restart continues rather than re-downloading.

Cost is dominated by how much of the archive gets fetched, and two things keep
that down for a rare collection:

- The planner works from bloom filters, so it over-selects. Where it asks for a
  whole segment (`mode: "segment"`), the importer instead Range-fetches the
  file header and the segment's **collection index**, which names the blocks
  holding each NSID, and downloads only those — orders of magnitude less than
  the whole file. A segment whose index can't be read falls back to the full
  download.
- Blocks are fetched with bounded concurrency and applied in index order, since
  the run is a chain of round trips rather than a bandwidth problem.

Run an estimate first, and read it carefully: the plan's `planner_entries`
counts the planner's own work units, **not** records, and understates the
records recovered by a wide margin. `estimated_bytes` is the number to judge.

Start, watch and cancel an import from the admin panel on `/manage` (visible
to accounts listed in `SKYBRIDGE_ADMINS` after signing in), or queue one from
the CLI with `python -m skybridge import`.

---

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SKYBRIDGE_DOMAIN` | `localhost:8000` | Public host of this relay (the single source of identity) |
| `SKYBRIDGE_SCHEME` | `https` (`http` for localhost) | URL scheme |
| `SKYBRIDGE_DATA` | `./data` | Folder for all mutable state (`skybridge.db`, `relay_key.pem`); under compose it is the host folder bind-mounted to the container's `/data` |
| `SKYBRIDGE_PORT` | `8000` | Host port docker compose publishes the server on (compose-only) |
| `SKYBRIDGE_JETSTREAM` | public Jetstream **v2** us-east | Jetstream WebSocket endpoint; a v1 endpoint still works, but import/discovery are v2-only |
| `SKYBRIDGE_JETSTREAM_API_KEY` | unset | Jetstream v2 archive key, for importing history. Not needed for normal operation — the live socket takes no key |
| `SKYBRIDGE_ADMINS` | unset | Comma/space-separated DIDs and/or handles that get the admin panel on `/manage`. Prefer DIDs: handles are transferable |
| `SKYBRIDGE_RELAY_KEY` | **required** | Service actor private key (PEM); alternatively place a PEM at `$SKYBRIDGE_DATA/relay_key.pem` |
| `SKYBRIDGE_RELAYS` | unset | Comma/space-separated relay inbox URLs to publish through (Mastodon-style); empty = pure normal-server mode |

The service actor signs outbound activities with an RSA key that **you must
provide** — either as `SKYBRIDGE_RELAY_KEY` in `.env` (compose supports
quoted multi-line values) or as a PEM file at `$SKYBRIDGE_DATA/relay_key.pem`.
Startup fails if neither is present. To generate one:

```bash
printf 'SKYBRIDGE_RELAY_KEY="%s"\n' \
  "$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048)" >> .env
```

Keep the key safe and back it up — losing it changes the server's ActivityPub
identity, and peers that cached the old public key will reject signatures
until they re-fetch the actor.
| `SKYBRIDGE_INGEST` | unset | set to `1` to start live ingestion inside `serve` |
| `SKYBRIDGE_BACKFILL_LIMIT` | `1000` | max records fetched per user-triggered "Import recent activity" run (total; reviews and shelf items are budgeted before archive-only lists) |
| `SKYBRIDGE_BACKFILL_DAYS` | `7` | only records written within the last N days (by TID rkey, falling back to `createdAt`) are re-published by an import |
| `SKYBRIDGE_LOG` | `INFO` | log level |
| `SKYBRIDGE_SENTRY_DSN` | unset | optional; enables Sentry error reporting and a `atproto.record_ingested` counter metric with `collection`/`operation` attributes |

---

## Install & run

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync            # create .venv and install runtime + dev deps from uv.lock
```

The CLI has six subcommands (run them via `uv run`):

```bash
# Serve the ActivityPub endpoints + dashboard (set SKYBRIDGE_INGEST=1 to also
# stream live popfeed activity in the same process).
SKYBRIDGE_DOMAIN=bridge.example.social uv run python -m skybridge serve --port 8000

# Stream live popfeed activity from Jetstream (Ctrl-C to stop; --limit N to bound).
SKYBRIDGE_DOMAIN=bridge.example.social uv run python -m skybridge ingest

# Seed from a single DID's existing popfeed records (--days N to only replay
# recent ones, --limit to cap the fetch; default SKYBRIDGE_BACKFILL_LIMIT).
# Signed-in users can trigger the same thing from /manage via the "Import
# recent activity" button. Avoid running with --deliver while users may be
# opting out live: opt-out can only cancel imports inside the server process.
uv run python -m skybridge backfill did:plc:i6k6scfcdaup4e2va33nkprb

# Replay a captured JSONL fixture through the full pipeline (offline).
uv run python -m skybridge replay fixtures/jetstream_sample.jsonl --reset

# Survey which collections are being published under the bridged namespaces.
uv run python -m skybridge discover --seconds 120

# Import history from the Jetstream v2 archive. ALWAYS estimate first — the
# archive is billed by bytes downloaded, and a rare collection plans far more
# of it than the matching-record count suggests.
uv run python -m skybridge import --dry-run
uv run python -m skybridge import            # queues it; the server runs it
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
- Service actor: `GET /actor`, shared inbox `POST /inbox`, `POST /actor/inbox`
  — follow bridged users directly instead; a Follow of the service actor
  now gets a `Reject` (see relays above for the Mastodon-style alternative)
- Bridged actors: `GET /users/{handle}` (+ `/inbox` `/outbox` `/followers`) —
  inbound `Like`/`Undo(Like)` on a local post is stored and forwarded to
  configured relays
- Objects: `GET /users/{handle}/posts/{rkey}` (Note / Tombstone),
  `GET /catalog/{type}/{id}` (catalog work)
- UI / stats: `GET /` (dashboard), `GET /archive`, `GET /archive/{at_uri}`
  (original record vs. translated AP side-by-side), `GET /catalog`, `GET /stats`

Each of these URLs serves ActivityPub to a peer (`Accept: application/activity+json`)
and HTML to a browser. The HTML views cross-link: the profile and catalog item
pages list the 100 most recently bridged posts, `/archive` links each published
row to its post page, and a post page names both its ActivityPub id and the
`at://` record behind it.

The HTML also carries schema.org JSON-LD, saying what the ActivityPub document
at the same URL says in NeoDB's vocabulary, in the one search engines and
unfurlers read. A post carrying a rating or review text embeds a `Review`; a
catalog item embeds the work itself (`Movie` / `Book` / `VideoGame` / …, with
its identifier URLs as `sameAs`), plus the reviews it lists and an
`aggregateRating`, which is also shown on the page. Spoiler-marked review text
is left out of both, as it is out of the link-preview tags.

The average counts **every** rating bridged for the item, including from
authors carrying `!no-unauthenticated`, whose posts are not listed on the page
and whose reviews are not embedded. The label hides an author's posts, not the
existence of a score: an average names nobody and quotes nobody. A retracted
record does drop out of it — an opt-out tombstones the data rather than hiding
it.
- Manage (self-service opt-out / import): `GET /manage` (sign-in form; the
  account view once signed in),
  `POST /manage` (starts the sign-in), `GET /oauth/client-metadata.json`,
  `GET /oauth/callback` (opens the session), then `POST /manage/opt-out`,
  `POST /manage/opt-in`, `POST /manage/import`, `POST /manage/signout` from
  the account view — users prove control of their account via **AT Protocol
  OAuth** against their own authorization server (PAR + PKCE + DPoP); no
  passwords ever touch the relay, tokens are discarded right after the
  identity check, and the signed-in session is a short-lived in-memory
  cookie. "Import recent activity" backfills the account's existing records
  (capped at `SKYBRIDGE_BACKFILL_LIMIT`, re-publishing only the last
  `SKYBRIDGE_BACKFILL_DAYS` days) in the background, one import per account
  at a time, disabled while opted out. Known limit: content re-imported
  after an opt-out -> opt-in round trip is re-published under its original
  object ids, which peers that cache tombstones may reject

## Development

make sure CI is clean before commit

```bash
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
```
