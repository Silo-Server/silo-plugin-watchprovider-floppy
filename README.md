# Floppy watch-provider plugin for Silo

Connects Silo profiles to a self-hosted [Floppy](https://github.com/dannyvfilms/Floppy) instance through Silo's `watch_sync_provider.v1` plugin contract.

## Capabilities

- Validates each profile's Floppy API token without persisting it in the plugin.
- Imports movie and episode watch history.
- Imports durable movie and episode resume progress.
- Exports completed movie and episode watches.
- Sends live playback start, pause, and stop events.
- Imports and exports movie and series ratings.
- Uses provider timestamps and Floppy history lookups to make completed-watch retries idempotent.

The plugin deliberately does not advertise favorites, watchlists, or unwatched export. Floppy does not currently expose those concepts with the stable identifiers and semantics required by Silo's reconciliation contract. Silo only enables capabilities a plugin explicitly advertises, so listing the series media type for ratings does not turn on series favorites or watchlist sync.

## Ratings

This version needs a Silo server release that supports rating sync for watch-provider plugins. An older server handles it in one of two ways:

- A server that parses the manifest built into the plugin binary rejects the update.
- A server that installs from the release archive and discards manifest fields it does not know ignores the rating flags and the series media type. The plugin then syncs history, progress, and scrobbles without ratings.

Floppy scores titles from 0 to 10 in steps of 0.1, and Silo's plugin contract uses whole ratings from 1 to 10:

- Import rounds a Floppy score half up and clamps it to 1–10, so 7.5 becomes 8 and 0 becomes 1.
- Export writes the rating as the title's score, and a removal clears the score.

Every rating import reads the full set of rated movies, then rated TV shows, as a complete snapshot. Floppy has no feed of rating changes, so there is no incremental import, and Silo treats a title missing from the snapshot as unrated. Each page re-reads the last title of the page before it. If a rating added or cleared during the read has shifted the list, or the plugin cannot read a listed title's rating, the plugin abandons the snapshot instead of leaving titles out, and the next sync starts over.

Floppy stores a rating on the title it tracks, so:

- Only TMDB titles sync. Floppy tracks movies and TV from TMDB or manual entries, and the plugin cannot address manual entries or TV that Floppy keeps in its anime library.
- Silo can rate a title only after Floppy tracks it. A rating for an untracked title is rejected as "Floppy is not tracking this title", and removing a rating from an untracked title succeeds without a change.
- Floppy can hold several entries for one title, one per viewing, and shows the score on the most recently active entry that has one. Floppy's rated list reports the newest entry's score instead, so import reads each rated title's entries to find the score Floppy shows, one extra request per title. Export puts the rating on the newest entry, and also on a more recently active entry that shows a different score. A removal clears the score on every entry.
- For a movie watched through Floppy's play API, Floppy lists per-play records instead of entries, and the plugin can reach only the newest entry. It writes the rating there, and a snapshot fails if the score Floppy shows sits on an older entry.

A scoped Floppy token needs `watchlist:read` to import ratings and `watchlist:write` to send them. Floppy's tracking preset includes both, and legacy full-access tokens need no change. Without `watchlist:write`, each rating update fails with a message that the token needs that scope, and history, progress, and scrobble sync continue.

## Setup

1. Install the plugin.
2. In Silo's watch-provider settings, connect each profile with its Floppy server URL and that Floppy user's API token.

The server URL and token are profile-scoped connection data encrypted and owned by the Silo host. Different profiles can connect to different Floppy servers. A saved installation-wide URL from plugin v0.1.0 remains an invisible upgrade fallback for existing connections, but v0.2.0 no longer exposes or accepts a global Floppy server setting and every new connection supplies its own URL.

The plugin requires a Floppy release that provides:

- `GET /apis/listenbrainz/1/validate-token`
- `GET /api/v1/history/`
- `GET /api/v1/media/{media_type}/` with the `rating=rated` filter
- `PATCH /api/v1/media/{media_type}/{source}/{media_id}/`
- `GET /api/v1/media/{media_type}/{source}/{media_id}/history/`
- `PATCH /api/v1/media/{media_type}/{source}/{media_id}/history/{consumption_id}/`
- `GET /api/v1/playback/progress/`
- `POST /api/v1/scrobble/`

## Development

The plugin builds against the `silo-plugin-sdk` rating contract (v0.17.0 once tagged).

```bash
make test
make build
./plugin manifest
```

`make build-all` produces static binaries for the platforms declared in `manifest.json`.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to
authentication, reconciliation, idempotency, or the watch-sync contract should
start as an issue.

## License

AGPL-3.0-only.
