# Floppy watch-provider plugin for Silo

Connects Silo profiles to a self-hosted [Floppy](https://github.com/dannyvfilms/Floppy) instance through Silo's `watch_sync_provider.v1` plugin contract.

## Capabilities

- Validates each profile's Floppy API token without persisting it in the plugin.
- Imports movie and episode watch history.
- Imports durable movie and episode resume progress.
- Exports completed movie and episode watches.
- Sends live playback start, pause, and stop events.
- Uses provider timestamps and Floppy history lookups to make completed-watch retries idempotent.

The plugin deliberately does not advertise favorites, watchlists, or unwatched export. Floppy does not currently expose those concepts with the stable identifiers and semantics required by Silo's reconciliation contract. Silo only enables capabilities a plugin explicitly advertises.

## Setup

1. Install the plugin and set **Base URL** to the root URL of the Floppy instance, such as `https://floppy.example.com`.
2. In Silo, connect each profile with that Floppy user's API token.

The base URL is installation-wide configuration. Tokens are profile-scoped credentials encrypted and owned by the Silo host; the plugin receives them only for the duration of an RPC and does not store or log them.

The plugin requires a Floppy release that provides:

- `GET /apis/listenbrainz/1/validate-token`
- `GET /api/v1/history/`
- `GET /api/v1/playback/progress/`
- `POST /api/v1/scrobble/`

## Development

The plugin depends on `silo-plugin-sdk` v0.13.0 or newer.

```bash
make test
make build
./plugin manifest
```

`make build-all` produces static binaries for the platforms declared in `manifest.json`.

## License

AGPL-3.0-only.
