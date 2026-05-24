# fetchtv

Download **Fetch TV** PVR recordings over UPnP/DLNA — works against any Mighty (M616T) or Gen-2 Fetch TV box on your LAN. No Fetch credentials, no cloud account. CLI + programmatic ESM API.

- **GitHub:** <https://github.com/furey/fetchtv>
- **npm:** <https://www.npmjs.com/package/fetchtv>
- **License:** GPL-3.0

---

## Quick start

### List recordings on the box

```sh
docker run --rm --network host furey/fetchtv recordings
```

### Download new episodes of a show to the host

```sh
docker run --rm --network host \
  -v /path/on/host:/downloads \
  furey/fetchtv recordings \
  --folder "Bluey" \
  --save /downloads
```

### Auto-discover the box on the LAN

```sh
docker run --rm --network host furey/fetchtv info
```

### Browse shows on the box

```sh
docker run --rm --network host furey/fetchtv shows
```

---

## Why `--network host`?

Fetch TV discovery uses SSDP multicast (`239.255.255.250:1900`), which doesn't traverse Docker's default bridge network. Host networking is the simplest fix.

If you already know your box's IP, skip discovery and run on bridge networking:

```sh
docker run --rm furey/fetchtv recordings --ip 192.168.1.20
```

---

## Common flags

| Flag                   | Purpose                                            |
| ---------------------- | -------------------------------------------------- |
| `--ip <addr>`          | Target a specific Fetch TV IP, skip discovery      |
| `--port <num>`         | Fetch TV port (default `49152`)                    |
| `--folder <name>`      | Filter to a specific show/folder (repeatable)      |
| `--save <path>`        | Output directory for downloads                     |
| `--overwrite`          | Re-download files that already exist locally       |
| `--debug`              | Verbose UPnP/SSDP logs                             |

Full CLI reference, path-template variables, and the programmatic ESM API are documented in the [GitHub README](https://github.com/furey/fetchtv#readme).

---

## Programmatic use

`fetchtv.js` is import-safe — `docker run` is one way to use it, but the same module powers Node-based watchers, dashboards, and integrations. See the [Programmatic API](https://github.com/furey/fetchtv#programmatic-api) section upstream.

---

## Tags

| Tag                | What                                  |
| ------------------ | ------------------------------------- |
| `latest`           | Most recent release                   |
| `1.7`, `1.6`, …    | Pinned major.minor (e.g. `1.7`)       |
| `1.7.0`, `1.6.1`, …| Pinned exact version                  |

Multi-arch: `linux/amd64`, `linux/arm64`.

---

## Issues / contributing

Please use the [GitHub issue tracker](https://github.com/furey/fetchtv/issues).
