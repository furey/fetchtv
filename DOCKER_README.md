# fetchtv

[![NPM Version](https://img.shields.io/npm/v/fetchtv)](https://www.npmjs.com/package/fetchtv)
[![Docker Pulls](https://img.shields.io/docker/pulls/furey/fetchtv)](https://hub.docker.com/r/furey/fetchtv)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![GitHub Repository](https://img.shields.io/badge/GitHub-furey/fetchtv-blue)](https://github.com/furey/fetchtv)

Download Fetch TV PVR recordings over the local network.

No Fetch credentials or cloud account required.

Based on [`lingfish/fetchtv-cli`](https://github.com/lingfish/fetchtv-cli) (Python) which is based on [`jinxo13/FetchTV-Helpers`](https://github.com/jinxo13/FetchTV-Helpers) (also Python).

## Contents

- [Quick start](#quick-start)
- [Why `--network host`?](#why---network-host)
- [Common flags](#common-flags)
- [Programmatic use](#programmatic-use)
- [Disclaimer](#disclaimer)
- [Support](#support)

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

## Why `--network host`?

Fetch TV discovery uses SSDP multicast (`239.255.255.250:1900`), which doesn't traverse Docker's default bridge network. Host networking is the simplest fix.

If you already know your box's IP, skip discovery and run on bridge networking:

```sh
docker run --rm furey/fetchtv recordings --ip 192.168.1.20
```

## Common flags

| Flag              | Purpose                                       |
| ----------------- | --------------------------------------------- |
| `--ip <addr>`     | Target a specific Fetch TV IP, skip discovery |
| `--port <num>`    | Fetch TV port (default `49152`)               |
| `--folder <name>` | Filter to a specific show/folder (repeatable) |
| `--save <path>`   | Output directory for downloads                |
| `--overwrite`     | Re-download files that already exist locally  |
| `--debug`         | Verbose UPnP/SSDP logs                        |

Full CLI reference, path-template variables, and the programmatic ESM API are documented in the [GitHub README](https://github.com/furey/fetchtv#readme).

## Programmatic use

`fetchtv.js` is import-safe—`docker run` is one way to use it, but the same module powers Node-based watchers, dashboards, and integrations. See the [Programmatic API](https://github.com/furey/fetchtv#programmatic-api) section upstream.

## Disclaimer

This project:

- Is licensed under the [GNU GPLv3 License](./LICENSE.txt).
- Is not affiliated with or endorsed by Fetch TV.
- Is a derivative work based on [`lingfish/fetchtv-cli`](https://github.com/lingfish/fetchtv-cli).
- Is written with the assistance of AI and may contain errors.
- Is intended for educational and experimental purposes only.
- Is provided as-is with no warranty—please use at your own risk.

## Support

If you've found this project helpful consider supporting my work through:

[Buy Me a Coffee](https://www.buymeacoffee.com/furey) | [GitHub Sponsorship](https://github.com/sponsors/furey)

Contributions help me continue developing and improving this tool, allowing me to dedicate more time to add new features and ensuring it remains a valuable resource for the community.
