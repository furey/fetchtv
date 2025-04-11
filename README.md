# fetchtv

[![NPM Version](https://img.shields.io/npm/v/fetchtv)](https://www.npmjs.com/package/fetchtv)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

A Node.js CLI tool to download Fetch TV recordings.

Based on [`lingfish/fetchtv-cli`](https://github.com/lingfish/fetchtv-cli) (Python) which is based on [`jinxo13/FetchTV-Helpers`](https://github.com/jinxo13/FetchTV-Helpers) (also Python).

## Contents

- [Demo](#demo)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)
- [Disclaimer](#disclaimer)
- [Support](#support)

## Demo

https://gist.github.com/user-attachments/assets/61dfab62-a715-4cc3-a4d1-93ee0db43827

## Quick Start

- [Install](#installation) `fetchtv`
- [Run](#usage) `fetchtv`

## Installation

- [NPX](#installation-npx) (Easiest)
- [Node.js from Source](#installation-nodejs-from-source)
- [Docker from Source](#installation-docker-from-source)

### Installation: NPX

> [!NOTE]<br>
> NPX requires [Node.js](https://nodejs.org/en/download) installed and running on your system (suggestion: use [Volta](https://volta.sh)).

The easiest way to install `fetchtv` is via NPX.

First, ensure Node.js is running:

```console
node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
```

Then, run `fetchtv` via NPX:

```console
npx fetchtv             # Run the tool
npx fetchtv@latest      # (optional) "@latest" ensures package is up-to-date
npx -y fetchtv@latest   # (optional) "-y" flag skips any prompts

npx fetchtv info
npx fetchtv shows
npx fetchtv recordings

# etc…
```

> If you encounter permissions errors with `npx` try running `npx clear-npx-cache` prior to running `npx -y fetchtv` (this clears the cache and re-downloads the package).

### Installation: Node.js from Source

> [!NOTE]<br>
> Node.js from source requires [Node.js](https://nodejs.org/en/download) installed and running on your system (suggestion: use [Volta](https://volta.sh)).

1. Clone the `fetchtv` repository:<br>
    ```console
    git clone https://github.com/furey/fetchtv.git
    ```
1. Navigate to the cloned repository directory:<br>
    ```console
    cd /path/to/fetchtv
    ```
1. Ensure Node.js is running:<br>
    ```console
    node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
    ```
1. Install Node.js dependencies:<br>
    ```console
    npm ci
    ```
1. Run `fetchtv`:<br>
    ```console
    node fetchtv.js
    node fetchtv.js info
    node fetchtv.js recordings
    node fetchtv.js shows

    # etc…
    ```

#### Optional: Link `fetchtv` Tool

You may optionally link the `fetchtv` tool to your system path for easier access:

```console
npm link
```

This will create a symlink to the `fetchtv` command in your global `node_modules` directory, allowing you to run it from anywhere in your terminal:

```console
fetchtv
fetchtv info
fetchtv shows
fetchtv recordings

# etc…
```

To uninstall the linked tool, run:

```console
npm unlink
```

### Installation: Docker from Source

> [!NOTE]<br>
> Docker from source requires [Docker](https://docs.docker.com/get-started/get-docker) installed and running on your system.

1. Clone the `fetchtv` repository:<br>
    ```console
    git clone https://github.com/furey/fetchtv.git
    ```
1. Navigate to the cloned repository directory:<br>
    ```console
    cd /path/to/fetchtv
    ```
1. Ensure Docker is running:<br>
    ```console
    docker --version # Ideally >= v27.x
    ```
1. Build the Docker image:<br>
    ```console
    docker build -t fetchtv .
    ```
1. Run the container:<br>
    ```console
    docker run -t --rm fetchtv
    docker run -t --rm fetchtv info
    docker run -t --rm fetchtv shows
    docker run -t --rm fetchtv recordings

    # etc…
    ```

#### UPnP/SSDP Discovery Issues

UPnP/SSDP discovery can be unreliable in Docker containers.

To work around this, it's recommended to specify your Fetch TV server's IP address directly with the `--ip` (and optionally `--port`) option when running the container. For example:

```console
docker run -t --rm fetchtv
docker run -t --rm fetchtv info --ip=192.168.86.71
docker run -t --rm fetchtv shows --ip=192.168.86.71
docker run -t --rm fetchtv recordings --ip=192.168.86.71

# etc…
```

## Usage

If you [installed via NPX](#installation-npx), you can run it from anywhere:

```command
npx fetchtv <COMMAND> [OPTIONS]
```

If you [installed from Node.js source](#installation-nodejs-from-source), you can run it from the cloned repo directory:

```command
cd /path/to/fetchtv
node fetchtv.js <COMMAND> [OPTIONS]
```

If you [linked the tool](#optional-link-fetchtv-tool) after installing from source, you can run it from anywhere:

```command
fetchtv <COMMAND> [OPTIONS]
```

| Command/Option   | Alias | Type      | Description                                                                     |
| ---------------- | ----- | --------- | ------------------------------------------------------------------------------- |
| `info`           |       | `command` | Returns Fetch TV server details                                                 |
| `recordings`     |       | `command` | List episode recordings                                                         |
| `shows`          |       | `command` | List show titles and not the episodes within                                    |
| `--ip`           |       | `string`  | Specify the IP Address of the Fetch TV server                                   |
| `--port`         |       | `number`  | Specify the port of the Fetch TV server (default: `49152`)                      |
| `--show`         | `-s`  | `array`   | Filter recordings to show titles containing the specified text (repeatable)     |
| `--exclude`      | `-e`  | `array`   | Filter recordings to show titles NOT containing the specified text (repeatable) |
| `--title`        | `-t`  | `array`   | Filter recordings to episode titles containing the specified text (repeatable)  |
| `--is-recording` |       | `boolean` | Filter recordings to only those that are currently recording                    |
| `--save`         |       | `string`  | Save recordings to the specified path                                           |
| `--overwrite`    | `-o`  | `boolean` | Overwrite existing files when saving                                            |
| `--json`         | `-j`  | `boolean` | Output show/recording/save results in JSON                                      |
| `--debug`        | `-d`  | `boolean` | Enable verbose logging for debugging                                            |
| `--help`         | `-h`  | `boolean` | Show help message                                                               |

## Examples

> [!NOTE]<br>
> The following examples assume you have a Fetch TV server on your local network and you've [linked the tool](#optional-link-fetchtv-tool) to your system path.

Search for Fetch TV servers:

```command
fetchtv
```

Display Fetch box details (uses auto-discovery):

```command
fetchtv info
```

List recorded show titles:

```command
fetchtv shows --ip=192.168.86.71
```

List recordings:

```command
fetchtv recordings --ip=192.168.86.71
```

List recordings and output as JSON:

```command
fetchtv recordings --ip=192.168.86.71 --json
```

Save new recordings to `./downloads` (creates directory if needed):

```command
fetchtv recordings --ip=192.168.86.71 --save=./downloads
```

Save new recordings but exclude show titles containing `News`:

```command
fetchtv recordings --ip=192.168.86.71 --exclude=News --save=./downloads
```

Save new episodes for the show `MasterChef`:

```command
fetchtv recordings --ip=192.168.86.71 --show=MasterChef --save=./downloads
```

Save & overwrite specific `MasterChef` episodes containing `S04E12` or `S04E13`:

```command
fetchtv recordings --ip=192.168.86.71 --show=MasterChef --title=S04E12 --title=S04E13 --save=./downloads --overwrite
```

List only items currently being recorded:

```command
fetchtv recordings --ip=192.168.86.71 --is-recording
```

Save only items currently being recorded:

```command
fetchtv recordings --ip=192.168.86.71 --is-recording --save=./in-progress
```

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
