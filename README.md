# fetchtv

[![NPM Version](https://img.shields.io/npm/v/fetchtv)](https://www.npmjs.com/package/fetchtv)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

A Node.js CLI tool to download Fetch TV recordings via UPnP.

Based on [`lingfish/fetchtv-cli`](https://github.com/lingfish/fetchtv-cli) (Python) which is based on [`jinxo13/FetchTV-Helpers`](https://github.com/jinxo13/FetchTV-Helpers) (also Python).

## Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)

## Quick Start

- [Install](#installation) `fetchtv`
- [Run](#usage) `fetchtv`

## Features

- Discover Fetch TV boxes on your network
- View basic information about your Fetch TV box
- List recorded shows and episodes
- Filter recordings by show and episode title
- Download recordings to a specified directory
- Output information in human-readable format or JSON

## Installation

- [NPX](#installation-npx) (Easiest)
- [Node.js from Source](#installation-nodejs-from-source)

### Installation: NPX

> [!NOTE]<br>
> NPX requires [Node.js](https://nodejs.org/en/download) installed and running on your system (suggestion: use [Volta](https://volta.sh)).

The easiest way to run `fetchtv` is using NPX.

First, ensure Node.js is installed:

```console
node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
```

Then, run `fetchtv` via NPX:

```console
# Supplying the "-y" flag to skip prompts
npx -y fetchtv

# Using "@latest" to keep the package up-to-date
npx -y fetchtv@latest
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
1. Ensure Node.js is installed:<br>
    ```console
    node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
    ```
1. Install Node.js dependencies:<br>
    ```console
    npm ci
    ```
1. Run the tool:<br>
    ```console
    node fetchtv.js
    ```

#### Optional: Link `fetchtv` Tool

You may optionally link the `fetchtv` tool to your system path for easier access:

```console
npm link
```

This will create a symlink to the `fetchtv` command in your global `node_modules` directory, allowing you to run it from anywhere in your terminal.


To uninstall the linked tool, run:

```console
npm unlink
```

## Usage

If you [installed via NPX](#installation-npx), you can run it from anywhere:

```command
npx -y fetchtv <OPTIONS>
```

If you [installed from Node.js source](#installation-nodejs-from-source), you can run it from the cloned repo directory:

```command
cd /path/to/fetchtv
node fetchtv.js <OPTIONS>
```

If you [linked the tool](#optional-link-fetchtv-tool) after installing from source, you can run it from anywhere:

```command
fetchtv <OPTIONS>
```

## Examples

> [!NOTE]<br>
> The following examples assume you have a Fetch TV box on your local network and you've [linked the tool](#optional-link-fetchtv-tool) to your system path.

Search for Fetch TV boxes:

```command
fetchtv
```

Display Fetch box details (uses auto-discovery):

```command
fetchtv --info
```

List recorded show titles:

```command
fetchtv --ip 192.168.86.71 --shows
```

List recordings:

```command
fetchtv --ip 192.168.86.71 --recordings
```

List recordings and output as JSON:

```command
fetchtv --ip 192.168.86.71 --recordings --json
```

Save all new recordings to `./downloads` (creates directory if needed):

```command
fetchtv --ip 192.168.86.71 --recordings --save=./downloads
```

Save all new recordings but exclude shows with `News` in the title:

```command
fetchtv --ip 192.168.86.71 --recordings --exclude=News --save=./downloads
```

Save only new episodes for the show `MasterChef`:

```command
fetchtv --ip 192.168.86.71 --recordings --folder=MasterChef --save=./downloads
```

Force download & overwrite specific `MasterChef` episodes containing `S04E12` or `S04E13`:

```command
fetchtv --ip 192.168.86.71 --recordings --folder=MasterChef --title=S04E12 --title=S04E13 --save=./downloads --overwrite
```

List only items currently being recorded:

```command
fetchtv --ip 192.168.86.71 --isrecording
```

Save only items currently being recorded:

```command
fetchtv --ip 192.168.86.71 --isrecording --save=./in-progress
```

## Command Line Options

| Option          | Alias | Type      | Description                                                                              |
| --------------- | ----- | --------- | ---------------------------------------------------------------------------------------- |
| `--info`        | `-i`  | `boolean` | Attempts auto-discovery and returns the Fetch TV Server details                          |
| `--ip`          |       | `string`  | Specify the IP Address of the Fetch TV Server                                            |
| `--port`        |       | `number`  | Specify the port of the Fetch TV Server (defaults to `49152`)                            |
| `--recordings`  | `-r`  | `boolean` | List recordings                                                                          |
| `--shows`       | `-s`  | `boolean` | List show titles and not the episodes within                                             |
| `--folder`      | `-f`  | `array`   | Only include recordings where the show title contains the specified text (repeatable)    |
| `--title`       | `-t`  | `array`   | Only include recordings where the episode title contains the specified text (repeatable) |
| `--exclude`     | `-e`  | `array`   | Don't include show titles containing the specified text (repeatable)                     |
| `--isrecording` |       | `boolean` | List only items that are currently recording                                             |
| `--save`        |       | `string`  | Save recordings to the specified path (creates directory if it doesn't exist)            |
| `--overwrite`   | `-o`  | `boolean` | Will save and overwrite any existing files                                               |
| `--json`        | `-j`  | `boolean` | Output show/recording/save results in JSON                                               |
| `--debug`       | `-d`  | `boolean` | Enable detailed debug logging of API responses                                           |
| `--help`        | `-h`  | `boolean` | Show help message                                                                        |

## Notes

- The tool relies on your Fetch TV box's UPnP/DLNA service being discoverable on your network.
- Firewalls on your computer might block SSDP discovery packets.
- This tool may list recordings marked for deletion on the Fetch box, as the DLNA service doesn't seem to expose this status.
- Downloads might sometimes report as incomplete due to how Fetch TV handles streaming (files may still be usable—check file sizes if concerned).
- The Fetch TV UPnP service can sometimes be unstable and drop connections. This tool implements retries and delays to mitigate this, but occasional warnings about failed first attempts may appear in the console.

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
