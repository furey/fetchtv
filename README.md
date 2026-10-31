# fetchtv

[![NPM Version](https://img.shields.io/npm/v/fetchtv)](https://www.npmjs.com/package/fetchtv)
[![Docker Pulls](https://img.shields.io/docker/pulls/furey/fetchtv)](https://hub.docker.com/r/furey/fetchtv)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

fetchtv copies the recordings on a Fetch TV box to your computer over your home network, with no Fetch account or cloud login. It comes as a desktop app, Fetch My Recordings, and as a command-line tool that you run in a terminal.

<https://github.com/user-attachments/assets/092d5692-9651-4589-bddd-f0fc509dda03>

> [!NOTE]<br>
> Fetch's [Gen 3 Extended Service Levy](https://news.fetchtv.com.au/extended-service-levy-1) applies to Mini Gen 3 and Mighty Gen 3 owners from `1 November 2026`. Copy your recordings while the box still lists them: a box whose Fetch account is cancelled lists no recordings. To record free-to-air TV without Fetch, see [Freetvarr](https://github.com/furey/freetvarr).

## Contents

- [Desktop app](#desktop-app)
- [Command line](#command-line)
- [Usage](#usage)
- [Save paths](#save-paths)
- [Examples](#examples)
- [Interrupted saves](#interrupted-saves)
- [Moving to Freetvarr or Plex](#moving-to-freetvarr-or-plex)
- [Programmatic API](#programmatic-api)
- [Building the desktop app](#building-the-desktop-app)
- [Tests](#tests)
- [GitHub workflows](#github-workflows)
- [Disclaimer](#disclaimer)
- [Support](#support)

## Desktop app

- [Desktop app overview](#desktop-app-overview)
- [Download](#download)
- [First open](#first-open)
- [First open on a Mac](#first-open-on-a-mac)
- [First open on Windows](#first-open-on-windows)
- [First open on Linux](#first-open-on-linux)
- [Using the app](#using-the-app)

### Desktop app overview

<img src="https://github.com/furey/fetchtv/releases/download/docs-media/app-icon.png" alt="The Fetch My Recordings app icon: three coloured bars in a mint tray" width="96"/>

Fetch My Recordings is a desktop app for Mac, Windows, and Linux. It finds your Fetch box on your home network and copies the recordings you choose to a folder on your computer. You do not need a terminal or a Fetch account.

Fetch My Recordings is an independent, free tool made by a Fetch customer. It is not made, endorsed, or supported by Fetch TV. Fetch is a trademark of its owner.

### Download

Download the file for your computer. Each link gets the newest version.

| Computer                                | Download                                                                                                                                           |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Mac with an Apple chip (M1 or later)    | [`Fetch-My-Recordings-mac-arm64.dmg`](https://github.com/furey/fetchtv/releases/latest/download/Fetch-My-Recordings-mac-arm64.dmg)                 |
| Mac with an Intel chip                  | [`Fetch-My-Recordings-mac-x64.dmg`](https://github.com/furey/fetchtv/releases/latest/download/Fetch-My-Recordings-mac-x64.dmg)                     |
| Windows PC                              | [`Fetch-My-Recordings-win-x64.exe`](https://github.com/furey/fetchtv/releases/latest/download/Fetch-My-Recordings-win-x64.exe)                     |
| Linux computer (Intel or AMD)           | [`Fetch-My-Recordings-linux-x86_64.AppImage`](https://github.com/furey/fetchtv/releases/latest/download/Fetch-My-Recordings-linux-x86_64.AppImage) |
| Linux computer (ARM, e.g. Raspberry Pi) | [`Fetch-My-Recordings-linux-arm64.AppImage`](https://github.com/furey/fetchtv/releases/latest/download/Fetch-My-Recordings-linux-arm64.AppImage)   |

To check which chip a Mac has, open the Apple menu and choose **About This Mac**. A **Chip** line that says Apple M1, M2, M3, or later means an Apple chip. A **Processor** line that says Intel means an Intel chip.

All versions are on the [releases page](https://github.com/furey/fetchtv/releases).

### First open

Fetch My Recordings is free and open source. Anyone can read its code in the [`app`](./app) folder of this repository. Apple and Microsoft charge every year for the certificate that signs an app, so the app is unsigned. Because of this, your computer shows a warning the first time you open it. The steps below show each warning and the button to press.

The app talks only to the Fetch box on your home network. It reads and writes only the folder you choose and its own settings. It deletes nothing from the Fetch box.

> [!NOTE]<br>
> The pictures in the steps below are illustrations of the system messages, not screenshots. The wording on your computer can differ a little.

### First open on a Mac

These steps match macOS 15 Sequoia and later, on a Mac with an Apple chip or an Intel chip.

1. Open the `.dmg` file you downloaded.
2. Drag **Fetch My Recordings** into the **Applications** folder.
3. Open the **Applications** folder and double-click **Fetch My Recordings**.
4. A message says "Fetch My Recordings" Not Opened. Click **Done**. Do not click **Move to Trash**.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-mac-not-opened.png" alt="Illustration of the macOS message: Fetch My Recordings Not Opened, Apple could not verify it is free of malware, with the Done button marked" width="600"/>
     <br/><em>Illustration: the first macOS message</em>
   </p>

5. Open the Apple menu and choose **System Settings**.
6. Click **Privacy & Security** in the sidebar.
7. Scroll down to the **Security** section.
8. Next to "Fetch My Recordings" was blocked to protect your Mac, click **Open Anyway**.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-mac-privacy-security.png" alt="Illustration of Privacy and Security settings, with the Open Anyway button marked" width="600"/>
     <br/><em>Illustration: Open Anyway in Privacy &amp; Security</em>
   </p>

9. Click **Open Anyway** in the next message.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-mac-open-anyway.png" alt="Illustration of the second macOS message, with the Open Anyway button marked" width="600"/>
     <br/><em>Illustration: the second Open Anyway message</em>
   </p>

10. Type your Mac login password, or use Touch ID, when your Mac asks.
11. Wait for the app to open.

The **Open Anyway** button stays for about one hour after you first try to open the app. If it is gone, do steps 3 and 4 again, then continue from step 5.

When the app first searches for your Fetch box, your Mac asks whether Fetch My Recordings can find devices on your local network. Click **Allow**. The app needs this to find your Fetch box.

<p align="center">
  <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-mac-local-network.png" alt="Illustration of the macOS question about devices on your local network, with the Allow button marked" width="600"/>
  <br/><em>Illustration: the local network question</em>
</p>

If you clicked **Don't Allow**, open **System Settings** → **Privacy & Security** → **Local Network**, then turn on **Fetch My Recordings**.

You do these steps once. After that, the app opens with a double-click.

> [!TIP]<br>
> On macOS 14 Sonoma or earlier, there is a shorter way. Hold the Control key and click **Fetch My Recordings** in the **Applications** folder. Choose **Open**, then click **Open** in the message.

### First open on Windows

1. Double-click the `.exe` file you downloaded.
2. A blue message says Windows protected your PC. Click **More info**.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-windows-protected.png" alt="Illustration of the blue Windows protected your PC message, with the More info link marked" width="600"/>
     <br/><em>Illustration: Windows protected your PC</em>
   </p>

3. Check that the **Publisher** line says Unknown publisher.
4. Click **Run anyway**.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-windows-run-anyway.png" alt="Illustration of the same message after More info, with the Run anyway button marked" width="600"/>
     <br/><em>Illustration: the Run anyway button</em>
   </p>

5. Wait for the app to install and open. It installs for your Windows user only, and it is in the Start menu after that.

The first time the app searches for your Fetch box, Windows Security can ask whether to allow Fetch My Recordings on your networks:

1. Tick **Private networks, such as my home or work network**.
2. Leave **Public networks** unticked.
3. Click **Allow access**.

<p align="center">
  <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-windows-firewall.png" alt="Illustration of the Windows Security Alert for Fetch My Recordings, with Private networks ticked and the Allow access button marked" width="600"/>
  <br/><em>Illustration: the Windows Security Alert</em>
</p>

If you click **Cancel**, the app cannot search for the box. You can still type the IP address of your Fetch box (see [Using the app](#using-the-app)).

### First open on Linux

1. Open your file manager and find the `.AppImage` file you downloaded.
2. Right-click the file and choose **Properties**.
3. Click the **Permissions** tab.
4. Turn on **Allow executing file as program**. Newer versions of GNOME Files call it **Executable as Program**. KDE Dolphin calls it **Is executable**.

   <p align="center">
     <img src="https://github.com/furey/fetchtv/releases/download/docs-media/first-open-linux-allow-executing.png" alt="Illustration of the file Properties window on the Permissions tab, with Allow executing file as program marked" width="600"/>
     <br/><em>Illustration: Allow executing file as program</em>
   </p>

5. Close the **Properties** window.
6. Double-click the file.

To do the same in a terminal, run these commands in the folder with the file:

```console
chmod +x Fetch-My-Recordings-linux-x86_64.AppImage
./Fetch-My-Recordings-linux-x86_64.AppImage
```

If the app does not open on Ubuntu 22.04 or later, install FUSE, which every AppImage needs:

- On Ubuntu 22.04, run `sudo apt install libfuse2`.
- On Ubuntu 24.04 or later, run `sudo apt install libfuse2t64`.

If the app still does not open on Ubuntu 24.04 or later, start it from a terminal with `--no-sandbox` after the file name. This turns off Electron's sandbox (a limit on what the app's window can do), so use the option only for this app.

### Using the app

Click **Start** on the welcome screen. The app then takes you through these steps:

1. **Find your Fetch box**: the app searches your home network. If it finds no box, check that the box is on and that your computer is on the same home network, then search again. You can also type the box's IP address: four numbers with dots between them, such as `192.168.1.50`. Find it in your router's list of connected devices, or in the network details of the Fetch box's settings menu.
2. **Choose recordings**: every recording you have not saved before starts ticked. Click **Show** next to a show to tick or untick single episodes. A recording that is still recording cannot be saved until it ends.
3. **Choose a folder**: the app shows the free space in the folder and the space the recordings need. Turn on **Names for Plex, Jellyfin or Kodi** if you watch recordings in one of those apps. The files are then saved as `Show/Season 01/Show - S01E02.ts`.
4. **Save**: the app shows the progress of each recording. Your computer stays awake until the save finishes. Click **Stop** at any time.

<p align="center">
  <img src="https://github.com/furey/fetchtv/releases/download/docs-media/app-welcome.png" alt="The Fetch My Recordings welcome screen with the Start button" width="720"/>
  <br/><em>The welcome screen</em>
</p>

The app remembers what it saved from each box. On the **Choose recordings** step, a recording you saved before shows **Saved** and starts unticked. A recording shows **Moved or deleted** when its saved file is no longer where the app put it. That recording starts ticked, so **Save** copies it again.

<p align="center">
  <img src="https://github.com/furey/fetchtv/releases/download/docs-media/app-choose-recordings.png" alt="The Choose recordings step: one episode marked Saved, one marked Moved or deleted, and one new episode with its size" width="720"/>
  <br/><em>Choose recordings, with Saved and Moved or deleted episodes</em>
</p>

When the save ends, the app counts the recordings it saved, the recordings that were already saved, and the recordings it could not save. Click **Open folder** to see the files. The files play in VLC, a free video player, or in a media library app such as Plex, Jellyfin, or Kodi.

<p align="center">
  <img src="https://github.com/furey/fetchtv/releases/download/docs-media/app-all-done.png" alt="The All done screen: 8 saved, 0 already saved, 0 not saved, with Open folder and Save again buttons" width="720"/>
  <br/><em>The All done screen</em>
</p>

If a save stops part way, or some recordings are not saved, click **Save again**. The app skips the recordings that are already in the folder and continues a part-saved recording from where it stopped. The app keeps its list of saved files in a `fetchtv.json` file in the folder, the same file the [command line](#command-line) uses. So the app and the command line can take turns on one folder.

If the app says the box has no recordings, check the TV. A Fetch box whose account is cancelled shows its setup screen on the TV and lists no recordings.

## Command line

- [Command line overview](#command-line-overview)
- [NPX](#npx)
- [Node.js from source](#nodejs-from-source)
- [Docker from Docker Hub](#docker-from-docker-hub)
- [Docker from source](#docker-from-source)

### Command line overview

The `fetchtv` command does the same work as the [desktop app](#desktop-app) from a terminal, with more options: filters, path templates, JSON output, and Docker. Use it to script saves, or to run them on a server or NAS (network storage drive).

Install it one of these ways:

- [NPX](#npx) (easiest)
- [Node.js from source](#nodejs-from-source)
- [Docker from Docker Hub](#docker-from-docker-hub)
- [Docker from source](#docker-from-source)

Then see [Usage](#usage).

### NPX

> [!NOTE]<br>
> NPX requires [Node.js](https://nodejs.org/en/download) installed and running on your system (suggestion: use [Volta](https://volta.sh)).

NPX downloads and runs `fetchtv` in one command.

First, check that Node.js is running:

```console
node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
```

Then, run `fetchtv` with NPX:

```console
npx fetchtv             # Run the tool
npx fetchtv@latest      # (optional) "@latest" ensures package is up-to-date
npx -y fetchtv@latest   # (optional) "-y" flag skips any prompts

npx fetchtv info
npx fetchtv shows
npx fetchtv recordings

# etc…
```

> If you get permission errors with `npx`, run `npx clear-npx-cache` before `npx -y fetchtv`. This clears the cache and downloads the package again.

### Node.js from source

> [!NOTE]<br>
> Node.js from source requires [Node.js](https://nodejs.org/en/download) installed and running on your system (suggestion: use [Volta](https://volta.sh)).

1. Clone the `fetchtv` repository:<br>

    ```console
    git clone https://github.com/furey/fetchtv.git
    ```

1. Go to the cloned repository directory:<br>

    ```console
    cd /path/to/fetchtv
    ```

1. Check that Node.js is running:<br>

    ```console
    node --version # Ideally >= v22.x but fetchtv is >= v18.x compatible
    ```

1. Install the Node.js dependencies:<br>

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

1. (Optional) Link the `fetchtv` command to your system path, so that you can run `fetchtv` from any directory:<br>

    ```console
    npm link
    ```

    To remove the link, run `npm unlink`.

### Docker from Docker Hub

Pre-built multi-arch (`linux/amd64`, `linux/arm64`) images are at [`furey/fetchtv`](https://hub.docker.com/r/furey/fetchtv). You do not need to clone or build anything:

```console
docker run --rm --network host furey/fetchtv info
docker run --rm --network host furey/fetchtv recordings
```

See the [Docker Hub overview](https://hub.docker.com/r/furey/fetchtv) for a short quick start.

### Docker from source

> [!NOTE]<br>
> Docker from source requires [Docker](https://docs.docker.com/get-started/get-docker) installed and running on your system.

1. Clone the `fetchtv` repository:<br>

    ```console
    git clone https://github.com/furey/fetchtv.git
    ```

1. Go to the cloned repository directory:<br>

    ```console
    cd /path/to/fetchtv
    ```

1. Check that Docker is running:<br>

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

> [!TIP]<br>
> UPnP/SSDP discovery (the search for Fetch boxes on the network) often fails inside Docker containers. Give your Fetch box's IP address with the `--ip` option (and `--port` if needed):
>
> ```console
> docker run -t --rm fetchtv info --ip=192.168.86.71
> docker run -t --rm fetchtv shows --ip=192.168.86.71
> docker run -t --rm fetchtv recordings --ip=192.168.86.71
> ```

## Usage

If you [installed with NPX](#npx), run it from any directory:

```console
npx fetchtv <COMMAND> [OPTIONS]
```

If you [installed from Node.js source](#nodejs-from-source), run it from the cloned repository directory:

```console
cd /path/to/fetchtv
node fetchtv.js <COMMAND> [OPTIONS]
```

If you linked the command after you installed from source, run it from any directory:

```console
fetchtv <COMMAND> [OPTIONS]
```

| Command/Option   | Alias | Type      | Description                                                                                     |
| ---------------- | ----- | --------- | ----------------------------------------------------------------------------------------------- |
| `info`           |       | `command` | Returns Fetch TV server details                                                                 |
| `recordings`     |       | `command` | List episode recordings                                                                         |
| `shows`          |       | `command` | List show titles and not the episodes within                                                    |
| `--ip`           |       | `string`  | Specify the IP Address of the Fetch TV server                                                   |
| `--port`         |       | `number`  | Specify the port of the Fetch TV server (default: `49152`)                                      |
| `--show`         | `-s`  | `array`   | Filter recordings to show titles containing the specified text (repeatable)                     |
| `--exclude`      | `-e`  | `array`   | Filter recordings to show titles NOT containing the specified text (repeatable)                 |
| `--title`        | `-t`  | `array`   | Filter recordings to episode titles containing the specified text (repeatable)                  |
| `--is-recording` |       | `boolean` | Filter recordings to only those that are currently recording                                    |
| `--save`         |       | `string`  | Save recordings to the specified path                                                           |
| `--template`     |       | `string`  | Template for save path/filename structure (uses --save as base path)                            |
| `--for-plex`     |       | `boolean` | Saves in the Plex and Freetvarr layout `Show/Season 01/Show - S01E02.ts` (overrides --template) |
| `--overwrite`    | `-o`  | `boolean` | Save again over files that fetchtv saved earlier (never over other files)                       |
| `--concurrency`  |       | `number`  | Number of recordings to save at the same time, `1` to `10` (default: `2`)                       |
| `--json`         | `-j`  | `boolean` | Output show/recording/save results in JSON                                                      |
| `--debug`        | `-d`  | `boolean` | Enable verbose logging for debugging                                                            |
| `--help`         | `-h`  | `boolean` | Show help message                                                                               |

## Save paths

- [Template variables](#template-variables)
- [Plex layout](#plex-layout)
- [Example templates](#example-templates)

### Template variables

> [!IMPORTANT]<br>
> Put the `--template` string in single quotes (`'`), so that the shell does not expand the variables. For example:<br>
>
> ```console
>fetchtv recordings --save=./downloads --template='${show_title}/${recording_title}.${ext}'
> ```

A `--template` can use these variables:

| Variable                   | Description                      | Example                                        |
| -------------------------- | -------------------------------- | ---------------------------------------------- |
| `${show_title}`            | Title of the show                | `Australian Survivor`                          |
| `${recording_title}`       | Title of the recording/episode   | `S10 E2 - Episode 2 of Season 10 - Tue 18 Feb` |
| `${season_number}`         | Season number (if available)     | `10`                                           |
| `${season_number_padded}`  | Season number with leading zero  | `10`                                           |
| `${episode_number}`        | Episode number (if available)    | `2`                                            |
| `${episode_number_padded}` | Episode number with leading zero | `02`                                           |
| `${ext}`                   | File extension (ts, mp4, etc)    | `ts`                                           |

### Plex layout

The `--for-plex` option saves each episode in the layout that Plex expects and that Freetvarr uses for its own recordings. The desktop app's **Names for Plex, Jellyfin or Kodi** switch uses the same layout.

```text
<show>/Season 01/<show> - S01E02.ts
```

- **Season folder**: `Season` and the season number with at least two digits (`Season 01`, `Season 22`). If the show folder already has a folder for the same season number without the zero (e.g. `Season 1` from an earlier `fetchtv` save, Sonarr, or a manual copy), `fetchtv` saves into that folder instead of making a second one.
- **File name**: the show folder name, then `SxxEyy` with two-digit season and episode numbers.
- **Show folder**: chosen as Freetvarr chooses it. A folder with the same name (ignoring case) comes first. If there is none, and exactly one folder has the name plus a year or country (e.g. `Bluey (2018)` or `The Traitors (AU)`), `fetchtv` uses it. Otherwise it makes a folder named after the show. The default layout picks show folders the same way.

If a recording has no season or episode number (a film or news bulletin), `--for-plex` saves it under the default name (`<show>/<title>.<ext>`), prints a warning, and carries on with the rest.

`fetchtv` uses a custom `--template` as written: it does not match the template's folder names against existing folders.

The default name gets an `SxxEyy -` prefix when the season and episode are known but the title has no `SxxEyy` tag (for example when they come only from the recording's parent task name). Plex and Jellyfin use the prefix to match the episode, for example `Australian Survivor/S10E02 - Episode 2 - Tue 18 Feb.ts`.

### Example templates

Save recordings with show folder:

```text
${show_title}/${recording_title}.${ext}
```

Save recordings with show folder and `SXXEXX` episode naming:

```text
${show_title}/S${season_number_padded}E${episode_number_padded}.${ext}
```

Save recordings with show and season folders:

```text
${show_title}/Season ${season_number}/${recording_title}.${ext}
```

## Examples

> [!NOTE]<br>
> These examples assume a Fetch TV server on your local network, and that you [linked the command](#nodejs-from-source) to your system path.

Search for Fetch TV servers:

```console
fetchtv
```

Display Fetch box details (uses auto-discovery):

```console
fetchtv info
```

List recorded show titles:

```console
fetchtv shows --ip=192.168.86.71
```

List recordings:

```console
fetchtv recordings --ip=192.168.86.71
```

List recordings and output as JSON:

```console
fetchtv recordings --ip=192.168.86.71 --json
```

Save new recordings to `./downloads` (creates directory if needed):

```console
fetchtv recordings --ip=192.168.86.71 --save=./downloads
```

Save new recordings but exclude show titles containing `News`:

```console
fetchtv recordings --ip=192.168.86.71 --exclude=News --save=./downloads
```

Save new episodes for the show `MasterChef`:

```console
fetchtv recordings --ip=192.168.86.71 --show=MasterChef --save=./downloads
```

Save & overwrite specific `MasterChef` episodes containing `S04E12` or `S04E13`:

```console
fetchtv recordings --ip=192.168.86.71 --show=MasterChef --title=S04E12 --title=S04E13 --save=./downloads --overwrite
```

List only items currently being recorded:

```console
fetchtv recordings --ip=192.168.86.71 --is-recording
```

Save only items currently being recorded:

```console
fetchtv recordings --ip=192.168.86.71 --is-recording --save=./in-progress
```

Save recordings using a custom path template:

```console
fetchtv recordings --ip=192.168.86.71 --save=./downloads --template='${show_title}/${recording_title}.${ext}'
```

Save recordings in Plex-compatible path format:

```console
fetchtv recordings --ip=192.168.86.71 --save=./media --for-plex
```

## Interrupted saves

A save can stop part way: the network drops, the Fetch box resets the connection, or you press `Ctrl+C`. To finish it, run the same command again. `fetchtv` resumes partial files from where they stopped and skips recordings that are already complete.

While a recording downloads, `fetchtv` writes it to `<name>.part` beside a `<name>.lock` file. When the size matches what the Fetch box reported, `fetchtv` renames the `.part` file to its final name and removes the lock. Plex and Freetvarr do not read `.part` or `.lock` files, so a half-copied recording does not show up in your library.

The Fetch box often states a size a few bytes bigger than the file it sends, then closes the connection. `fetchtv` counts a copy that ends within `1 KB` of the stated size as complete. If a resume asks for bytes past the end of the file and the box answers that there is nothing left to send (HTTP `416`), and the partial file is within `1 KB` of the box's size, `fetchtv` also counts the copy as complete. A `416` on a shorter partial file discards it, and the next attempt starts again.

Each recording gets up to two retries in the same run (after `5 s`, then `15 s`) before `fetchtv` moves on. `fetchtv.json` in the save folder records each recording that `fetchtv` started, with its path inside the save folder, and adds the size when the save is complete. A later run uses it to find a file that is missing or short and save it again.

`fetchtv` only resumes, replaces, or skips files that `fetchtv.json` says it saved. If another file already has the name a recording would get (e.g. a recording that Freetvarr imported), `fetchtv` leaves that file alone, saves the recording as `<name> - copy 2.ts` (the next free number, as Freetvarr does), and prints which name it used. `--overwrite` follows the same rule.

> [!NOTE]<br>
> Versions before this change wrote partial files under their final name and did not record them in `fetchtv.json`. A partial file left by an older version is now treated as another tool's file: the new run saves a `- copy 2` file beside it. Delete the old partial file when the new save finishes.

The last line of a save tells you how many recordings are left:

```text
Saved 12 of 412, 386 already saved. 9 failed, 5 still recording. Run the same command again to finish the remaining 14.
```

If any recording failed, `fetchtv` exits with code `1`. With `--json`, the output is an object with a `summary` of these counts and the per-recording `results`.

If saves fail often, try `--concurrency=1` so that only one recording downloads at a time.

## Moving to Freetvarr or Plex

These steps copy a Fetch box's recordings into the media folders of [Freetvarr](https://github.com/furey/freetvarr) (or any Plex library), so that each show's Fetch episodes and new recordings share one folder.

1. Save the series with `--for-plex`, straight into the TV folder. Leave out films and one-off programmes:

    ```console
    fetchtv recordings --ip=192.168.86.71 --for-plex --save=/Volumes/data/media/tv --exclude=News
    ```

1. Save each film into the movies folder:

    ```console
    fetchtv recordings --ip=192.168.86.71 --show="Some Film" --save=/Volumes/data/media/movies
    ```

1. Save news, sport, and other one-off programmes into the one-offs folder:

    ```console
    fetchtv recordings --ip=192.168.86.71 --show=News --save=/Volumes/data/media/one-offs
    ```

1. Scan the libraries in Plex, or wait for the next automatic scan.

Points to check:

- **Episode numbers**: Fetch numbers episodes from its own guide data. The numbers can differ from the guide that Freetvarr and Plex use, for example Fetch's `S22 E34` of The Block was `S22E35` in the guide. When a show has both Fetch and Freetvarr recordings from the same weeks, compare the air dates and rename the Fetch files where the numbers differ. `fetchtv` cannot see the guide, so it cannot fix the numbers itself.
- **Default layout**: without `--for-plex`, episodes go straight into the show folder with Fetch's own titles (e.g. `The Block/S22 E34 - Kids Room and Re-Do Room Week - Tue 29 Sep.ts`). Plex reads these, but Freetvarr only looks for replacement files inside season folders, so use `--for-plex` for a Freetvarr library.
- **File owner**: Freetvarr can only rename, edit, or delete files that its user can write. Saving over a network share (SMB) from a Mac or PC gives the files the share user's ownership, which is usually correct. With Docker, run the container as the Freetvarr user (`PUID` and `PGID`), e.g. `docker run --rm --network host --user 1026:100 -v /path/to/media/tv:/downloads furey/fetchtv recordings --for-plex --save /downloads`. Without `--user`, the files belong to `root`.
- **Other files**: `fetchtv.json` stays in the top of the save folder. Keep it there so that later runs know which files `fetchtv` saved. Plex and Freetvarr ignore it.

## Programmatic API

In addition to the CLI, `fetchtv.js` can be imported as an ES module by other Node projects that want to drive a Fetch TV box programmatically (e.g. a long-running watcher that mirrors new recordings into a media library).

```js
import {
  discoverFetchServers,
  discoverFetch,
  getFetchRecordings,
  downloadFile
} from 'fetchtv'

// Enumerate every Fetch TV box on the LAN
const servers = await discoverFetchServers()

// Or grab a single box by IP (returns the full UPnP location object
// needed by the other helpers below)
const location = await discoverFetch({ ip: '192.168.1.50', port: 49152 })

// List recordings (optionally filtered)
const shows = await getFetchRecordings({
  location,
  filters: {
    folderFilter: [],       // include only shows whose title contains any of these (lowercased)
    excludeFilter: [],      // exclude shows whose title contains any of these (lowercased)
    titleFilter: [],        // include only items whose title contains any of these (lowercased)
    showsOnly: false,       // true → return just the show folders, no items
    isRecordingFilter: false // true → return only items still being recorded
  }
})

// Download a single item (fetchtv writes /tmp/example.ts.part, then renames it)
await downloadFile({
  item: shows[0].items[0],
  filePath: '/tmp/example.ts',
  progressBar: null,
  overwrite: false
})
```

| Export                  | Signature                                                                                                                                                           | Returns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `discoverFetchServers`  | `({ timeoutMs = 3000 } = {}) => Promise<Server[]>`                                                                                                                  | Every Fetch TV device found on the LAN via SSDP. Empty array if none.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `discoverFetch`         | `({ ip, port }) => Promise<Location \| null>`                                                                                                                       | First Fetch TV device matching the given `ip`/`port` (or first found via SSDP if `ip` is omitted), with the full `_rawDeviceXml` payload needed by `getFetchRecordings` and friends.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `getFetchRecordings`    | `({ location, filters = {}, onProgress }) => Promise<Show[]>`                                                                                                       | Show folders and their items. See example above for `filters` shape; every filter is optional. `onProgress({ done, total })`, if given, is called as each show is read, in place of the terminal progress bar.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `downloadFile`          | `({ item, filePath, progressBar, overwrite, signal }) => Promise<{ recorded, resumed?, cancelled?, size?, error?, warning? }>`                                      | Streams a recording to `<filePath>.part` and renames it to `filePath` once its size matches. Resumes an existing `.part` file, or a shorter file at `filePath`. The caller must own `filePath`: `downloadFile` does not check `fetchtv.json`. Aborting `signal` (an `AbortSignal`) stops the copy, keeps the `.part` file, removes the lock file, and returns `cancelled: true`.                                                                                                                                                                                                                                                                                                                                                                             |
| `isCurrentlyRecording`  | `(item) => Promise<boolean>`                                                                                                                                        | Whether an item is still being recorded (vs. a complete file).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `formatItem`            | `(item) => string`                                                                                                                                                  | Human-readable description (title, size, duration).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `createValidFilename`   | `(name) => string`                                                                                                                                                  | Filesystem-safe version of a string (strips/replaces problematic characters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `processPathTemplate`   | `({ templateString, data }) => string`                                                                                                                              | Substitutes the [template variables](#template-variables) in a path template. Throws if the template uses a variable with no value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `parseXml`              | `(xmlString) => object \| null`                                                                                                                                     | Parses an XML string (SOAP envelopes, MediaServer.xml, etc.) with the same fast-xml-parser options the CLI uses. Returns `null` on empty/invalid input.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `parseLocations`        | `(urls) => Promise<Location[]>`                                                                                                                                     | Fetches and parses an array of UPnP MediaServer.xml URLs into Location objects. Useful if you have device URLs from somewhere other than SSDP.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `getApiService`         | `(location) => Promise<{ cd_ctr, cd_service } \| null>`                                                                                                             | Extracts the ContentDirectory control URL + service URN from a Location. Required to use the lower-level browse helpers below.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `findDirectories`       | `({ apiService, objectId }) => Promise<Container[]>`                                                                                                                | Lists child containers (folders) under a UPnP ObjectID — e.g. enumerate "Recordings" by passing the root's object id.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `findItems`             | `({ apiService, objectId, showTitle }) => Promise<Item[]>`                                                                                                          | Lists items (recordings) under a UPnP container, with season/episode numbers parsed from titles and the file extension inferred from `protocolInfo`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `browseRequest`         | `({ apiService, objectId }) => Promise<object \| null>`                                                                                                             | Raw SOAP Browse, returns the parsed DIDL-Lite payload. Use this if you need attributes neither `findItems` nor `findDirectories` surface.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `saveRecordings`        | `({ recordings, savePath, template, forPlex = false, overwrite, concurrency = 2, retryDelays = [5000, 15000], onProgress, onResult, signal }) => Promise<Result[]>` | Higher-level batch save: takes the output of `getFetchRecordings`, writes each item to disk with resume, retry, and lock-file handling, and updates the saved-files DB. It never writes over a file that `fetchtv.json` does not list; it uses a `- copy N` name instead. Each result has a `status`: `saved`, `already_saved`, `still_recording`, `locked`, `failed`, or `cancelled`. `onProgress({ id, filePath, bytes, total })`, if given, replaces the terminal progress bars. `onResult(result)` is called with each result as soon as it is known. Aborting `signal` stops the running copies (their `.part` files stay for the next run) and marks the rest `cancelled`; with a `signal`, `saveRecordings` leaves the process signal handlers alone. |
| `loadSavedFiles`        | `(savePath) => Promise<Record<string, { title, size?, path? } \| string>>`                                                                                          | Reads `fetchtv.json` from `savePath`: id → `{ title, size, path }`, where `path` is relative to `savePath` and `size` is set once the save is complete. Older files map id → `{ title, size }` or id → title. Returns `{}` if missing or unreadable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `addSavedFile`          | `({ savePath, savedFilesDb, item, size, filePath }) => Promise<void>`                                                                                               | Marks an item as saved with its size (and its path, when `filePath` is given) by updating `savedFilesDb` in memory and writing it back to `fetchtv.json`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `syncCleanupLockFiles`  | `() => number`                                                                                                                                                      | Removes the lock files of the copies still running and returns how many it removed. An app that embeds fetchtv calls it before it quits.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `tsToSeconds`           | `(timestamp) => number`                                                                                                                                             | Parses a `HH:MM:SS` / `MM:SS` / `SS` string into seconds; returns `0` for non-string or malformed input.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `processFilter`         | `(arr) => string[]`                                                                                                                                                 | Normalizes an array of filter strings: splits on commas, trims, lowercases, drops empties. Used to build the `filters` argument for `getFetchRecordings`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `sortRecordingsByTitle` | `(recordings) => Recording[]`                                                                                                                                       | Sorts shows alphabetically, ignoring a leading "The " prefix. Returns a deep clone — does not mutate the input.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

`isCurrentlyRecording` / `downloadFile` recognise two distinct "still recording" sentinels in the UPnP directory listing: the `4398046510080`-byte marker, and any non-positive size (typically `-1`, used by Fetch TV when a recording has started but its final size isn't known yet). Both cause `downloadFile` to refuse the download — partial bytes from an in-progress recording would otherwise be written out as a truncated file.

Deletion is intentionally not exposed: Fetch TV firmware advertises the standard UPnP `DestroyObject` action in its ContentDirectory SCPD but its request handler rejects it (`Unknown Service Action`), and HTTP `DELETE` on the item URL returns `501`. The Fetch box's real control plane for deletion lives in Fetch's cloud APIs (auth + WebSocket to `messages.fetchtv.com.au`) and is out of scope for this LAN-only library.

The module is safe to `import` — running the CLI requires invoking `fetchtv.js` directly as a script.

## Building the desktop app

The app's source is in the [`app`](./app) folder. It is an [Electron](https://www.electronjs.org) app that calls `fetchtv.js` to find boxes, list recordings, and save them.

```console
npm ci
cd app
npm ci
npm start
```

To try the app without a Fetch box, run `npm run fake-box` in the `app` folder in a second terminal. It starts a pretend Fetch box with made-up shows on port `49152`. The pretend box acts like a real one: it overstates each file's size, resets the connection at the end, and answers a resume with `416`. It also answers the app's search, or you can type `127.0.0.1` as the IP address. Add `--empty` for a box with no recordings, or `--mb-per-second=2` for a slow box.

| Command              | Effect                                                                                    |
| -------------------- | ----------------------------------------------------------------------------------------- |
| `npm test`           | Runs the unit tests and the end-to-end tests, which drive the app against the pretend box |
| `npm run dist`       | Builds the app for the computer you run it on, into `app/release`                         |
| `npm run dist:mac`   | Builds the macOS `.dmg` and `.zip` files (Apple chip and Intel)                           |
| `npm run dist:win`   | Builds the Windows installer                                                              |
| `npm run dist:linux` | Builds the Linux AppImages (`x86_64` and `arm64`)                                         |

[`app/scripts/record-demo.mjs`](./app/scripts/record-demo.mjs) records the demo video at the top of this README. It starts a pretend box with demo shows, drives the app, and joins the frames, cursor, and sounds into an MP4 file with `ffmpeg`. Run `npm run bundle` first, then, in the `app` folder:

```console
node scripts/record-demo.mjs --out /tmp/demo --mp4 /tmp/demo/demo.mp4 --poster /tmp/demo/poster.png
```

Add `--executable <path>` to record a built app instead of the source.

## Tests

A feature-level test suite covers every command and code path that doesn't require talking to a real Fetch TV box:

```console
npm install
npm test
```

Tests use Node's built-in `node:test` runner (no external framework) and [`nock`](https://github.com/nock/nock) to intercept HTTP. The CLI-level tests in `test/commands.test.js` stand up a local `http.createServer` and spawn `node fetchtv.js --ip 127.0.0.1 --port <random>` against it.

| File                          | What it covers                                                                                                                                                                                                                                                                                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `helpers.test.js`             | Pure helpers: filename sanitization, timestamp parsing, filter normalization, "The"-prefix-aware sort, XML node navigation, item projection                                                                                                                                                                                                       |
| `xml.test.js`                 | `parseXml` against a Browse-shaped fixture with >1000 entity references (regression catch for `fast-xml-parser` entity-expansion cap changes)                                                                                                                                                                                                     |
| `didl.test.js`                | DIDL-Lite item/container parsing: S/E number extraction, extension inference from `protocolInfo`, size/duration coercion                                                                                                                                                                                                                          |
| `discovery.test.js`           | `discoverFetch` via explicit `--ip`, including the non-Fetch and unreachable cases                                                                                                                                                                                                                                                                |
| `filters.test.js`             | `--show` / `--exclude` / `--title` filter behaviour end-to-end through `getFetchRecordings`                                                                                                                                                                                                                                                       |
| `recording-detection.test.js` | `isCurrentlyRecording` size sentinels and HEAD/GET fallback paths                                                                                                                                                                                                                                                                                 |
| `templates.test.js`           | `processPathTemplate`: standard placeholders, Plex template, missing-placeholder throw, traversal sanitization                                                                                                                                                                                                                                    |
| `save.test.js`                | `loadSavedFiles` / `addSavedFile`, `isLockFileStale`, end-to-end save flow with a mocked download                                                                                                                                                                                                                                                 |
| `save-reliability.test.js`    | Local HTTP server serving full, short, and reset bodies, plus the Fetch box's overstated sizes and `416` replies: size checks, retries with `Range` resume from `.part` files, skip of complete files, `- copy N` names beside files fetchtv did not save, `--for-plex` season folders, show folder matching, final summary line, `--concurrency` |
| `save-events.test.js`         | `onProgress` / `onResult` events from `saveRecordings`, and stopping a save with an `AbortSignal`: the partial file stays, the lock goes, and the next run resumes                                                                                                                                                                                |
| `commands.test.js`            | Spawned CLI: `info` / `recordings` / `shows`, prefix-matched commands, `--show` / `--exclude` / `--title`, `--is-recording`, `--json`, `--for-plex`                                                                                                                                                                                               |

The [Desktop App](#desktop-app-builds) workflow runs these tests on every push to `main` or `develop`.

## GitHub workflows

- [Workflows overview](#workflows-overview)
- [Publish to NPM](#publish-to-npm)
- [Publish to Docker Hub](#publish-to-docker-hub)
- [Desktop app builds](#desktop-app-builds)
- [Sync Docker Hub description](#sync-docker-hub-description)

### Workflows overview

GitHub Actions workflows run the tests, publish the packages, and build the desktop app. Two run on release, one keeps the Docker Hub overview in sync, and one tests and builds the desktop app.

| Workflow                    | File                                                                         | Trigger                                                                                | Effect                                                                                                              |
| --------------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Publish to NPM              | [`publish-npm.yml`](./.github/workflows/publish-npm.yml)                     | `release: created`                                                                     | Publishes [`fetchtv` on NPM](https://www.npmjs.com/package/fetchtv)                                                 |
| Publish to Docker Hub       | [`publish-docker.yml`](./.github/workflows/publish-docker.yml)               | `release: created`                                                                     | Publishes [`furey/fetchtv` on Docker Hub](https://hub.docker.com/r/furey/fetchtv)                                   |
| Sync Docker Hub Description | [`dockerhub-description.yml`](./.github/workflows/dockerhub-description.yml) | push to `main` touching `DOCKER_README.md` (or manual)                                 | Pushes `DOCKER_README.md` to the [`furey/fetchtv` Hub overview](https://hub.docker.com/r/furey/fetchtv)             |
| Desktop App                 | [`desktop-app.yml`](./.github/workflows/desktop-app.yml)                     | push to `main` or `develop`, a version tag, a pull request touching the app, or manual | Tests and builds the desktop app on macOS, Windows, and Linux; attaches the builds to the release for a version tag |

### Publish to NPM

Checks out the repo, sets up Node.js 22, runs `npm ci`, then `npm publish` against the public NPM registry using the `NPM_TOKEN` secret.

### Publish to Docker Hub

Builds multi-arch images (`linux/amd64`, `linux/arm64`) via Buildx + QEMU, authenticates with `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`, and pushes tags derived from the release's semver (`{{version}}`, `{{major}}.{{minor}}`, `latest`) to `furey/fetchtv`.

### Desktop app builds

On macOS, Windows, and Ubuntu runners: runs `npm test` (Ubuntu only), runs the desktop app's end-to-end tests against a fake Fetch box (under `xvfb` on Ubuntu), and builds the app with `electron-builder`. The builds are kept as workflow artefacts. For a pushed version tag (e.g. `2.2.0`), a second job waits up to 30 minutes for the GitHub release of that tag, then attaches the `.dmg`, `.zip`, `.exe`, and `.AppImage` files to it. Only that job can write to the repository.

### Sync Docker Hub description

Pushes [`DOCKER_README.md`](./DOCKER_README.md) to the `furey/fetchtv` Docker Hub overview using `peter-evans/dockerhub-description@v4`. Uses the same `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` credentials as the image-publish workflow. Triggers on any push to `main` that touches the README or the workflow file, and can be run manually via `workflow_dispatch`.

## Disclaimer

This project:

- Is licensed under the [GNU GPLv3 License](./LICENSE.txt).
- Is not made, endorsed, or supported by Fetch TV.
- Is a derivative work based on [`lingfish/fetchtv-cli`](https://github.com/lingfish/fetchtv-cli) (Python), which is based on [`jinxo13/FetchTV-Helpers`](https://github.com/jinxo13/FetchTV-Helpers) (also Python).
- Is written with the assistance of AI and may contain errors.
- Is intended for educational and experimental purposes only.
- Is provided as-is with no warranty—please use at your own risk.

## Support

If you've found this project helpful consider supporting my work through:

[Buy Me a Coffee](https://www.buymeacoffee.com/furey) | [GitHub Sponsorship](https://github.com/sponsors/furey)

Contributions help me continue developing and improving this tool, allowing me to dedicate more time to add new features and ensuring it remains a valuable resource for the community.
