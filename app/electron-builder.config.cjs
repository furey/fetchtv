const path = require('node:path')
const rootPackage = require(path.join(__dirname, '..', 'package.json'))

module.exports = {
  appId: 'io.github.furey.fetchtv',
  productName: 'Fetch My Recordings',
  copyright: 'Copyright © James Furey. GPL-3.0-or-later. An independent tool, not made, endorsed, or supported by Fetch TV.',
  directories: { output: 'release', buildResources: 'build' },
  files: ['dist/**/*', 'package.json'],
  asar: true,
  electronLanguages: ['en', 'en-GB', 'en_GB'],
  extraMetadata: { version: rootPackage.version, main: 'dist/main.mjs' },
  artifactName: 'Fetch-My-Recordings-${version}-${os}-${arch}.${ext}',
  publish: null,
  electronFuses: {
    runAsNode: false,
    enableCookieEncryption: true,
    enableNodeOptionsEnvironmentVariable: false,
    onlyLoadAppFromAsar: true,
    loadBrowserProcessSpecificV8Snapshot: false
  },
  mac: {
    target: [
      { target: 'dmg', arch: ['arm64', 'x64'] },
      { target: 'zip', arch: ['arm64', 'x64'] }
    ],
    icon: 'build/icon.png',
    identity: '-',
    hardenedRuntime: false,
    gatekeeperAssess: false,
    category: 'public.app-category.utilities',
    extendInfo: {
      NSLocalNetworkUsageDescription: 'Fetch My Recordings looks for your Fetch box on your home network and copies your recordings from it.'
    }
  },
  dmg: { sign: false, format: 'ULFO', title: 'Fetch My Recordings ${version}' },
  win: {
    target: [{ target: 'nsis', arch: ['x64'] }],
    icon: 'build/icon.ico',
    signAndEditExecutable: process.platform === 'win32'
  },
  nsis: {
    oneClick: true,
    perMachine: false,
    deleteAppDataOnUninstall: false,
    shortcutName: 'Fetch My Recordings'
  },
  linux: {
    target: [{ target: 'AppImage', arch: ['x64', 'arm64'] }],
    category: 'AudioVideo',
    executableName: 'fetch-my-recordings',
    icon: 'build/icons',
    synopsis: 'Copy the recordings on a Fetch box to this computer.'
  }
}
