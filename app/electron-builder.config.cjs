const path = require('node:path')
const rootPackage = require(path.join(__dirname, '..', 'package.json'))

module.exports = {
  appId: 'io.github.furey.fetchtv',
  productName: 'fetchtv',
  copyright: 'Copyright © James Furey. GPL-3.0-or-later. An independent tool, not made, endorsed, or supported by Fetch TV.',
  directories: { output: 'release', buildResources: 'build' },
  files: ['dist/**/*', 'package.json'],
  asar: true,
  electronLanguages: ['en', 'en-GB', 'en_GB'],
  extraMetadata: { version: rootPackage.version, main: 'dist/main.mjs' },
  artifactName: '${productName}-${version}-${os}-${arch}.${ext}',
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
    identity: '-',
    hardenedRuntime: false,
    gatekeeperAssess: false,
    category: 'public.app-category.utilities',
    extendInfo: {
      NSLocalNetworkUsageDescription: 'fetchtv looks for your Fetch box on your home network and copies your recordings from it.'
    }
  },
  dmg: { sign: false, format: 'ULFO', title: 'fetchtv ${version}' },
  win: {
    target: [{ target: 'nsis', arch: ['x64'] }],
    signAndEditExecutable: process.platform === 'win32'
  },
  nsis: {
    oneClick: true,
    perMachine: false,
    deleteAppDataOnUninstall: false,
    shortcutName: 'fetchtv'
  },
  linux: {
    target: [{ target: 'AppImage', arch: ['x64', 'arm64'] }],
    category: 'AudioVideo',
    synopsis: 'Copy the recordings on a Fetch box to this computer.'
  }
}
