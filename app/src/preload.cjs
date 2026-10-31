const { contextBridge, ipcRenderer } = require('electron')

const invoke = (channel) => (args) => ipcRenderer.invoke(channel, args)

const subscribe = (channel) => (listener) => {
  const handler = (_event, payload) => listener(payload)
  ipcRenderer.on(channel, handler)
  return () => ipcRenderer.removeListener(channel, handler)
}

contextBridge.exposeInMainWorld('fetchtv', {
  appInfo: invoke('app:info'),
  findBoxes: invoke('boxes:find'),
  connectToBox: invoke('boxes:connect'),
  listRecordings: invoke('recordings:list'),
  onListProgress: subscribe('recordings:progress'),
  chooseFolder: invoke('folder:choose'),
  describeFolder: invoke('folder:describe'),
  startSave: invoke('save:start'),
  cancelSave: invoke('save:cancel'),
  onSaveProgress: subscribe('save:progress'),
  onSaveResult: subscribe('save:result'),
  openSavedFolder: invoke('folder:open'),
  openProjectPage: invoke('link:project'),
  setSound: invoke('settings:sound')
})
