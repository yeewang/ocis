import { defineStore } from 'pinia'
import { ref } from 'vue'
import { useClientService } from '@ownclouders/web-pkg'
import { App } from '../types'

type InstalledApp = { id: string; directory: string; version?: string }

export const useInstallationsStore = defineStore('app-store-installations', () => {
  const installed = ref<InstalledApp[]>([])
  const available = ref(false)
  const busy = ref('')
  const justInstalled = ref('')
  let loaded: Promise<void>

  const load = (service: ReturnType<typeof useClientService>) => {
    if (!loaded) {
      loaded = service.httpAuthenticated
        .get<{ installed: InstalledApp[] }>('/api/app-store/installed')
        .then(({ data }) => {
          if (Array.isArray(data.installed)) {
            installed.value = data.installed
            available.value = true
          }
        })
        .catch(() => {
          available.value = false
        })
    }
    return loaded
  }

  const isInstalled = (app: App) =>
    installed.value.some(
      (entry) => entry.id === app.id || (!entry.id && app.id.endsWith(`.${entry.directory}`))
    )

  const install = async (
    service: ReturnType<typeof useClientService>,
    app: App,
    version: string
  ) => {
    if (busy.value || isInstalled(app)) return
    busy.value = app.id
    try {
      const { data } = await service.httpAuthenticated.post('/api/app-store/install', {
        id: app.id,
        version
      })
      installed.value.push(data)
      justInstalled.value = app.id
    } finally {
      busy.value = ''
    }
  }

  return { installed, available, busy, justInstalled, load, isInstalled, install }
})
