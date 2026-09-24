import { createPinia, setActivePinia } from 'pinia'
import { useInstallationsStore } from '../../src/piniaStores/installations'
import { App } from '../../src/types'
import { useClientService } from '@ownclouders/web-pkg'

const app = { id: 'org.example.demo' } as App
const makeClient = () => ({
  httpAuthenticated: {
    get: vi.fn().mockResolvedValue({ data: { installed: [] } }),
    post: vi.fn().mockResolvedValue({ data: { id: app.id, directory: 'demo', version: '1.0.0' } })
  }
})

describe('App installations', () => {
  beforeEach(() => setActivePinia(createPinia()))

  it('loads once and recognizes manually installed apps', async () => {
    const client = makeClient()
    client.httpAuthenticated.get.mockResolvedValue({ data: { installed: [{ directory: 'demo' }] } })
    const store = useInstallationsStore()
    const service = client as unknown as ReturnType<typeof useClientService>
    await Promise.all([store.load(service), store.load(service)])
    expect(client.httpAuthenticated.get).toHaveBeenCalledTimes(1)
    expect(store.available).toBe(true)
    expect(store.isInstalled(app)).toBe(true)
  })

  it('installs the selected version and records its installed state', async () => {
    const client = makeClient()
    const store = useInstallationsStore()
    await store.install(client as unknown as ReturnType<typeof useClientService>, app, '1.0.0')
    expect(client.httpAuthenticated.post).toHaveBeenCalledWith('/api/app-store/install', {
      id: app.id,
      version: '1.0.0'
    })
    expect(store.isInstalled(app)).toBe(true)
    expect(store.justInstalled).toBe(app.id)
    expect(store.busy).toBe('')
  })

  it('clears busy state after failure and permits retry', async () => {
    const client = makeClient()
    client.httpAuthenticated.post.mockRejectedValueOnce(new Error('download failed'))
    const store = useInstallationsStore()
    const service = client as unknown as ReturnType<typeof useClientService>
    await expect(store.install(service, app, '1.0.0')).rejects.toThrow('download failed')
    expect(store.busy).toBe('')
    expect(store.isInstalled(app)).toBe(false)
    await store.install(service, app, '1.0.0')
    expect(store.isInstalled(app)).toBe(true)
  })

  it('hides installation when the server denies access', async () => {
    const client = makeClient()
    client.httpAuthenticated.get.mockRejectedValue(new Error('403'))
    const store = useInstallationsStore()
    await store.load(client as unknown as ReturnType<typeof useClientService>)
    expect(store.available).toBe(false)
  })
})
