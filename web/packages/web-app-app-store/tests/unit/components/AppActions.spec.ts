import { defaultPlugins, mount } from '@ownclouders/web-test-helpers'
import AppActions from '../../../src/components/AppActions.vue'
import { App, AppVersion } from '../../../src/types'
import { mock } from 'vitest-mock-extended'
import { nextTick } from 'vue'
import { useInstallationsStore } from '../../../src/piniaStores/installations'

const version1: AppVersion = {
  version: '1.0.0',
  url: 'https://example.com/app-1.0.0.zip'
}
const version2: AppVersion = {
  version: '1.1.0',
  url: 'https://example.com/app-1.1.0.zip'
}
const versions = [version1, version2]
const mostRecentVersion = version2

const selectors = {
  downloadButton: 'button'
}

describe('AppActions', () => {
  it('offers installation only after the server authorizes it', async () => {
    const { wrapper } = getWrapper({})
    expect(wrapper.find('.app-install-button').exists()).toBe(false)
    const store = useInstallationsStore()
    store.available = true
    await nextTick()
    expect(wrapper.find('.app-install-button').text()).toBe('Install')
  })
  it('installs the selected release when Install is clicked', async () => {
    const { wrapper } = getWrapper({ version: version1 })
    const store = useInstallationsStore()
    store.available = true
    await nextTick()
    await wrapper.find('.app-install-button').trigger('click')
    expect(store.install).toHaveBeenCalledWith(
      undefined,
      expect.objectContaining({ id: 'org.example.demo' }),
      version1.version
    )
  })
  it('disables installation for an installed app', async () => {
    const { wrapper } = getWrapper({})
    const store = useInstallationsStore()
    vi.mocked(store.isInstalled).mockReturnValue(true)
    store.available = true
    await nextTick()
    expect(wrapper.find('.app-install-button').text()).toBe('Installed')
    expect(wrapper.find('.app-install-button').attributes('disabled')).toBeDefined()
  })
  it('renders a "Download" button', () => {
    const { wrapper } = getWrapper({})
    expect(wrapper.find(selectors.downloadButton).text()).toBe('Download')
  })
  describe('calling the "download" handler', () => {
    it('uses the most recent version when none is specified', async () => {
      const { wrapper } = getWrapper({})
      await wrapper.find(selectors.downloadButton).trigger('click')
      expect(window.location.href).toBe(mostRecentVersion.url)
    })
    it('uses the version provided via props', async () => {
      const { wrapper } = getWrapper({ version: version1 })
      await wrapper.find(selectors.downloadButton).trigger('click')
      expect(window.location.href).toBe(version1.url)
    })
  })
})

const getWrapper = ({ version }: { version?: AppVersion }) => {
  const app = {
    ...mock<App>({}),
    id: 'org.example.demo',
    repository: { name: 'official', url: 'https://marketplace.owncloud.com/api/ocis/v1/apps.json' },
    versions,
    mostRecentVersion
  }

  return {
    wrapper: mount(AppActions, {
      props: {
        app,
        version
      },
      global: {
        plugins: [...defaultPlugins()]
      }
    })
  }
}
