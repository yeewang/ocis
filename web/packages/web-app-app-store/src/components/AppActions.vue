<template>
  <oc-list class="app-actions">
    <action-menu-item
      v-for="action in actions"
      :key="`app-action-${action.name}`"
      size="small"
      :action="action"
      :action-options="{ app, version }"
    />
    <oc-button
      v-if="installer.available && isOfficial"
      size="small"
      appearance="filled"
      :disabled="!!installer.busy || installer.isInstalled(app)"
      class="app-install-button"
      @click="install"
    >
      {{
        installer.busy === app.id
          ? $gettext('Installing…')
          : installer.isInstalled(app)
            ? $gettext('Installed')
            : $gettext('Install')
      }}
    </oc-button>
    <oc-button v-if="installer.justInstalled === app.id" size="small" @click="reload">
      {{ $gettext('Reload to use app') }}
    </oc-button>
  </oc-list>
</template>
<script lang="ts" setup>
import { ActionMenuItem, useMessages, useClientService } from '@ownclouders/web-pkg'
import { useAppActionsDownload } from '../composables'
import { computed, onMounted } from 'vue'
import { useGettext } from 'vue3-gettext'
import { useInstallationsStore } from '../piniaStores/installations'
import { App, AppVersion } from '../types'

interface Props {
  app?: App
  version?: AppVersion | null
}
const { app = undefined, version = null } = defineProps<Props>()
const installer = useInstallationsStore()
const clientService = useClientService()
const { $gettext } = useGettext()
const { showMessage, showErrorMessage } = useMessages()
const isOfficial = computed(
  () => app?.repository?.url === 'https://marketplace.owncloud.com/api/ocis/v1/apps.json'
)
onMounted(() => installer.load(clientService))
const reload = () => window.location.reload()
const install = async () => {
  try {
    await installer.install(clientService, app, (version || app.mostRecentVersion).version)
    showMessage({ title: $gettext('App installed. Reload this page to start using it.') })
  } catch (error) {
    const failure = error as { response?: { data?: { message?: string } } }
    showErrorMessage({
      title: $gettext('Could not install app'),
      desc:
        typeof failure?.response?.data?.message === 'string'
          ? failure.response.data.message
          : $gettext('Please try again.'),
      errors: [error]
    })
  }
}
const { downloadAppAction } = useAppActionsDownload()
const actions = computed(() => {
  return [downloadAppAction]
})
</script>

<style lang="scss">
.app-actions {
  display: flex;
  justify-content: flex-start;
  gap: 1rem;
}
</style>
