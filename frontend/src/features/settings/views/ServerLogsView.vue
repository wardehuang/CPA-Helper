<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  NButton,
  NDataTable,
  NIcon,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import { Download } from 'lucide-vue-next'

import { getServerLogDownloadUrl, getServerLogs, type ServerLogFile } from '@/features/settings/api/serverLogsApi'
import { useI18n } from '@/shared/i18n'
import { formatDateTime } from '@/shared/utils/format'

const AUTO_REFRESH_INTERVAL_MS = 5000

const message = useMessage()
const { currentLanguage, errorText, t } = useI18n()

const isLoading = ref(false)
const isAutoRefreshing = ref(false)
const autoRefreshError = ref<string | null>(null)
const lastRefreshedAt = ref<Date | null>(null)
const logs = ref<ServerLogFile[]>([])

const refreshStatusText = computed(() => {
  const lastRefreshTime = lastRefreshedAt.value
  if (!lastRefreshTime) {
    return autoRefreshError.value
      ? t('自动刷新异常 · 尚无成功同步', 'Auto refresh error · no successful sync yet')
      : t('每 5 秒自动刷新 · 等待首次同步', 'Auto refresh every 5 seconds · waiting for first sync')
  }
  const lastRefreshText = new Intl.DateTimeFormat(currentLanguage.value === 'zh' ? 'zh-CN' : 'en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(lastRefreshTime)
  if (autoRefreshError.value) {
    return t(`自动刷新异常 · 最近成功 ${lastRefreshText}`, `Auto refresh error · last success ${lastRefreshText}`)
  }
  return t(`每 5 秒自动刷新 · 最近 ${lastRefreshText}`, `Auto refresh every 5 seconds · latest ${lastRefreshText}`)
})

async function refresh(silent = false) {
  if (isLoading.value || isAutoRefreshing.value) {
    return
  }
  if (silent) {
    isAutoRefreshing.value = true
  } else {
    isLoading.value = true
  }
  try {
    logs.value = await getServerLogs()
    autoRefreshError.value = null
    lastRefreshedAt.value = new Date()
  } catch (error) {
    const errorMessage = errorText(error, '加载日志列表失败', 'Failed to load logs')
    if (silent) {
      autoRefreshError.value = errorMessage
    } else {
      message.error(errorMessage)
    }
  } finally {
    if (silent) {
      isAutoRefreshing.value = false
    } else {
      isLoading.value = false
    }
  }
}

let autoRefreshTimer: number | undefined
function startAutoRefresh() {
  if (autoRefreshTimer !== undefined) {
    return
  }
  autoRefreshTimer = window.setInterval(() => {
    void refresh(true)
  }, AUTO_REFRESH_INTERVAL_MS)
}

function stopAutoRefresh() {
  if (autoRefreshTimer !== undefined) {
    window.clearInterval(autoRefreshTimer)
    autoRefreshTimer = undefined
  }
}

onMounted(() => {
  void refresh()
  startAutoRefresh()
})

onBeforeUnmount(() => {
  stopAutoRefresh()
})

function formatSize(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(2)} KB`
  }
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`
}

function handleDownload(row: ServerLogFile) {
  const url = getServerLogDownloadUrl(row.name)
  const a = document.createElement('a')
  a.href = url
  a.download = row.name
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
}

const columns = computed<DataTableColumns<ServerLogFile>>(() => [
  {
    title: t('时间', 'Time'),
    key: 'time',
    width: 200,
    render: (row) => formatDateTime(row.time),
  },
  {
    title: t('文件名', 'File Name'),
    key: 'name',
    render: (row) => row.name,
  },
  {
    title: t('下载', 'Download'),
    key: 'actions',
    width: 120,
    render: (row) => {
      return h(
        NButton,
        {
          size: 'small',
          type: 'primary',
          secondary: true,
          onClick: () => handleDownload(row),
        },
        {
          default: () => t('下载', 'Download'),
          icon: () => h(NIcon, null, { default: () => h(Download) }),
        },
      )
    },
  },
])
</script>

<template>
  <div class="page-container">
    <div class="page-header">
      <div class="header-titles">
        <h1 class="page-title">{{ t('请求日志', 'Request Logs') }}</h1>
        <p class="page-subtitle">
          {{ t('实时查看并下载服务器上的请求日志', 'View and download request logs from the server in real-time') }}
        </p>
      </div>
    </div>

    <div class="table-container">
      <div class="table-toolbar">
        <div class="refresh-status">
          <span
            class="refresh-indicator"
            :class="{ 'is-active': isAutoRefreshing, 'is-error': !!autoRefreshError }"
          />
          <span class="refresh-text">{{ refreshStatusText }}</span>
        </div>
        <NButton
          secondary
          :loading="isLoading"
          :disabled="isAutoRefreshing"
          @click="refresh(false)"
        >
          {{ t('刷新', 'Refresh') }}
        </NButton>
      </div>

      <NDataTable
        :columns="columns"
        :data="logs"
        :loading="isLoading"
        :bordered="true"
        :single-line="false"
        size="small"
        :row-key="(row) => row.name"
      />
    </div>
  </div>
</template>

<style scoped>
.page-container {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}

.header-titles {
  display: grid;
  gap: 6px;
}

.page-title {
  margin: 0;
  color: var(--cpa-text-strong);
  font-size: 24px;
  font-weight: 760;
  line-height: 1.2;
}

.page-subtitle {
  margin: 0;
  color: var(--cpa-text-muted);
  font-size: 13px;
  line-height: 1.5;
}

.table-container {
  display: flex;
  flex-direction: column;
  gap: 12px;
  background: var(--cpa-surface-card);
  border: 1px solid var(--cpa-border);
  border-radius: var(--cpa-radius-lg);
  padding: 16px;
}

.table-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.refresh-status {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 0 4px;
}

.refresh-indicator {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background-color: var(--cpa-text-muted);
  transition: all 0.3s ease;
}

.refresh-indicator.is-active {
  background-color: var(--cpa-primary);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--cpa-primary) 20%, transparent);
  animation: pulse 1.5s infinite;
}

.refresh-indicator.is-error {
  background-color: var(--cpa-error);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--cpa-error) 20%, transparent);
}

.refresh-text {
  color: var(--cpa-text-muted);
  font-size: 12px;
}

@keyframes pulse {
  0% {
    transform: scale(0.95);
    opacity: 0.8;
  }
  50% {
    transform: scale(1.05);
    opacity: 1;
  }
  100% {
    transform: scale(0.95);
    opacity: 0.8;
  }
}
</style>
