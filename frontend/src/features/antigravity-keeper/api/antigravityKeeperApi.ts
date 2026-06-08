import { apiClient } from '@/shared/api/apiClient'
import type {
  AntigravityKeeperBulkDeletePayload,
  AntigravityKeeperBulkDeleteResponse,
  AntigravityKeeperCronPreviewPayload,
  AntigravityKeeperCronPreviewResponse,
  AntigravityKeeperAccountsResponse,
  AntigravityKeeperRefreshPayload,
  AntigravityKeeperSettings,
  AntigravityKeeperSettingsUpdatePayload,
  AntigravityKeeperStatus,
} from '@/shared/types/api'

export function getAntigravityKeeperSettings(): Promise<AntigravityKeeperSettings> {
  return apiClient.get<AntigravityKeeperSettings>('/antigravity-keeper/settings')
}

export function updateAntigravityKeeperSettings(
  payload: AntigravityKeeperSettingsUpdatePayload,
): Promise<AntigravityKeeperSettings> {
  return apiClient.put<AntigravityKeeperSettings>('/antigravity-keeper/settings', payload)
}

export function previewAntigravityKeeperSchedule(
  payload: AntigravityKeeperCronPreviewPayload,
): Promise<AntigravityKeeperCronPreviewResponse> {
  return apiClient.post<AntigravityKeeperCronPreviewResponse>('/antigravity-keeper/schedule/preview', payload)
}

export function getAntigravityKeeperStatus(): Promise<AntigravityKeeperStatus> {
  return apiClient.get<AntigravityKeeperStatus>('/antigravity-keeper/status')
}

export function listAntigravityKeeperAccounts(): Promise<AntigravityKeeperAccountsResponse> {
  return apiClient.get<AntigravityKeeperAccountsResponse>('/antigravity-keeper/accounts')
}

export function runAntigravityKeeperOnce(): Promise<void> {
  return apiClient.post<void>('/antigravity-keeper/run-once')
}

export function startAntigravityKeeper(): Promise<void> {
  return apiClient.post<void>('/antigravity-keeper/start')
}

export function stopAntigravityKeeper(): Promise<void> {
  return apiClient.post<void>('/antigravity-keeper/stop')
}

export function clearAntigravityKeeperLogs(): Promise<void> {
  return apiClient.post<void>('/antigravity-keeper/logs/clear')
}

export function enableAntigravityKeeperAccount(authName: string): Promise<void> {
  return apiClient.post<void>(`/antigravity-keeper/accounts/${encodeURIComponent(authName)}/enable`)
}

export function disableAntigravityKeeperAccount(authName: string): Promise<void> {
  return apiClient.post<void>(`/antigravity-keeper/accounts/${encodeURIComponent(authName)}/disable`)
}

export function deleteAntigravityKeeperAccount(authName: string): Promise<void> {
  return apiClient.delete(`/antigravity-keeper/accounts/${encodeURIComponent(authName)}`)
}

export function bulkDeleteAntigravityKeeperAccounts(
  payload: AntigravityKeeperBulkDeletePayload,
): Promise<AntigravityKeeperBulkDeleteResponse> {
  return apiClient.post<AntigravityKeeperBulkDeleteResponse>(
    '/antigravity-keeper/accounts/bulk-delete',
    payload,
  )
}

export function refreshAntigravityKeeperAccounts(payload: AntigravityKeeperRefreshPayload): Promise<void> {
  return apiClient.post<void>('/antigravity-keeper/accounts/refresh', payload)
}

export function updateAntigravityKeeperPriority(authName: string, priority: number): Promise<void> {
  return apiClient.patch<void>(`/antigravity-keeper/accounts/${encodeURIComponent(authName)}/priority`, {
    priority,
  })
}
