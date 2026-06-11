import { apiClient } from '@/shared/api/apiClient'

export interface ServerLogFile {
  name: string
  time: string
  size: number
}

export async function getServerLogs(): Promise<ServerLogFile[]> {
  return apiClient.get<ServerLogFile[]>('/server-logs')
}

export function getServerLogDownloadUrl(name: string): string {
  return `/api/server-logs/download?name=${encodeURIComponent(name)}`
}
