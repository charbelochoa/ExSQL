// ============================================================
// ExSQL Migrator - Preload Script (Context Bridge)
// ============================================================
// Expone APIs seguras al renderer process

import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('exsql', {
  // Archivo
  selectFile: () => ipcRenderer.invoke('select-file'),
  parseExcel: (filePaths: string | string[]) => ipcRenderer.invoke('parse-excel', filePaths),

  // Conexión
  testConnection: (config: unknown) => ipcRenderer.invoke('test-connection', config),
  getDefaultPorts: () => ipcRenderer.invoke('get-default-ports'),
  listDatabaseTables: (config: unknown) => ipcRenderer.invoke('list-database-tables', config),
  runReadOnlyQuery: (config: unknown, query: string, maxRows?: number) =>
    ipcRenderer.invoke('run-readonly-query', config, query, maxRows),

  // DDL Preview
  previewDDL: (config: unknown, options: unknown) =>
    ipcRenderer.invoke('preview-ddl', config, options),

  // Migración
  startMigration: (config: unknown, options: unknown, tableIds?: string[]) =>
    ipcRenderer.invoke('start-migration', config, options, tableIds),
  cancelMigration: () => ipcRenderer.invoke('cancel-migration'),

  // Listeners de progreso
  onParseProgress: (callback: (data: { message: string; percent: number }) => void) => {
    const handler = (_event: unknown, data: { message: string; percent: number }) => callback(data);
    ipcRenderer.on('parse-progress', handler);
    return () => ipcRenderer.removeListener('parse-progress', handler);
  },

  onMigrationProgress: (callback: (progress: unknown) => void) => {
    const handler = (_event: unknown, progress: unknown) => callback(progress);
    ipcRenderer.on('migration-progress', handler);
    return () => ipcRenderer.removeListener('migration-progress', handler);
  },

  onMigrationComplete: (callback: (result: unknown) => void) => {
    const handler = (_event: unknown, result: unknown) => callback(result);
    ipcRenderer.on('migration-complete', handler);
    return () => ipcRenderer.removeListener('migration-complete', handler);
  },

  onMigrationError: (callback: (error: string) => void) => {
    const handler = (_event: unknown, error: string) => callback(error);
    ipcRenderer.on('migration-error', handler);
    return () => ipcRenderer.removeListener('migration-error', handler);
  },
});
