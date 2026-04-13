// ============================================================
// ExSQL Migrator - Proceso Principal de Electron
// ============================================================

import { app, BrowserWindow, ipcMain, dialog } from 'electron';
import path from 'path';
import fs from 'fs';
import { MigrationEngine } from './migration-engine';
import { SqlGenerator } from './sql-generator';
import {
  DatabaseConnection,
  DetectedTable,
  MigrationOptions,
} from '../shared/types';

let mainWindow: BrowserWindow | null = null;
const engine = new MigrationEngine();

interface ParsedFileInfo {
  filePath: string;
  fileName: string;
  fileSize: number;
  tablesDetected: number;
}

// Estado global
let parsedTables: DetectedTable[] = [];
let parsedFiles: ParsedFileInfo[] = [];

function getFileSizeSafe(filePath: string): number {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return 0;
  }
}

function ensureUniqueTableNames(tables: DetectedTable[]): DetectedTable[] {
  const nameRegistry = new Map<string, number>();

  return tables.map((table) => {
    const count = (nameRegistry.get(table.tableName) ?? 0) + 1;
    nameRegistry.set(table.tableName, count);

    if (count === 1) {
      return table;
    }

    return {
      ...table,
      tableName: `${table.tableName}_${count}`,
    };
  });
}

async function withSqlGenerator<T>(
  config: DatabaseConnection,
  action: (generator: SqlGenerator) => Promise<T>
): Promise<T> {
  const generator = new SqlGenerator(config.dialect);
  await generator.connect(config);

  try {
    return await action(generator);
  } finally {
    await generator.disconnect();
  }
}

function createWindow(): void {
  mainWindow = new BrowserWindow({
    width: 1100,
    height: 750,
    minWidth: 850,
    minHeight: 600,
    title: 'ExSQL Migrator',
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
    },
    backgroundColor: '#f0f0f0',
    autoHideMenuBar: false,
  });

  // Cargar la UI
  mainWindow.loadFile(path.join(__dirname, '..', '..', 'src', 'renderer', 'index.html'));

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

// ============================================================
// IPC Handlers
// ============================================================

/** Seleccionar archivo Excel */
ipcMain.handle('select-file', async () => {
  if (!mainWindow) return null;

  const result = await dialog.showOpenDialog(mainWindow, {
    title: 'Seleccionar archivo Excel',
    filters: [
      { name: 'Archivos Excel', extensions: ['xlsx', 'xls', 'xlsm'] },
      { name: 'Todos los archivos', extensions: ['*'] },
    ],
    properties: ['openFile', 'multiSelections'],
  });

  if (result.canceled || result.filePaths.length === 0) return null;
  return result.filePaths;
});

/** Parsear archivo Excel */
ipcMain.handle('parse-excel', async (_event, filePathsInput: string | string[]) => {
  try {
    const filePaths = Array.isArray(filePathsInput) ? filePathsInput : [filePathsInput];
    parsedFiles = [];

    const aggregatedTables: DetectedTable[] = [];

    for (let index = 0; index < filePaths.length; index++) {
      const filePath = filePaths[index];
      const fileName = path.basename(filePath);

      const fileTables = await engine.parseExcel(filePath, (msg, percent) => {
        const overallPercent = Math.round(((index + (percent / 100)) / filePaths.length) * 100);
        const progressMessage = filePaths.length > 1
          ? `[${index + 1}/${filePaths.length}] ${fileName} · ${msg}`
          : msg;

        mainWindow?.webContents.send('parse-progress', {
          message: progressMessage,
          percent: overallPercent,
        });
      });

      aggregatedTables.push(...fileTables);
      parsedFiles.push({
        filePath,
        fileName,
        fileSize: getFileSizeSafe(filePath),
        tablesDetected: fileTables.length,
      });
    }

    parsedTables = ensureUniqueTableNames(aggregatedTables);

    return {
      success: true,
      tables: parsedTables,
      files: parsedFiles,
      totalFiles: parsedFiles.length,
      totalSize: parsedFiles.reduce((sum, file) => sum + file.fileSize, 0),
    };
  } catch (err: unknown) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Error al parsear el archivo',
    };
  }
});

/** Probar conexión a base de datos */
ipcMain.handle('test-connection', async (_event, config: DatabaseConnection) => {
  return engine.testConnection(config);
});

/** Listar tablas disponibles en la base conectada */
ipcMain.handle('list-database-tables', async (_event, config: DatabaseConnection) => {
  try {
    const tables = await withSqlGenerator(config, (generator) => generator.listTables());
    return { success: true, tables };
  } catch (err: unknown) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Error listando tablas',
    };
  }
});

/** Ejecutar consulta de solo lectura */
ipcMain.handle('run-readonly-query', async (
  _event,
  config: DatabaseConnection,
  query: string,
  maxRows: number = 500
) => {
  try {
    const result = await withSqlGenerator(
      config,
      (generator) => generator.runReadOnlyQuery(query, maxRows)
    );

    return { success: true, result };
  } catch (err: unknown) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Error ejecutando la consulta',
    };
  }
});

/** Obtener puertos por defecto */
ipcMain.handle('get-default-ports', () => {
  return engine.getDefaultPorts();
});

/** Generar preview DDL */
ipcMain.handle('preview-ddl', async (
  _event,
  config: DatabaseConnection,
  options: Partial<MigrationOptions>
) => {
  if (parsedTables.length === 0) {
    return { success: false, error: 'No hay tablas parseadas' };
  }

  try {
    const ddlStatements = engine.generatePreviewDDL(parsedTables, config, options);
    return { success: true, ddl: ddlStatements };
  } catch (err: unknown) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Error generando DDL',
    };
  }
});

/** Iniciar migración */
ipcMain.handle('start-migration', async (
  _event,
  config: DatabaseConnection,
  options: Partial<MigrationOptions>,
  selectedTableIds?: string[]
) => {
  if (parsedTables.length === 0) {
    return { success: false, error: 'No hay tablas para migrar' };
  }

  // Filtrar tablas si se proporcionan IDs
  const tablesToMigrate = selectedTableIds
    ? parsedTables.filter(t => selectedTableIds.includes(t.id))
    : parsedTables;

  try {
    const result = await engine.migrate(
      tablesToMigrate,
      config,
      options,
      (progress) => {
        mainWindow?.webContents.send('migration-progress', progress);
      }
    );

    mainWindow?.webContents.send('migration-complete', result);
    return { success: true, result };
  } catch (err: unknown) {
    const error = err instanceof Error ? err.message : 'Error en la migración';
    mainWindow?.webContents.send('migration-error', error);
    return { success: false, error };
  }
});

/** Cancelar migración */
ipcMain.handle('cancel-migration', () => {
  engine.cancel();
  return { success: true };
});

// ============================================================
// App Lifecycle
// ============================================================

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
