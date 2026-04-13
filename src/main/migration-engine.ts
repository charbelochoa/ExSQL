// ============================================================
// ExSQL Migrator - Motor de Migración
// ============================================================
// Orquesta el proceso completo: parse → analyze → create → insert
// Maneja batches, progreso, errores y cancelación

import {
  DatabaseConnection,
  DetectedTable,
  MigrationOptions,
  MigrationProgress,
  MigrationResult,
  MigrationError,
  MigrationTableResult,
} from '../shared/types';
import { ExcelParser } from './excel-parser';
import { SqlGenerator, DEFAULT_PORTS } from './sql-generator';

/** Opciones por defecto */
const DEFAULT_OPTIONS: MigrationOptions = {
  batchSize: 5000,
  createIndexes: true,
  ifTableExists: 'drop',
  useTransactions: true,
  tablePrefix: '',
  encoding: 'utf-8',
  maxErrors: 100,
  createMigrationLog: true,
};

export class MigrationEngine {
  private parser: ExcelParser;
  private generator: SqlGenerator | null = null;
  private cancelled = false;
  private progress: MigrationProgress;

  constructor() {
    this.parser = new ExcelParser();
    this.progress = this.createInitialProgress();
  }

  /**
   * Paso 1: Parsea el archivo Excel y devuelve las tablas detectadas
   */
  async parseExcel(
    filePath: string,
    onProgress?: (msg: string, percent: number) => void
  ): Promise<DetectedTable[]> {
    return this.parser.parseFile(filePath, onProgress);
  }

  /**
   * Paso 2: Prueba la conexión a la base de datos
   */
  async testConnection(config: DatabaseConnection): Promise<{
    success: boolean;
    message: string;
    serverVersion?: string;
  }> {
    const gen = new SqlGenerator(config.dialect);
    return gen.testConnection(config);
  }

  /**
   * Paso 3: Genera preview del DDL sin ejecutar
   */
  generatePreviewDDL(
    tables: DetectedTable[],
    config: DatabaseConnection,
    options: Partial<MigrationOptions> = {}
  ): string[] {
    const gen = new SqlGenerator(config.dialect);
    const opts = { ...DEFAULT_OPTIONS, ...options };
    return tables.map(t => gen.generateDDL(t, opts));
  }

  /**
   * Paso 4: Ejecuta la migración completa
   */
  async migrate(
    tables: DetectedTable[],
    connection: DatabaseConnection,
    options: Partial<MigrationOptions> = {},
    onProgress?: (progress: MigrationProgress) => void
  ): Promise<MigrationResult> {
    this.cancelled = false;
    const opts = { ...DEFAULT_OPTIONS, ...options };
    const startTime = Date.now();

    // Inicializar progreso
    this.progress = {
      phase: 'creating_tables',
      currentTable: '',
      tablesTotal: tables.length,
      tablesCompleted: 0,
      rowsTotal: tables.reduce((sum, t) => sum + t.rowCount, 0),
      rowsInserted: 0,
      currentBatch: 0,
      totalBatches: 0,
      elapsedMs: 0,
      estimatedRemainingMs: 0,
      errors: [],
      warnings: [],
    };

    const result: MigrationResult = {
      success: true,
      tablesCreated: 0,
      totalRowsInserted: 0,
      totalRowsUpdated: 0,
      totalRowsSkipped: 0,
      totalDuplicateRows: 0,
      totalRowsDeleted: 0,
      totalErrors: 0,
      duration: 0,
      tables: [],
    };

    try {
      // Conectar
      this.generator = new SqlGenerator(connection.dialect);
      await this.generator.connect(connection);

      // Crear tabla de log si se solicita
      if (opts.createMigrationLog) {
        await this.generator.createMigrationLog();
      }

      // Procesar cada tabla
      for (let i = 0; i < tables.length; i++) {
        if (this.cancelled) {
          this.progress.warnings.push('Migración cancelada por el usuario');
          break;
        }

        const table = tables[i];
        const tableStart = Date.now();
        const tableResult: MigrationTableResult = {
          name: opts.tablePrefix ? `${opts.tablePrefix}${table.tableName}` : table.tableName,
          sourceFileName: table.sourceFileName,
          rowsInserted: 0,
          rowsUpdated: 0,
          rowsSkipped: 0,
          duplicateRows: 0,
          deletedRows: 0,
          columnsCreated: table.columns.length,
          warnings: [],
          suggestions: [],
          errors: [] as MigrationError[],
        };

        try {
          // Fase: Crear tabla
          this.progress.phase = 'creating_tables';
          this.progress.currentTable = `${table.tableName} (${table.sourceFileName})`;
          this.updateTiming(startTime);
          onProgress?.({ ...this.progress });

          const preparation = await this.generator.createTable(table, opts);
          if (preparation.created) {
            result.tablesCreated++;
          }

          this.appendUniqueMessages(tableResult.warnings, preparation.warnings);
          this.appendUniqueMessages(tableResult.suggestions, preparation.suggestions);
          this.appendUniqueMessages(this.progress.warnings, preparation.warnings);

          if (!preparation.shouldProcessData) {
            result.tables.push(tableResult);
            result.totalErrors += tableResult.errors.length;
            this.progress.tablesCompleted = i + 1;
            this.updateTiming(startTime);
            onProgress?.({ ...this.progress });
            continue;
          }

          const syncSession = opts.ifTableExists === 'sync'
            ? await this.generator.prepareSyncSession(table, opts.tablePrefix)
            : null;

          if (syncSession) {
            this.appendUniqueMessages(tableResult.warnings, syncSession.warnings);
            this.appendUniqueMessages(tableResult.suggestions, syncSession.suggestions);
            this.appendUniqueMessages(this.progress.warnings, syncSession.warnings);
          }

          // Fase: Insertar datos
          this.progress.phase = 'inserting_data';
          const totalBatches = Math.ceil(table.rowCount / opts.batchSize);
          this.progress.totalBatches = totalBatches;
          this.progress.currentBatch = 0;
          onProgress?.({ ...this.progress });

          // Leer y enviar filas en chunks
          let tableRowsInserted = 0;
          let tableRowsUpdated = 0;
          let tableRowsSkipped = 0;
          let tableDuplicateRows = 0;

          await this.parser.getTableRows(
            table.sourceFilePath,
            table,
            async (rows, chunkIndex) => {
              if (this.cancelled) return;

              this.progress.currentBatch = chunkIndex + 1;

              if (opts.ifTableExists === 'sync' && syncSession) {
                const syncResult = await this.generator!.syncBatch(
                  table.tableName,
                  rows,
                  table.columns,
                  opts.useTransactions,
                  opts.tablePrefix,
                  syncSession
                );

                tableRowsInserted += syncResult.inserted;
                tableRowsUpdated += syncResult.updated;
                tableRowsSkipped += syncResult.skipped;
                tableDuplicateRows += syncResult.duplicateRows;
                this.progress.rowsInserted += rows.length;
                this.appendUniqueMessages(tableResult.warnings, syncResult.warnings);
                this.appendUniqueMessages(this.progress.warnings, syncResult.warnings);

                for (const err of syncResult.errors) {
                  const migError: MigrationError = {
                    table: table.tableName,
                    row: err.row,
                    message: err.message,
                    timestamp: new Date(),
                  };
                  tableResult.errors.push(migError);
                  this.progress.errors.push(migError);

                  if (this.progress.errors.length >= opts.maxErrors) {
                    this.progress.warnings.push(
                      `Se alcanzó el límite de ${opts.maxErrors} errores. Abortando tabla ${table.tableName}`
                    );
                    return;
                  }
                }
              } else {
                const { inserted, errors } = await this.generator!.insertBatch(
                  table.tableName,
                  rows,
                  table.columns,
                  opts.useTransactions,
                  opts.tablePrefix
                );

                tableRowsInserted += inserted;
                this.progress.rowsInserted += rows.length;

                // Registrar errores
                for (const err of errors) {
                  const migError: MigrationError = {
                    table: table.tableName,
                    row: err.row,
                    message: err.message,
                    timestamp: new Date(),
                  };
                  tableResult.errors.push(migError);
                  this.progress.errors.push(migError);

                  if (this.progress.errors.length >= opts.maxErrors) {
                    this.progress.warnings.push(
                      `Se alcanzó el límite de ${opts.maxErrors} errores. Abortando tabla ${table.tableName}`
                    );
                    return;
                  }
                }
              }

              this.updateTiming(startTime);
              onProgress?.({ ...this.progress });
            },
            opts.batchSize
          );

          if (opts.ifTableExists === 'sync' && syncSession) {
            const finalize = await this.generator.finalizeSyncSession(syncSession);
            tableResult.deletedRows = finalize.deletedRows;
            this.appendUniqueMessages(tableResult.warnings, finalize.warnings);
            this.appendUniqueMessages(tableResult.suggestions, finalize.suggestions);
            this.appendUniqueMessages(this.progress.warnings, finalize.warnings);
          }

          tableResult.rowsInserted = tableRowsInserted;
          tableResult.rowsUpdated = tableRowsUpdated;
          tableResult.rowsSkipped = tableRowsSkipped;
          tableResult.duplicateRows = tableDuplicateRows;
          result.totalRowsInserted += tableRowsInserted;
          result.totalRowsUpdated += tableRowsUpdated;
          result.totalRowsSkipped += tableRowsSkipped;
          result.totalDuplicateRows += tableDuplicateRows;
          result.totalRowsDeleted += tableResult.deletedRows;

          // Log de migración
          if (opts.createMigrationLog) {
            await this.generator.logMigration(
                table.sourceFileName,
              table.tableName,
              tableRowsInserted + tableRowsUpdated,
              tableResult.errors.length,
              JSON.stringify(tableResult.errors.slice(0, 10)),
              new Date(tableStart),
              tableResult.errors.length > 0 ? 'partial' : 'success'
            );
          }
        } catch (err: unknown) {
          const errMsg = err instanceof Error ? err.message : 'Error desconocido';
          tableResult.errors.push({
            table: table.tableName,
            message: `Error crítico: ${errMsg}`,
            timestamp: new Date(),
          });

          if (opts.createMigrationLog && this.generator) {
            await this.generator.logMigration(
              table.sourceFileName,
              table.tableName,
              tableResult.rowsInserted + tableResult.rowsUpdated,
              tableResult.errors.length,
              errMsg,
              new Date(tableStart),
              'error'
            );
          }
        }

        result.tables.push(tableResult);
        result.totalErrors += tableResult.errors.length;
        this.progress.tablesCompleted = i + 1;
        this.updateTiming(startTime);
        onProgress?.({ ...this.progress });
      }

      // Fase: Crear índices (ya se hacen en createTable)
      this.progress.phase = 'done';
      this.updateTiming(startTime);
      onProgress?.({ ...this.progress });

    } catch (err: unknown) {
      result.success = false;
      this.progress.phase = 'error';
      const msg = err instanceof Error ? err.message : 'Error desconocido';
      this.progress.errors.push({
        table: 'GLOBAL',
        message: msg,
        timestamp: new Date(),
      });
      onProgress?.({ ...this.progress });
    } finally {
      // Desconectar
      if (this.generator) {
        await this.generator.disconnect();
        this.generator = null;
      }
    }

    result.duration = Date.now() - startTime;
    result.success = result.success && result.totalErrors === 0;

    return result;
  }

  /**
   * Cancela la migración en curso
   */
  cancel(): void {
    this.cancelled = true;
  }

  /**
   * Retorna los puertos por defecto
   */
  getDefaultPorts(): Record<string, number> {
    return { ...DEFAULT_PORTS };
  }

  /**
   * Actualiza el timing del progreso
   */
  private updateTiming(startTime: number): void {
    this.progress.elapsedMs = Date.now() - startTime;

    if (this.progress.rowsInserted > 0 && this.progress.rowsTotal > 0) {
      const rate = this.progress.rowsInserted / this.progress.elapsedMs;
      const remaining = this.progress.rowsTotal - this.progress.rowsInserted;
      this.progress.estimatedRemainingMs = Math.round(remaining / rate);
    }
  }

  /**
   * Crea un progreso inicial limpio
   */
  private createInitialProgress(): MigrationProgress {
    return {
      phase: 'parsing',
      currentTable: '',
      tablesTotal: 0,
      tablesCompleted: 0,
      rowsTotal: 0,
      rowsInserted: 0,
      currentBatch: 0,
      totalBatches: 0,
      elapsedMs: 0,
      estimatedRemainingMs: 0,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Inserta mensajes sin duplicarlos en el acumulador de advertencias o sugerencias.
   */
  private appendUniqueMessages(target: string[], messages: string[]): void {
    for (const message of messages) {
      if (message && !target.includes(message)) {
        target.push(message);
      }
    }
  }
}
