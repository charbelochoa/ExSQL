// ============================================================
// ExSQL Migrator - Generador SQL Multi-Dialecto
// ============================================================
// Genera DDL (CREATE TABLE) y DML (INSERT) para MySQL, PostgreSQL, SQL Server
// usando Knex.js como abstracción de queries

import Knex, { Knex as KnexType } from 'knex';
import {
  SqlDialect,
  SqlDataType,
  DatabaseConnection,
  DatabaseQueryResult,
  DatabaseTableInfo,
  DetectedTable,
  ColumnDefinition,
  MigrationOptions,
} from '../shared/types';

/** Mapeo de tipos internos a tipos específicos por dialecto */
const TYPE_MAP: Record<SqlDialect, Record<SqlDataType, string>> = {
  mysql: {
    INTEGER: 'INT',
    BIGINT: 'BIGINT',
    DECIMAL: 'DECIMAL',
    FLOAT: 'DOUBLE',
    BOOLEAN: 'TINYINT(1)',
    DATE: 'DATE',
    DATETIME: 'DATETIME',
    TIME: 'TIME',
    VARCHAR: 'VARCHAR',
    TEXT: 'LONGTEXT',
    JSON: 'JSON',
  },
  postgresql: {
    INTEGER: 'INTEGER',
    BIGINT: 'BIGINT',
    DECIMAL: 'NUMERIC',
    FLOAT: 'DOUBLE PRECISION',
    BOOLEAN: 'BOOLEAN',
    DATE: 'DATE',
    DATETIME: 'TIMESTAMP',
    TIME: 'TIME',
    VARCHAR: 'VARCHAR',
    TEXT: 'TEXT',
    JSON: 'JSONB',
  },
  mssql: {
    INTEGER: 'INT',
    BIGINT: 'BIGINT',
    DECIMAL: 'DECIMAL',
    FLOAT: 'FLOAT',
    BOOLEAN: 'BIT',
    DATE: 'DATE',
    DATETIME: 'DATETIME2',
    TIME: 'TIME',
    VARCHAR: 'NVARCHAR',
    TEXT: 'NVARCHAR(MAX)',
    JSON: 'NVARCHAR(MAX)',
  },
};

/** Puertos por defecto */
export const DEFAULT_PORTS: Record<SqlDialect, number> = {
  mysql: 3306,
  postgresql: 5432,
  mssql: 1433,
};

const IDENTITY_COLUMN_PATTERNS = [
  /(^|_)(id|codigo|cod|clave|barcode|ean|upc|sku|folio|serie|serial|documento|dni|ruc|nit|cedula|cuenta|referencia|ticket|uuid)(_|$)/i,
];

interface TablePreparationResult {
  created: boolean;
  shouldProcessData: boolean;
  warnings: string[];
  suggestions: string[];
}

interface SyncSession {
  fullName: string;
  keyColumns: string[];
  seenKeys: Set<string>;
  hasExistingData: boolean;
  canUpdateExistingRows: boolean;
  canDeleteMissingRows: boolean;
  duplicateDatabaseKeys: Set<string>;
  warnings: string[];
  suggestions: string[];
}

interface SyncBatchResult {
  inserted: number;
  updated: number;
  skipped: number;
  duplicateRows: number;
  errors: Array<{ row: number; message: string }>;
  warnings: string[];
}

interface SyncFinalizeResult {
  deletedRows: number;
  warnings: string[];
  suggestions: string[];
}

export class SqlGenerator {
  private knex: KnexType | null = null;
  private dialect: SqlDialect;

  constructor(dialect: SqlDialect) {
    this.dialect = dialect;
  }

  /**
   * Crea una conexión a la base de datos
   */
  async connect(config: DatabaseConnection): Promise<void> {
    const knexConfig = this.buildKnexConfig(config);
    this.knex = Knex(knexConfig);

    // Test de conexión
    await this.knex.raw('SELECT 1');
  }

  /**
   * Prueba la conexión sin mantenerla abierta
   */
  async testConnection(config: DatabaseConnection): Promise<{
    success: boolean;
    message: string;
    serverVersion?: string;
  }> {
    let testKnex: KnexType | null = null;
    try {
      const knexConfig = this.buildKnexConfig(config);
      testKnex = Knex(knexConfig);
      await testKnex.raw('SELECT 1');

      // Obtener versión del servidor
      let version = 'Desconocida';
      try {
        if (config.dialect === 'mysql') {
          const [rows] = await testKnex.raw('SELECT VERSION() as version');
          version = rows[0]?.version || version;
        } else if (config.dialect === 'postgresql') {
          const result = await testKnex.raw('SHOW server_version');
          version = result.rows?.[0]?.server_version || version;
        } else if (config.dialect === 'mssql') {
          const result = await testKnex.raw('SELECT @@VERSION as version');
          version = result[0]?.version?.split('\n')[0] || version;
        }
      } catch {
        // No es crítico si falla obtener la versión
      }

      return {
        success: true,
        message: `Conexión exitosa a ${config.dialect.toUpperCase()}`,
        serverVersion: version,
      };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Error desconocido';
      return {
        success: false,
        message: `Error de conexión: ${message}`,
      };
    } finally {
      if (testKnex) await testKnex.destroy();
    }
  }

  /**
   * Crea una tabla en la base de datos
   */
  async createTable(
    table: DetectedTable,
    options: MigrationOptions
  ): Promise<TablePreparationResult> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const fullName = options.tablePrefix
      ? `${options.tablePrefix}${table.tableName}`
      : table.tableName;
    const result: TablePreparationResult = {
      created: false,
      shouldProcessData: true,
      warnings: [],
      suggestions: [],
    };

    // Verificar si la tabla existe
    const exists = await this.knex.schema.hasTable(fullName);

    if (exists) {
      switch (options.ifTableExists) {
        case 'drop':
          await this.knex.schema.dropTable(fullName);
          break;

        case 'truncate': {
          const structure = await this.ensureTableStructure(fullName, table.columns, options.createIndexes);
          await this.knex(fullName).truncate();
          result.warnings.push(...structure.warnings);
          result.suggestions.push(...structure.suggestions);
          return result;
        }

        case 'append': {
          const structure = await this.ensureTableStructure(fullName, table.columns, options.createIndexes);
          result.warnings.push(...structure.warnings);
          result.suggestions.push(...structure.suggestions);
          return result;
        }

        case 'skip':
          result.shouldProcessData = false;
          result.warnings.push(`La tabla ${fullName} se omitió porque ya existe.`);
          return result;

        case 'sync': {
          const structure = await this.ensureTableStructure(fullName, table.columns, options.createIndexes);
          result.warnings.push(...structure.warnings);
          result.suggestions.push(...structure.suggestions);
          return result;
        }
      }
    }

    // Crear la tabla
    await this.knex.schema.createTable(fullName, (builder) => {
      // ID autoincremental
      builder.increments('id').primary();

      for (const col of table.columns) {
        this.addColumn(builder, col);
      }

      // Timestamps de control
      builder.timestamp('_migrated_at').defaultTo(this.knex!.fn.now());
    });

    result.created = true;

    const indexResult = await this.ensureCandidateIndexes(fullName, table.columns, options.createIndexes);
    result.warnings.push(...indexResult.warnings);
    result.suggestions.push(...indexResult.suggestions);

    return result;
  }

  /**
   * Inserta un batch de filas en una tabla
   */
  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[],
    columns: ColumnDefinition[],
    useTransaction: boolean,
    tablePrefix: string = ''
  ): Promise<{ inserted: number; errors: Array<{ row: number; message: string }> }> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const fullName = tablePrefix ? `${tablePrefix}${tableName}` : tableName;
    const errors: Array<{ row: number; message: string }> = [];

    // Transformar y limpiar los datos antes de insertar
    const cleanRows = rows.map((row, idx) => {
      try {
        return this.transformRow(row, columns);
      } catch (err) {
        errors.push({
          row: idx,
          message: err instanceof Error ? err.message : 'Error de transformación',
        });
        return null;
      }
    }).filter((r): r is Record<string, unknown> => r !== null);

    if (cleanRows.length === 0) {
      return { inserted: 0, errors };
    }

    try {
      if (useTransaction) {
        await this.knex.transaction(async (trx) => {
          // Insertar en sub-batches para evitar límites de parámetros
          const subBatchSize = this.getSubBatchSize();
          for (let i = 0; i < cleanRows.length; i += subBatchSize) {
            const subBatch = cleanRows.slice(i, i + subBatchSize);
            await trx(fullName).insert(subBatch);
          }
        });
      } else {
        const subBatchSize = this.getSubBatchSize();
        for (let i = 0; i < cleanRows.length; i += subBatchSize) {
          const subBatch = cleanRows.slice(i, i + subBatchSize);
          await this.knex(fullName).insert(subBatch);
        }
      }

      return { inserted: cleanRows.length, errors };
    } catch (err: unknown) {
      // Si falla el batch completo, intentar fila por fila
      return this.insertRowByRow(fullName, cleanRows, errors);
    }
  }

  /**
   * Inserta fila por fila cuando un batch falla (fallback)
   */
  private async insertRowByRow(
    tableName: string,
    rows: Record<string, unknown>[],
    existingErrors: Array<{ row: number; message: string }>
  ): Promise<{ inserted: number; errors: Array<{ row: number; message: string }> }> {
    if (!this.knex) throw new Error('No hay conexión activa');

    let inserted = 0;
    const errors = [...existingErrors];

    for (let i = 0; i < rows.length; i++) {
      try {
        await this.knex(tableName).insert(rows[i]);
        inserted++;
      } catch (err: unknown) {
        errors.push({
          row: i,
          message: err instanceof Error ? err.message : 'Error de inserción',
        });
      }
    }

    return { inserted, errors };
  }

  /**
   * Transforma una fila de datos crudos al formato adecuado para SQL
   */
  private transformRow(
    row: Record<string, unknown>,
    columns: ColumnDefinition[]
  ): Record<string, unknown> {
    const transformed: Record<string, unknown> = {};

    for (const col of columns) {
      let value = row[col.sqlName];

      // Null handling
      if (value === null || value === undefined || value === '') {
        transformed[col.sqlName] = null;
        continue;
      }

      // Transformar según el tipo
      switch (col.dataType) {
        case 'INTEGER':
        case 'BIGINT': {
          const num = typeof value === 'number' ? value : parseInt(String(value), 10);
          transformed[col.sqlName] = isNaN(num) ? null : num;
          break;
        }

        case 'DECIMAL':
        case 'FLOAT': {
          const num = typeof value === 'number' ? value : parseFloat(String(value));
          transformed[col.sqlName] = isNaN(num) ? null : num;
          break;
        }

        case 'BOOLEAN': {
          const str = String(value).toLowerCase().trim();
          const truthy = ['true', 'yes', 'si', 'sí', '1', 'verdadero', 'v', 'y', 'activo', 'active'];
          transformed[col.sqlName] = truthy.includes(str) ? 1 : 0;
          break;
        }

        case 'DATE': {
          if (value instanceof Date) {
            transformed[col.sqlName] = value.toISOString().split('T')[0];
          } else {
            const parsed = new Date(String(value));
            transformed[col.sqlName] = isNaN(parsed.getTime()) ? String(value) : parsed.toISOString().split('T')[0];
          }
          break;
        }

        case 'DATETIME': {
          if (value instanceof Date) {
            transformed[col.sqlName] = value.toISOString().replace('T', ' ').replace('Z', '');
          } else {
            const parsed = new Date(String(value));
            transformed[col.sqlName] = isNaN(parsed.getTime()) ? String(value) : parsed.toISOString().replace('T', ' ').replace('Z', '');
          }
          break;
        }

        case 'TIME': {
          transformed[col.sqlName] = String(value);
          break;
        }

        case 'JSON': {
          transformed[col.sqlName] = typeof value === 'string' ? value : JSON.stringify(value);
          break;
        }

        case 'VARCHAR':
        case 'TEXT':
        default: {
          transformed[col.sqlName] = String(value);
          break;
        }
      }
    }

    return transformed;
  }

  /**
   * Agrega una columna al schema builder de Knex
   */
  private addColumn(builder: KnexType.CreateTableBuilder, col: ColumnDefinition): void {
    let chain: KnexType.ColumnBuilder;

    switch (col.dataType) {
      case 'INTEGER':
        chain = builder.integer(col.sqlName);
        break;
      case 'BIGINT':
        chain = builder.bigInteger(col.sqlName);
        break;
      case 'DECIMAL':
        chain = builder.decimal(col.sqlName, col.precision || 18, col.scale || 4);
        break;
      case 'FLOAT':
        chain = builder.float(col.sqlName);
        break;
      case 'BOOLEAN':
        chain = builder.boolean(col.sqlName);
        break;
      case 'DATE':
        chain = builder.date(col.sqlName);
        break;
      case 'DATETIME':
        chain = builder.datetime(col.sqlName);
        break;
      case 'TIME':
        chain = builder.time(col.sqlName);
        break;
      case 'TEXT':
        chain = builder.text(col.sqlName, 'longtext');
        break;
      case 'JSON':
        if (this.dialect === 'postgresql') {
          chain = builder.jsonb(col.sqlName);
        } else {
          chain = builder.text(col.sqlName, 'longtext');
        }
        break;
      case 'VARCHAR':
      default:
        chain = builder.string(col.sqlName, col.maxLength || 255);
        break;
    }

    if (col.nullable) {
      chain.nullable();
    } else {
      chain.notNullable();
    }
  }

  /**
   * Sub-batch size basado en el dialecto para no exceder límites de parámetros
   */
  private getSubBatchSize(): number {
    switch (this.dialect) {
      case 'mssql':
        return 500;  // SQL Server tiene límite de ~2100 parámetros
      case 'mysql':
        return 1000;
      case 'postgresql':
        return 1000;
      default:
        return 500;
    }
  }

  /**
   * Genera el DDL como string (para preview sin ejecutar)
   */
  generateDDL(table: DetectedTable, options: MigrationOptions): string {
    const fullName = options.tablePrefix
      ? `${options.tablePrefix}${table.tableName}`
      : table.tableName;

    const typeMap = TYPE_MAP[this.dialect];
    const lines: string[] = [];

    lines.push(`CREATE TABLE ${this.quoteIdentifier(fullName)} (`);
    lines.push(`  ${this.quoteIdentifier('id')} ${this.getAutoIncrementSyntax()},`);

    for (let i = 0; i < table.columns.length; i++) {
      const col = table.columns[i];
      let typeDef = typeMap[col.dataType];

      // Agregar longitud/precisión
      if (col.dataType === 'VARCHAR') {
        typeDef = `${typeDef}(${col.maxLength || 255})`;
      } else if (col.dataType === 'DECIMAL') {
        typeDef = `${typeDef}(${col.precision || 18}, ${col.scale || 4})`;
      }

      const nullable = col.nullable ? 'NULL' : 'NOT NULL';
      const comma = i < table.columns.length - 1 ? ',' : ',';

      lines.push(`  ${this.quoteIdentifier(col.sqlName)} ${typeDef} ${nullable}${comma}`);
    }

    lines.push(`  ${this.quoteIdentifier('_migrated_at')} ${typeMap['DATETIME']} DEFAULT ${this.getCurrentTimestampSyntax()}`);
    lines.push(');');

    return lines.join('\n');
  }

  /**
   * Prepara una sesión de sincronización incremental para una tabla existente.
   */
  async prepareSyncSession(
    table: DetectedTable,
    tablePrefix: string = ''
  ): Promise<SyncSession> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const fullName = tablePrefix ? `${tablePrefix}${table.tableName}` : table.tableName;
    const keyResolution = this.resolveSyncKeyColumns(table.columns);
    const session: SyncSession = {
      fullName,
      keyColumns: keyResolution.keyColumns,
      seenKeys: new Set<string>(),
      hasExistingData: false,
      canUpdateExistingRows: keyResolution.keyColumns.length > 0,
      canDeleteMissingRows: keyResolution.keyColumns.length > 0,
      duplicateDatabaseKeys: new Set<string>(),
      warnings: [...keyResolution.warnings],
      suggestions: [...keyResolution.suggestions],
    };

    const exists = await this.knex.schema.hasTable(fullName);
    if (!exists) {
      return session;
    }

    const existingRowCount = await this.getTableRowCount(fullName);
    session.hasExistingData = existingRowCount > 0;

    if (!session.hasExistingData) {
      return session;
    }

    if (session.keyColumns.length === 0) {
      session.canUpdateExistingRows = false;
      session.canDeleteMissingRows = false;
      session.warnings.push(
        `La tabla ${fullName} ya existe con datos, pero no se encontró una clave confiable para sincronizarla sin duplicados.`
      );
      session.suggestions.push(
        'Agrega una columna identificadora única en Excel o usa el modo Eliminar y recrear para reemplazar completamente la tabla.'
      );
      return session;
    }

    const duplicateKeys = await this.findDuplicateKeys(fullName, session.keyColumns, 20);
    duplicateKeys.forEach((key) => session.duplicateDatabaseKeys.add(key));

    if (duplicateKeys.length > 0) {
      session.warnings.push(
        `Se detectaron claves duplicadas en la base de datos para ${session.keyColumns.join(', ')}. Esas filas no se actualizarán automáticamente.`
      );
      session.suggestions.push(
        `Corrige los duplicados existentes en ${fullName} y considera agregar una restricción única sobre ${session.keyColumns.join(', ')}.`
      );
    }

    return session;
  }

  /**
   * Sincroniza un chunk de filas contra la tabla destino, insertando y actualizando sin duplicar.
   */
  async syncBatch(
    tableName: string,
    rows: Record<string, unknown>[],
    columns: ColumnDefinition[],
    useTransaction: boolean,
    tablePrefix: string = '',
    session?: SyncSession
  ): Promise<SyncBatchResult> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const fullName = tablePrefix ? `${tablePrefix}${tableName}` : tableName;
    const activeSession = session ?? {
      fullName,
      keyColumns: [],
      seenKeys: new Set<string>(),
      hasExistingData: false,
      canUpdateExistingRows: false,
      canDeleteMissingRows: false,
      duplicateDatabaseKeys: new Set<string>(),
      warnings: [],
      suggestions: [],
    };

    const errors: Array<{ row: number; message: string }> = [];
    const warnings: string[] = [];

    const cleanRows = rows.map((row, idx) => {
      try {
        return this.transformRow(row, columns);
      } catch (err) {
        errors.push({
          row: idx,
          message: err instanceof Error ? err.message : 'Error de transformación',
        });
        return null;
      }
    }).filter((row): row is Record<string, unknown> => row !== null);

    if (cleanRows.length === 0) {
      return { inserted: 0, updated: 0, skipped: 0, duplicateRows: 0, errors, warnings };
    }

    if (activeSession.keyColumns.length === 0) {
      if (activeSession.hasExistingData) {
        warnings.push(
          `Se omitieron ${cleanRows.length} filas en ${fullName} porque no hay una clave de sincronización confiable para evitar duplicados.`
        );
        return {
          inserted: 0,
          updated: 0,
          skipped: cleanRows.length,
          duplicateRows: 0,
          errors,
          warnings,
        };
      }

      const insertResult = await this.insertPreparedRows(fullName, cleanRows, useTransaction);
      return {
        inserted: insertResult.inserted,
        updated: 0,
        skipped: 0,
        duplicateRows: 0,
        errors: [...errors, ...insertResult.errors],
        warnings,
      };
    }

    let skipped = 0;
    let duplicateRows = 0;
    let missingKeyRows = 0;
    const candidateRows: Array<{ key: string; row: Record<string, unknown> }> = [];

    for (const row of cleanRows) {
      const rowKey = this.buildCompositeKey(row, activeSession.keyColumns);

      if (!rowKey) {
        skipped++;
        missingKeyRows++;
        continue;
      }

      if (activeSession.seenKeys.has(rowKey)) {
        skipped++;
        duplicateRows++;
        continue;
      }

      activeSession.seenKeys.add(rowKey);
      candidateRows.push({ key: rowKey, row });
    }

    if (missingKeyRows > 0) {
      warnings.push(
        `Se omitieron ${missingKeyRows} filas sin valor completo en la clave de sincronización (${activeSession.keyColumns.join(', ')}).`
      );
    }

    if (candidateRows.length === 0) {
      return { inserted: 0, updated: 0, skipped, duplicateRows, errors, warnings };
    }

    const existingRows = activeSession.hasExistingData
      ? await this.fetchRowsByKeys(fullName, activeSession.keyColumns, candidateRows.map((entry) => entry.row), columns)
      : [];

    const existingMap = new Map<string, Record<string, unknown>[]>();
    for (const existingRow of existingRows) {
      const existingKey = this.buildCompositeKey(existingRow, activeSession.keyColumns);
      if (!existingKey) continue;

      const bucket = existingMap.get(existingKey) ?? [];
      bucket.push(existingRow);
      existingMap.set(existingKey, bucket);
    }

    const rowsToInsert: Record<string, unknown>[] = [];
    const rowsToUpdate: Record<string, unknown>[] = [];
    let ambiguousRows = 0;

    for (const entry of candidateRows) {
      const matches = existingMap.get(entry.key) ?? [];

      if (matches.length === 0) {
        rowsToInsert.push(entry.row);
        continue;
      }

      if (matches.length > 1 || activeSession.duplicateDatabaseKeys.has(entry.key)) {
        ambiguousRows++;
        skipped++;
        duplicateRows++;
        continue;
      }

      if (this.rowsAreEquivalent(entry.row, matches[0], columns)) {
        skipped++;
        continue;
      }

      rowsToUpdate.push(entry.row);
    }

    if (ambiguousRows > 0) {
      warnings.push(
        `Se omitieron ${ambiguousRows} filas porque la base de datos ya contiene claves duplicadas para ${activeSession.keyColumns.join(', ')}.`
      );
    }

    const insertResult = await this.insertPreparedRows(fullName, rowsToInsert, useTransaction);
    const updateResult = await this.updatePreparedRows(
      fullName,
      activeSession.keyColumns,
      rowsToUpdate,
      columns,
      useTransaction
    );

    return {
      inserted: insertResult.inserted,
      updated: updateResult.updated,
      skipped,
      duplicateRows,
      errors: [...errors, ...insertResult.errors, ...updateResult.errors],
      warnings,
    };
  }

  /**
   * Elimina filas que ya no aparecen en el Excel para mantener sincronizada la tabla.
   */
  async finalizeSyncSession(session: SyncSession): Promise<SyncFinalizeResult> {
    if (!this.knex) throw new Error('No hay conexión activa');

    if (!session.keyColumns.length || !session.hasExistingData || !session.canDeleteMissingRows) {
      return { deletedRows: 0, warnings: [], suggestions: [] };
    }

    const existingKeyRows = await this.knex(session.fullName).select(session.keyColumns);
    const tuplesToDelete = new Map<string, unknown[]>();

    for (const rawRow of existingKeyRows as Record<string, unknown>[]) {
      const normalizedRow = this.normalizeRow(rawRow);
      const rowKey = this.buildCompositeKey(normalizedRow, session.keyColumns);
      if (!rowKey || session.duplicateDatabaseKeys.has(rowKey) || session.seenKeys.has(rowKey)) {
        continue;
      }

      tuplesToDelete.set(rowKey, session.keyColumns.map((column) => normalizedRow[column]));
    }

    const tuples = [...tuplesToDelete.values()];
    if (tuples.length === 0) {
      return { deletedRows: 0, warnings: [], suggestions: [] };
    }

    await this.deleteRowsByKeys(session.fullName, session.keyColumns, tuples);

    return {
      deletedRows: tuples.length,
      warnings: [
        `Se eliminaron ${tuples.length} filas ausentes en el Excel para mantener sincronizada la tabla ${session.fullName}.`,
      ],
      suggestions: [],
    };
  }

  /**
   * Lista las tablas base disponibles en la base de datos actual.
   */
  async listTables(): Promise<DatabaseTableInfo[]> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const databaseName = this.getCurrentDatabaseName();
    let rawResult: unknown;

    switch (this.dialect) {
      case 'mysql':
        rawResult = await this.knex.raw(
          `SELECT table_name AS name
           FROM information_schema.tables
           WHERE table_schema = ?
             AND table_type = 'BASE TABLE'
           ORDER BY table_name`,
          [databaseName]
        );
        break;

      case 'postgresql':
        rawResult = await this.knex.raw(
          `SELECT table_name AS name
           FROM information_schema.tables
           WHERE table_catalog = ?
             AND table_schema = 'public'
             AND table_type = 'BASE TABLE'
           ORDER BY table_name`,
          [databaseName]
        );
        break;

      case 'mssql':
        rawResult = await this.knex.raw(
          `SELECT TABLE_NAME AS name
           FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_CATALOG = ?
             AND TABLE_TYPE = 'BASE TABLE'
           ORDER BY TABLE_NAME`,
          [databaseName]
        );
        break;

      default:
        throw new Error(`Dialecto no soportado: ${this.dialect}`);
    }

    return this.normalizeQueryRows(rawResult)
      .map((row) => ({
        name: String(row.name ?? row.table_name ?? row.TABLE_NAME ?? Object.values(row)[0] ?? ''),
      }))
      .filter((table) => table.name.length > 0);
  }

  /**
   * Ejecuta una consulta de solo lectura para exploración y gráficos.
   */
  async runReadOnlyQuery(query: string, maxRows: number = 500): Promise<DatabaseQueryResult> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const sanitizedQuery = this.sanitizeReadOnlyQuery(query);
    const rawResult = await this.knex.raw(sanitizedQuery);
    const normalizedRows = this.normalizeQueryRows(rawResult);
    const columns = this.extractColumns(rawResult, normalizedRows);
    const safeLimit = Math.max(1, Math.min(maxRows, 5000));

    return {
      columns,
      rows: normalizedRows.slice(0, safeLimit),
      rowCount: normalizedRows.length,
      truncated: normalizedRows.length > safeLimit,
      executedQuery: sanitizedQuery,
    };
  }

  /**
   * Garantiza que la tabla existente tenga las columnas necesarias para seguir importando o sincronizando.
   */
  private async ensureTableStructure(
    fullName: string,
    columns: ColumnDefinition[],
    createIndexes: boolean
  ): Promise<{ warnings: string[]; suggestions: string[] }> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const warnings: string[] = [];
    const suggestions: string[] = [];
    const columnInfo = await this.knex(fullName).columnInfo() as Record<string, {
      type?: string;
      maxLength?: number | string | null;
      nullable?: boolean;
    }>;

    const missingColumns = columns.filter((column) => !columnInfo[column.sqlName]);
    const missingMigratedAt = !columnInfo._migrated_at;

    if (missingColumns.length > 0 || missingMigratedAt) {
      await this.knex.schema.alterTable(fullName, (builder) => {
        for (const column of missingColumns) {
          this.addColumn(builder, column);
        }

        if (missingMigratedAt) {
          builder.timestamp('_migrated_at').defaultTo(this.knex!.fn.now());
        }
      });

      if (missingColumns.length > 0) {
        suggestions.push(
          `Se agregaron columnas nuevas en ${fullName}: ${missingColumns.map((column) => column.sqlName).join(', ')}.`
        );
      }
    }

    for (const column of columns) {
      const existing = columnInfo[column.sqlName];
      if (!existing) continue;

      suggestions.push(...this.compareColumnCompatibility(column, existing));
    }

    const indexResult = await this.ensureCandidateIndexes(fullName, columns, createIndexes);
    warnings.push(...indexResult.warnings);
    suggestions.push(...indexResult.suggestions);

    return { warnings, suggestions };
  }

  /**
   * Crea índices candidatos si el usuario lo solicitó.
   */
  private async ensureCandidateIndexes(
    fullName: string,
    columns: ColumnDefinition[],
    createIndexes: boolean
  ): Promise<{ warnings: string[]; suggestions: string[] }> {
    if (!this.knex || !createIndexes) {
      return { warnings: [], suggestions: [] };
    }

    for (const col of columns) {
      if (!col.isPrimaryKeyCandidate || col.dataType === 'TEXT' || col.dataType === 'JSON') {
        continue;
      }

      try {
        await this.knex.schema.alterTable(fullName, (builder) => {
          builder.index([col.sqlName], `idx_${fullName}_${col.sqlName}`);
        });
      } catch {
        // El índice ya puede existir o el dialecto puede no admitirlo con ese nombre.
      }
    }

    return { warnings: [], suggestions: [] };
  }

  /**
   * Compara la inferencia del Excel con el tipo existente en SQL para sugerir correcciones.
   */
  private compareColumnCompatibility(
    column: ColumnDefinition,
    existing: { type?: string; maxLength?: number | string | null; nullable?: boolean }
  ): string[] {
    const suggestions: string[] = [];
    const existingType = String(existing.type || '').toLowerCase();
    const maxLength = existing.maxLength == null ? null : Number(existing.maxLength);

    if ((column.dataType === 'VARCHAR' || column.dataType === 'TEXT') && maxLength && column.maxLength > maxLength) {
      suggestions.push(
        `Revisa ${column.sqlName}: el Excel sugiere longitud ${column.maxLength}, pero la columna SQL reporta ${maxLength}.`
      );
    }

    if (this.isTextColumn(column) && /(int|decimal|numeric|float|double|real|bigint|smallint|tinyint|bit)/.test(existingType)) {
      suggestions.push(
        `Revisa ${column.sqlName}: en Excel se detectó texto, pero en SQL existe como tipo numérico (${existing.type}).`
      );
    }

    if (this.isNumericColumn(column) && /(char|text|nchar|nvarchar|varchar|longtext|mediumtext|tinytext)/.test(existingType)) {
      suggestions.push(
        `Revisa ${column.sqlName}: en Excel se detectó un dato numérico, pero en SQL existe como texto (${existing.type}).`
      );
    }

    return suggestions;
  }

  /**
   * Decide qué columnas usar como clave para la sincronización.
   */
  private resolveSyncKeyColumns(columns: ColumnDefinition[]): {
    keyColumns: string[];
    warnings: string[];
    suggestions: string[];
  } {
    const warnings: string[] = [];
    const suggestions: string[] = [];

    const preferredIdentityColumns = columns.filter((column) => this.isIdentityColumn(column));
    const uniquePreferred = preferredIdentityColumns.filter((column) => column.isPrimaryKeyCandidate && !column.nullable);

    if (uniquePreferred.length > 0) {
      const chosen = this.pickBestSyncColumn(uniquePreferred);
      return { keyColumns: [chosen.sqlName], warnings, suggestions };
    }

    const uniqueCandidates = columns.filter((column) => column.isPrimaryKeyCandidate && !column.nullable);
    if (uniqueCandidates.length > 0) {
      const chosen = this.pickBestSyncColumn(uniqueCandidates);

      if (!this.isIdentityColumn(chosen)) {
        warnings.push(`Se usará ${chosen.sqlName} como clave de sincronización porque luce única en el Excel.`);
      }

      return { keyColumns: [chosen.sqlName], warnings, suggestions };
    }

    const compositeIdentity = preferredIdentityColumns.filter((column) => !column.nullable);
    if (compositeIdentity.length >= 2) {
      const selected = compositeIdentity.slice(0, 2).map((column) => column.sqlName);
      warnings.push(`Se usará una clave compuesta por ${selected.join(', ')} para sincronizar los cambios.`);
      suggestions.push(`Confirma que la combinación ${selected.join(', ')} sea realmente única en el Excel y en SQL.`);
      return { keyColumns: selected, warnings, suggestions };
    }

    const fallbackIdentity = preferredIdentityColumns.find((column) => !column.nullable)
      || columns.find((column) => !column.nullable && column.dataType !== 'TEXT' && column.dataType !== 'JSON');

    if (fallbackIdentity) {
      warnings.push(`Se usará ${fallbackIdentity.sqlName} como clave tentativa para sincronizar.`);
      suggestions.push(`Verifica que ${fallbackIdentity.sqlName} no tenga duplicados; de lo contrario conviene usar reemplazo completo.`);
      return { keyColumns: [fallbackIdentity.sqlName], warnings, suggestions };
    }

    suggestions.push('No se encontró una columna identificadora confiable para sincronizar cambios sin duplicados.');
    return { keyColumns: [], warnings, suggestions };
  }

  /**
   * Busca claves duplicadas existentes en la tabla destino.
   */
  private async findDuplicateKeys(
    fullName: string,
    keyColumns: string[],
    limit: number
  ): Promise<string[]> {
    if (!this.knex || keyColumns.length === 0) return [];

    const rows = await this.knex(fullName)
      .select(keyColumns)
      .count({ duplicate_count: '*' })
      .groupBy(keyColumns)
      .havingRaw('COUNT(*) > 1')
      .limit(limit);

    return (rows as Record<string, unknown>[])
      .map((row) => this.buildCompositeKey(this.normalizeRow(row), keyColumns))
      .filter((key): key is string => Boolean(key));
  }

  /**
   * Obtiene filas existentes para las claves presentes en el chunk actual.
   */
  private async fetchRowsByKeys(
    fullName: string,
    keyColumns: string[],
    rows: Record<string, unknown>[],
    columns: ColumnDefinition[]
  ): Promise<Record<string, unknown>[]> {
    if (!this.knex || keyColumns.length === 0 || rows.length === 0) return [];

    const selectColumns = [...new Set([...keyColumns, ...columns.map((column) => column.sqlName)])];
    const keyTuples = this.buildUniqueKeyTuples(rows, keyColumns);
    const chunkSize = keyColumns.length === 1 ? 500 : 200;
    const matches: Record<string, unknown>[] = [];

    for (let index = 0; index < keyTuples.length; index += chunkSize) {
      const chunk = keyTuples.slice(index, index + chunkSize);
      let query = this.knex(fullName).select(selectColumns) as any;

      if (keyColumns.length === 1) {
        query = query.whereIn(String(keyColumns[0]), chunk.map((tuple) => tuple[0]) as unknown[]);
      } else {
        query = query.whereIn(keyColumns as readonly string[], chunk as ReadonlyArray<readonly unknown[]>);
      }

      const rowsChunk = await query;
      matches.push(...(rowsChunk as Record<string, unknown>[]).map((row) => this.normalizeRow(row)));
    }

    return matches;
  }

  /**
   * Inserta filas ya transformadas.
   */
  private async insertPreparedRows(
    fullName: string,
    rows: Record<string, unknown>[],
    useTransaction: boolean
  ): Promise<{ inserted: number; errors: Array<{ row: number; message: string }> }> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const errors: Array<{ row: number; message: string }> = [];
    if (rows.length === 0) {
      return { inserted: 0, errors };
    }

    try {
      if (useTransaction) {
        await this.knex.transaction(async (trx) => {
          const subBatchSize = this.getSubBatchSize();
          for (let i = 0; i < rows.length; i += subBatchSize) {
            await trx(fullName).insert(rows.slice(i, i + subBatchSize));
          }
        });
      } else {
        const subBatchSize = this.getSubBatchSize();
        for (let i = 0; i < rows.length; i += subBatchSize) {
          await this.knex(fullName).insert(rows.slice(i, i + subBatchSize));
        }
      }

      return { inserted: rows.length, errors };
    } catch {
      return this.insertRowByRow(fullName, rows, errors);
    }
  }

  /**
   * Actualiza filas ya existentes usando la clave de sincronización.
   */
  private async updatePreparedRows(
    fullName: string,
    keyColumns: string[],
    rows: Record<string, unknown>[],
    columns: ColumnDefinition[],
    useTransaction: boolean
  ): Promise<{ updated: number; errors: Array<{ row: number; message: string }> }> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const errors: Array<{ row: number; message: string }> = [];
    if (rows.length === 0) {
      return { updated: 0, errors };
    }

    const payloadColumns = columns
      .map((column) => column.sqlName)
      .filter((columnName) => !keyColumns.includes(columnName));

    const executeUpdates = async (executor: KnexType | KnexType.Transaction) => {
      let updated = 0;

      for (let index = 0; index < rows.length; index++) {
        const row = rows[index];

        try {
          const payload: Record<string, unknown> = {};
          for (const column of payloadColumns) {
            payload[column] = row[column];
          }
          payload._migrated_at = this.knex!.fn.now();

          let query = executor(fullName) as any;
          for (const keyColumn of keyColumns) {
            query = query.where(String(keyColumn), row[keyColumn] as any);
          }

          await query.update(payload);
          updated++;
        } catch (err: unknown) {
          errors.push({
            row: index,
            message: err instanceof Error ? err.message : 'Error actualizando fila',
          });
        }
      }

      return updated;
    };

    const updated = useTransaction
      ? await this.knex.transaction(async (trx) => executeUpdates(trx))
      : await executeUpdates(this.knex);

    return { updated, errors };
  }

  /**
   * Elimina filas por su clave de sincronización.
   */
  private async deleteRowsByKeys(
    fullName: string,
    keyColumns: string[],
    tuples: unknown[][]
  ): Promise<void> {
    if (!this.knex || keyColumns.length === 0 || tuples.length === 0) {
      return;
    }

    const chunkSize = keyColumns.length === 1 ? 500 : 200;
    for (let index = 0; index < tuples.length; index += chunkSize) {
      const chunk = tuples.slice(index, index + chunkSize);
      let query = this.knex(fullName) as any;

      if (keyColumns.length === 1) {
        await query.whereIn(String(keyColumns[0]), chunk.map((tuple) => tuple[0]) as unknown[]).delete();
      } else {
        await query.whereIn(keyColumns as readonly string[], chunk as ReadonlyArray<readonly unknown[]>).delete();
      }
    }
  }

  /**
   * Cuenta filas existentes en una tabla.
   */
  private async getTableRowCount(fullName: string): Promise<number> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const row = await this.knex(fullName).count({ total: '*' }).first() as Record<string, unknown> | undefined;
    const rawValue = row ? (row.total ?? Object.values(row)[0]) : 0;
    const total = typeof rawValue === 'bigint' ? Number(rawValue) : Number(rawValue || 0);
    return Number.isFinite(total) ? total : 0;
  }

  /**
   * Construye tuplas únicas para consultar o eliminar por clave.
   */
  private buildUniqueKeyTuples(rows: Record<string, unknown>[], keyColumns: string[]): unknown[][] {
    const uniqueTuples = new Map<string, unknown[]>();

    for (const row of rows) {
      const key = this.buildCompositeKey(row, keyColumns);
      if (!key) continue;
      uniqueTuples.set(key, keyColumns.map((column) => row[column]));
    }

    return [...uniqueTuples.values()];
  }

  /**
   * Determina si una fila cambió respecto a la fila ya existente en SQL.
   */
  private rowsAreEquivalent(
    incoming: Record<string, unknown>,
    existing: Record<string, unknown>,
    columns: ColumnDefinition[]
  ): boolean {
    return columns.every((column) =>
      this.normalizeComparableValue(incoming[column.sqlName])
        === this.normalizeComparableValue(existing[column.sqlName])
    );
  }

  /**
   * Construye una clave compuesta estable a partir de una fila.
   */
  private buildCompositeKey(row: Record<string, unknown>, keyColumns: string[]): string | null {
    const values: string[] = [];

    for (const column of keyColumns) {
      const normalized = this.normalizeComparableValue(row[column]);
      if (!normalized) {
        return null;
      }
      values.push(normalized);
    }

    return values.join('||');
  }

  /**
   * Normaliza un valor para compararlo o usarlo como clave.
   */
  private normalizeComparableValue(value: unknown): string {
    if (value === null || value === undefined || value === '') return '';
    const normalized = this.normalizeValue(value);
    return String(normalized).trim();
  }

  /**
   * Indica si una columna parece una clave de negocio o identificador.
   */
  private isIdentityColumn(column: ColumnDefinition): boolean {
    return IDENTITY_COLUMN_PATTERNS.some((pattern) =>
      pattern.test(column.originalName) || pattern.test(column.sqlName)
    );
  }

  /**
   * Elige la mejor columna disponible para sincronizar.
   */
  private pickBestSyncColumn(columns: ColumnDefinition[]): ColumnDefinition {
    return [...columns].sort((left, right) => {
      const leftScore = (this.isIdentityColumn(left) ? 10 : 0) + (left.isPrimaryKeyCandidate ? 5 : 0) + (left.nullable ? 0 : 2);
      const rightScore = (this.isIdentityColumn(right) ? 10 : 0) + (right.isPrimaryKeyCandidate ? 5 : 0) + (right.nullable ? 0 : 2);
      return rightScore - leftScore;
    })[0];
  }

  /**
   * Ayuda a clasificar columnas de texto.
   */
  private isTextColumn(column: ColumnDefinition): boolean {
    return column.dataType === 'VARCHAR' || column.dataType === 'TEXT' || column.dataType === 'JSON';
  }

  /**
   * Ayuda a clasificar columnas numéricas.
   */
  private isNumericColumn(column: ColumnDefinition): boolean {
    return ['INTEGER', 'BIGINT', 'DECIMAL', 'FLOAT'].includes(column.dataType);
  }

  /**
   * Quote identifier según dialecto
   */
  private quoteIdentifier(name: string): string {
    switch (this.dialect) {
      case 'mysql':
        return `\`${name}\``;
      case 'mssql':
        return `[${name}]`;
      case 'postgresql':
      default:
        return `"${name}"`;
    }
  }

  /**
   * Sintaxis de auto-increment
   */
  private getAutoIncrementSyntax(): string {
    switch (this.dialect) {
      case 'mysql':
        return 'INT AUTO_INCREMENT PRIMARY KEY';
      case 'postgresql':
        return 'SERIAL PRIMARY KEY';
      case 'mssql':
        return 'INT IDENTITY(1,1) PRIMARY KEY';
      default:
        return 'INTEGER PRIMARY KEY AUTOINCREMENT';
    }
  }

  /**
   * Timestamp actual según dialecto
   */
  private getCurrentTimestampSyntax(): string {
    switch (this.dialect) {
      case 'mysql':
        return 'CURRENT_TIMESTAMP';
      case 'postgresql':
        return 'NOW()';
      case 'mssql':
        return 'GETDATE()';
      default:
        return 'CURRENT_TIMESTAMP';
    }
  }

  /**
   * Obtiene el nombre de la base de datos configurada en la conexión activa.
   */
  private getCurrentDatabaseName(): string {
    if (!this.knex) return '';

    const client = this.knex.client as unknown as {
      config?: { connection?: { database?: string } };
      connectionSettings?: { database?: string };
    };

    return client.connectionSettings?.database
      || client.config?.connection?.database
      || '';
  }

  /**
   * Permite solo una consulta de lectura por ejecución.
   */
  private sanitizeReadOnlyQuery(query: string): string {
    const trimmed = query.trim();
    if (!trimmed) {
      throw new Error('La consulta SQL está vacía.');
    }

    const withoutTrailingSemicolon = trimmed.replace(/;\s*$/, '');
    if (withoutTrailingSemicolon.includes(';')) {
      throw new Error('Solo se permite ejecutar una consulta a la vez.');
    }

    if (!/^(select|show|describe|desc|explain|with)\b/i.test(withoutTrailingSemicolon)) {
      throw new Error('Solo se permiten consultas de lectura: SELECT, SHOW, DESCRIBE, EXPLAIN o WITH.');
    }

    return withoutTrailingSemicolon;
  }

  /**
   * Normaliza el resultado crudo de Knex a una lista simple de filas.
   */
  private normalizeQueryRows(rawResult: unknown): Record<string, unknown>[] {
    if (Array.isArray(rawResult)) {
      if (rawResult.length === 0) return [];

      if (Array.isArray(rawResult[0])) {
        return (rawResult[0] as Record<string, unknown>[]).map((row) => this.normalizeRow(row));
      }

      if (this.isRowObject(rawResult[0])) {
        return (rawResult as Record<string, unknown>[]).map((row) => this.normalizeRow(row));
      }
    }

    if (this.isRowObject(rawResult)) {
      const rowsContainer = rawResult as {
        rows?: Record<string, unknown>[];
        recordset?: Record<string, unknown>[];
      };

      if (Array.isArray(rowsContainer.rows)) {
        return rowsContainer.rows.map((row) => this.normalizeRow(row));
      }

      if (Array.isArray(rowsContainer.recordset)) {
        return rowsContainer.recordset.map((row) => this.normalizeRow(row));
      }
    }

    return [];
  }

  /**
   * Extrae nombres de columnas desde las filas o metadatos del driver.
   */
  private extractColumns(
    rawResult: unknown,
    rows: Record<string, unknown>[]
  ): string[] {
    if (rows.length > 0) {
      const columns = new Set<string>();
      for (const row of rows) {
        for (const column of Object.keys(row)) {
          columns.add(column);
        }
      }
      return [...columns];
    }

    if (Array.isArray(rawResult) && rawResult.length > 1 && Array.isArray(rawResult[1])) {
      return (rawResult[1] as Array<{ name?: string }>)
        .map((field) => String(field.name || ''))
        .filter(Boolean);
    }

    if (this.isRowObject(rawResult)) {
      const meta = rawResult as { fields?: Array<{ name?: string }> };
      if (Array.isArray(meta.fields)) {
        return meta.fields
          .map((field) => String(field.name || ''))
          .filter(Boolean);
      }
    }

    return [];
  }

  /**
   * Convierte una fila a valores seguros para IPC y visualización.
   */
  private normalizeRow(row: Record<string, unknown>): Record<string, unknown> {
    const normalized: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(row)) {
      normalized[key] = this.normalizeValue(value);
    }

    return normalized;
  }

  /**
   * Normaliza valores especiales devueltos por el driver.
   */
  private normalizeValue(value: unknown): unknown {
    if (value === null || value === undefined) return null;
    if (value instanceof Date) return value.toISOString();
    if (typeof value === 'bigint') return value.toString();
    if (Buffer.isBuffer(value)) return value.toString('hex');
    if (Array.isArray(value)) return JSON.stringify(value);

    if (typeof value === 'object') {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }

    return value;
  }

  /**
   * Verifica si el valor es una fila tipo objeto.
   */
  private isRowObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }

  /**
   * Construye configuración de Knex
   */
  private buildKnexConfig(config: DatabaseConnection): KnexType.Config {
    const base: KnexType.Config = {
      pool: {
        min: 2,
        max: 10,
        acquireTimeoutMillis: config.connectionTimeout || 30000,
      },
    };

    switch (config.dialect) {
      case 'mysql':
        return {
          ...base,
          client: 'mysql2',
          connection: {
            host: config.host,
            port: config.port || 3306,
            user: config.user,
            password: config.password,
            database: config.database,
            ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
            connectTimeout: config.connectionTimeout || 30000,
          },
        };

      case 'postgresql':
        return {
          ...base,
          client: 'pg',
          connection: {
            host: config.host,
            port: config.port || 5432,
            user: config.user,
            password: config.password,
            database: config.database,
            ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
            connectionTimeoutMillis: config.connectionTimeout || 30000,
          },
        };

      case 'mssql':
        return {
          ...base,
          client: 'tedious',
          connection: {
            server: config.host,
            port: config.port || 1433,
            userName: config.user,
            password: config.password,
            database: config.database,
            options: {
              encrypt: config.ssl !== false,
              instanceName: config.instanceName,
              trustServerCertificate: true,
              requestTimeout: config.connectionTimeout || 30000,
            } as any,
          },
        };

      default:
        throw new Error(`Dialecto no soportado: ${config.dialect}`);
    }
  }

  /**
   * Crear tabla de log de migración
   */
  async createMigrationLog(): Promise<void> {
    if (!this.knex) throw new Error('No hay conexión activa');

    const exists = await this.knex.schema.hasTable('_exsql_migration_log');
    if (!exists) {
      await this.knex.schema.createTable('_exsql_migration_log', (builder) => {
        builder.increments('id').primary();
        builder.string('source_file', 500);
        builder.string('table_name', 200);
        builder.integer('rows_migrated');
        builder.integer('errors_count');
        builder.text('error_details');
        builder.timestamp('started_at');
        builder.timestamp('completed_at');
        builder.string('status', 50);
        builder.string('dialect', 50);
      });
    }
  }

  /**
   * Registrar resultado de migración
   */
  async logMigration(
    sourceFileName: string,
    tableName: string,
    rowsMigrated: number,
    errorsCount: number,
    errorDetails: string,
    startedAt: Date,
    status: string
  ): Promise<void> {
    if (!this.knex) return;

    try {
      await this.knex('_exsql_migration_log').insert({
        source_file: sourceFileName,
        table_name: tableName,
        rows_migrated: rowsMigrated,
        errors_count: errorsCount,
        error_details: errorDetails.substring(0, 5000),
        started_at: startedAt,
        completed_at: new Date(),
        status,
        dialect: this.dialect,
      });
    } catch {
      // Log no es crítico
    }
  }

  /**
   * Cierra la conexión
   */
  async disconnect(): Promise<void> {
    if (this.knex) {
      await this.knex.destroy();
      this.knex = null;
    }
  }

  /**
   * Retorna si hay conexión activa
   */
  isConnected(): boolean {
    return this.knex !== null;
  }
}
