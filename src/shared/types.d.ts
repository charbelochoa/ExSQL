/** Motores SQL soportados */
export type SqlDialect = 'mysql' | 'postgresql' | 'mssql';
/** Tipos de datos SQL que el sistema puede inferir */
export type SqlDataType = 'INTEGER' | 'BIGINT' | 'DECIMAL' | 'FLOAT' | 'BOOLEAN' | 'DATE' | 'DATETIME' | 'TIME' | 'VARCHAR' | 'TEXT' | 'JSON';
/** Definición de una columna detectada */
export interface ColumnDefinition {
    /** Nombre original en Excel */
    originalName: string;
    /** Nombre sanitizado para SQL */
    sqlName: string;
    /** Tipo de dato inferido */
    dataType: SqlDataType;
    /** Longitud máxima (para VARCHAR) */
    maxLength: number;
    /** Precisión decimal (para DECIMAL) */
    precision?: number;
    /** Escala decimal (para DECIMAL) */
    scale?: number;
    /** Si permite nulos */
    nullable: boolean;
    /** Si es clave primaria candidata (valores únicos) */
    isPrimaryKeyCandidate: boolean;
    /** Porcentaje de valores nulos */
    nullPercentage: number;
    /** Cantidad de valores únicos */
    uniqueCount: number;
    /** Total de filas analizadas */
    totalRows: number;
    /** Muestras de valores para preview */
    sampleValues: unknown[];
}
/** Una tabla detectada dentro de una hoja de Excel */
export interface DetectedTable {
    /** ID único */
    id: string;
    /** Nombre sugerido para la tabla SQL */
    tableName: string;
    /** Nombre de la hoja de origen */
    sheetName: string;
    /** Fila donde inicia la tabla (0-indexed) */
    startRow: number;
    /** Columna donde inicia la tabla (0-indexed) */
    startCol: number;
    /** Fila donde termina */
    endRow: number;
    /** Columna donde termina */
    endCol: number;
    /** Columnas detectadas */
    columns: ColumnDefinition[];
    /** Total de filas de datos (sin encabezado) */
    rowCount: number;
    /** Preview de las primeras filas */
    previewRows: Record<string, unknown>[];
}
/** Configuración de conexión a la base de datos */
export interface DatabaseConnection {
    dialect: SqlDialect;
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
    /** Para SQL Server: nombre de la instancia */
    instanceName?: string;
    /** SSL/TLS habilitado */
    ssl?: boolean;
    /** Timeout de conexión en ms */
    connectionTimeout?: number;
}
/** Estado de progreso de la migración */
export interface MigrationProgress {
    phase: 'parsing' | 'analyzing' | 'creating_tables' | 'inserting_data' | 'creating_indexes' | 'done' | 'error';
    currentTable: string;
    tablesTotal: number;
    tablesCompleted: number;
    rowsTotal: number;
    rowsInserted: number;
    currentBatch: number;
    totalBatches: number;
    elapsedMs: number;
    estimatedRemainingMs: number;
    errors: MigrationError[];
    warnings: string[];
}
/** Error durante la migración */
export interface MigrationError {
    table: string;
    row?: number;
    column?: string;
    message: string;
    originalValue?: unknown;
    timestamp: Date;
}
/** Opciones de migración */
export interface MigrationOptions {
    /** Tamaño del batch para inserts */
    batchSize: number;
    /** Crear índices en columnas de clave primaria */
    createIndexes: boolean;
    /** Si la tabla existe: 'drop', 'truncate', 'append', 'skip' */
    ifTableExists: 'drop' | 'truncate' | 'append' | 'skip';
    /** Usar transacciones por batch */
    useTransactions: boolean;
    /** Prefijo para nombres de tablas */
    tablePrefix: string;
    /** Encoding del archivo Excel */
    encoding: string;
    /** Máximo de errores antes de abortar */
    maxErrors: number;
    /** Crear tabla de log de migración */
    createMigrationLog: boolean;
}
/** Resultado final de la migración */
export interface MigrationResult {
    success: boolean;
    tablesCreated: number;
    totalRowsInserted: number;
    totalErrors: number;
    duration: number;
    tables: {
        name: string;
        rowsInserted: number;
        columnsCreated: number;
        errors: MigrationError[];
    }[];
    migrationLogId?: string;
}
/** Mensajes IPC entre main y renderer */
export type IpcChannel = 'select-file' | 'file-selected' | 'parse-excel' | 'parse-progress' | 'parse-result' | 'test-connection' | 'connection-result' | 'start-migration' | 'migration-progress' | 'migration-complete' | 'migration-error' | 'cancel-migration' | 'get-default-ports';
//# sourceMappingURL=types.d.ts.map