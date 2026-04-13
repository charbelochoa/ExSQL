// ============================================================
// ExSQL Migrator - Motor de inferencia automática de tipos
// ============================================================
// Analiza columnas de datos y determina el tipo SQL óptimo.
// Muestrea los datos para eficiencia en datasets grandes.

import { ColumnDefinition, SqlDataType } from '../shared/types';

/** Tamaño máximo de muestra para análisis */
const MAX_SAMPLE_SIZE = 10000;
/** Umbral para considerar columna como TEXT en vez de VARCHAR */
const TEXT_THRESHOLD = 1000;
/** Muestras para preview */
const SAMPLE_PREVIEW_COUNT = 5;

// Patrones de fechas comunes en Excel/datos mexicanos y universales
const DATE_PATTERNS = [
  /^\d{4}-\d{2}-\d{2}$/,                          // 2024-01-15
  /^\d{2}\/\d{2}\/\d{4}$/,                         // 15/01/2024 o 01/15/2024
  /^\d{2}-\d{2}-\d{4}$/,                           // 15-01-2024
  /^\d{1,2}\s+(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\s+\d{4}$/i,
  /^\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{4}$/i,
];

const DATETIME_PATTERNS = [
  /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$/,   // 2024-01-15 14:30:00
  /^\d{2}\/\d{2}\/\d{4}\s+\d{2}:\d{2}(:\d{2})?$/,  // 15/01/2024 14:30
];

const TIME_PATTERNS = [
  /^\d{2}:\d{2}(:\d{2})?$/,                         // 14:30 o 14:30:00
];

const BOOLEAN_VALUES = new Set([
  'true', 'false', 'yes', 'no', 'si', 'sí', 'no',
  '1', '0', 'verdadero', 'falso', 'v', 'f', 'y', 'n',
  'activo', 'inactivo', 'active', 'inactive',
]);

export class TypeInferenceEngine {

  /**
   * Analiza una columna completa y retorna su definición
   */
  analyzeColumn(headerName: string, values: unknown[]): ColumnDefinition {
    const sqlName = this.sanitizeColumnName(headerName);

    // Filtrar nulos y preparar muestra
    const nonNullValues: unknown[] = [];
    let nullCount = 0;

    for (const v of values) {
      if (v === null || v === undefined || v === '') {
        nullCount++;
      } else {
        nonNullValues.push(v);
      }
    }

    const totalRows = values.length;
    const nullable = nullCount > 0;
    const nullPercentage = totalRows > 0 ? (nullCount / totalRows) * 100 : 0;

    // Si todos son nulos, default a VARCHAR
    if (nonNullValues.length === 0) {
      return {
        originalName: headerName,
        sqlName,
        dataType: 'VARCHAR',
        maxLength: 255,
        nullable: true,
        isPrimaryKeyCandidate: false,
        nullPercentage: 100,
        uniqueCount: 0,
        totalRows,
        sampleValues: [],
      };
    }

    // Tomar muestra si hay demasiados valores
    const sample = nonNullValues.length > MAX_SAMPLE_SIZE
      ? this.takeSample(nonNullValues, MAX_SAMPLE_SIZE)
      : nonNullValues;

    // Contar únicos (en la muestra)
    const uniqueSet = new Set(sample.map(v => String(v)));
    const uniqueCount = uniqueSet.size;

    // Determinar si es PK candidata
    const isPrimaryKeyCandidate = !nullable && uniqueCount === sample.length;

    // Inferir tipo
    const { dataType, maxLength, precision, scale } = this.inferType(sample);

    // Muestras para preview
    const sampleValues = nonNullValues.slice(0, SAMPLE_PREVIEW_COUNT);

    return {
      originalName: headerName,
      sqlName,
      dataType,
      maxLength,
      precision,
      scale,
      nullable,
      isPrimaryKeyCandidate,
      nullPercentage: Math.round(nullPercentage * 100) / 100,
      uniqueCount,
      totalRows,
      sampleValues,
    };
  }

  /**
   * Infiere el tipo SQL óptimo para un conjunto de valores
   */
  private inferType(values: unknown[]): {
    dataType: SqlDataType;
    maxLength: number;
    precision?: number;
    scale?: number;
  } {
    // Contadores por tipo candidato
    let intCount = 0;
    let bigintCount = 0;
    let floatCount = 0;
    let decimalCount = 0;
    let boolCount = 0;
    let dateCount = 0;
    let datetimeCount = 0;
    let timeCount = 0;
    let jsonCount = 0;
    let maxStrLen = 0;
    let maxPrecision = 0;
    let maxScale = 0;

    for (const val of values) {
      const str = String(val).trim();
      maxStrLen = Math.max(maxStrLen, str.length);

      // Verificar booleano
      if (BOOLEAN_VALUES.has(str.toLowerCase())) {
        boolCount++;
        continue;
      }

      // Verificar fecha/hora (Date objects de Excel)
      if (val instanceof Date) {
        const hasTime = val.getHours() !== 0 || val.getMinutes() !== 0 || val.getSeconds() !== 0;
        if (hasTime) {
          datetimeCount++;
        } else {
          dateCount++;
        }
        continue;
      }

      // Verificar numérico
      if (typeof val === 'number') {
        if (Number.isInteger(val)) {
          if (val >= -2147483648 && val <= 2147483647) {
            intCount++;
          } else {
            bigintCount++;
          }
        } else {
          // Decimal/Float
          const parts = String(val).split('.');
          const intPart = parts[0].replace('-', '').length;
          const decPart = parts[1]?.length || 0;
          maxPrecision = Math.max(maxPrecision, intPart + decPart);
          maxScale = Math.max(maxScale, decPart);

          if (decPart <= 4 && maxPrecision <= 18) {
            decimalCount++;
          } else {
            floatCount++;
          }
        }
        continue;
      }

      // Verificar string numérico
      if (typeof val === 'string') {
        const trimmed = str;

        // ¿Es JSON?
        if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
            (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
          try {
            JSON.parse(trimmed);
            jsonCount++;
            continue;
          } catch { /* no es JSON válido */ }
        }

        // ¿Es fecha string?
        if (DATETIME_PATTERNS.some(p => p.test(trimmed))) {
          datetimeCount++;
          continue;
        }
        if (DATE_PATTERNS.some(p => p.test(trimmed))) {
          dateCount++;
          continue;
        }
        if (TIME_PATTERNS.some(p => p.test(trimmed))) {
          timeCount++;
          continue;
        }

        // ¿Es número como string? (ej: "12345", "3.14", "-99.9")
        const numMatch = trimmed.match(/^-?\d{1,18}(\.\d+)?$/);
        if (numMatch) {
          const num = parseFloat(trimmed);
          if (!isNaN(num)) {
            if (numMatch[1]) { // tiene decimales
              const parts = trimmed.split('.');
              const intPart = parts[0].replace('-', '').length;
              const decPart = parts[1].length;
              maxPrecision = Math.max(maxPrecision, intPart + decPart);
              maxScale = Math.max(maxScale, decPart);
              decimalCount++;
            } else {
              if (num >= -2147483648 && num <= 2147483647) {
                intCount++;
              } else {
                bigintCount++;
              }
            }
            continue;
          }
        }

        // ¿Booleano como string?
        if (BOOLEAN_VALUES.has(trimmed.toLowerCase())) {
          boolCount++;
          continue;
        }
      }

      // Si nada más, es string
      maxStrLen = Math.max(maxStrLen, str.length);
    }

    const total = values.length;
    const threshold = 0.95; // 95% deben coincidir para asignar el tipo

    // Prioridad de tipos (del más específico al más general)
    if (boolCount / total >= threshold) {
      return { dataType: 'BOOLEAN', maxLength: 5 };
    }

    if ((intCount + bigintCount) / total >= threshold) {
      if (bigintCount > 0) {
        return { dataType: 'BIGINT', maxLength: 20 };
      }
      return { dataType: 'INTEGER', maxLength: 11 };
    }

    if (decimalCount / total >= threshold ||
        (intCount + decimalCount) / total >= threshold) {
      return {
        dataType: 'DECIMAL',
        maxLength: maxPrecision + 2,
        precision: Math.min(maxPrecision + 2, 38), // margen
        scale: Math.min(maxScale + 1, 10),
      };
    }

    if (floatCount / total >= threshold ||
        (intCount + floatCount + decimalCount) / total >= threshold) {
      return { dataType: 'FLOAT', maxLength: 24 };
    }

    if (timeCount / total >= threshold) {
      return { dataType: 'TIME', maxLength: 8 };
    }

    if ((dateCount + datetimeCount) / total >= threshold) {
      if (datetimeCount > dateCount) {
        return { dataType: 'DATETIME', maxLength: 26 };
      }
      return { dataType: 'DATE', maxLength: 10 };
    }

    if (jsonCount / total >= threshold) {
      return { dataType: 'JSON', maxLength: maxStrLen };
    }

    // Default: VARCHAR o TEXT
    if (maxStrLen > TEXT_THRESHOLD) {
      return { dataType: 'TEXT', maxLength: maxStrLen };
    }

    // VARCHAR con margen del 50% redondeado
    const varcharLen = Math.min(
      Math.max(Math.ceil(maxStrLen * 1.5 / 50) * 50, 50),
      8000
    );
    return { dataType: 'VARCHAR', maxLength: varcharLen };
  }

  /**
   * Toma una muestra aleatoria representativa
   */
  private takeSample(values: unknown[], size: number): unknown[] {
    if (values.length <= size) return values;

    // Tomar del inicio, medio y fin para buena representación
    const third = Math.floor(size / 3);
    const sample: unknown[] = [
      ...values.slice(0, third),
      ...values.slice(Math.floor(values.length / 2) - Math.floor(third / 2), Math.floor(values.length / 2) + Math.ceil(third / 2)),
      ...values.slice(-third),
    ];

    // Completar con aleatorios si falta
    while (sample.length < size) {
      const idx = Math.floor(Math.random() * values.length);
      sample.push(values[idx]);
    }

    return sample.slice(0, size);
  }

  /**
   * Sanitiza un nombre de columna para SQL
   */
  private sanitizeColumnName(name: string): string {
    let clean = name
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')   // Quitar acentos
      .replace(/[^a-zA-Z0-9_\s]/g, '')   // Solo alfanuméricos
      .trim()
      .replace(/\s+/g, '_')              // Espacios a underscore
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '')
      .toLowerCase()
      .substring(0, 64);

    // Si empieza con número, agregar prefijo
    if (/^\d/.test(clean)) {
      clean = `col_${clean}`;
    }

    // Verificar que no sea palabra reservada SQL
    if (SQL_RESERVED_WORDS.has(clean.toUpperCase())) {
      clean = `${clean}_col`;
    }

    return clean || 'unnamed_column';
  }
}

/** Palabras reservadas SQL comunes */
const SQL_RESERVED_WORDS = new Set([
  'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'JOIN', 'LEFT',
  'RIGHT', 'INNER', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL',
  'CREATE', 'TABLE', 'DROP', 'ALTER', 'INDEX', 'VIEW', 'DATABASE', 'SCHEMA',
  'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
  'CONSTRAINT', 'CASCADE', 'SET', 'VALUES', 'INTO', 'AS', 'ORDER', 'BY',
  'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'CASE',
  'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'EXISTS', 'BETWEEN', 'LIKE', 'ASC',
  'DESC', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DATE', 'TIME', 'TIMESTAMP',
  'INT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'VARCHAR', 'TEXT',
  'CHAR', 'BOOLEAN', 'BOOL', 'TRUE', 'FALSE', 'USER', 'ROLE', 'GRANT',
  'REVOKE', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'BEGIN', 'TRIGGER',
  'PROCEDURE', 'FUNCTION', 'RETURN', 'DECLARE', 'CURSOR', 'FETCH', 'OPEN',
  'CLOSE', 'STATUS', 'NAME', 'TYPE', 'VALUE', 'LEVEL', 'OPTION', 'YEAR',
  'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
]);
