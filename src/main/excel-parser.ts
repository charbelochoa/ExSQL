// ============================================================
// ExSQL Migrator - Parser de Excel con detección de tablas múltiples
// ============================================================
// Usa ExcelJS en modo streaming para archivos grandes (100k+ filas)
// Detecta automáticamente múltiples tablas por hoja separadas por filas vacías

import ExcelJS from 'exceljs';
import { stat } from 'fs/promises';
import path from 'path';
import * as XLSX from 'xlsx';
import { DetectedTable, ColumnDefinition } from '../shared/types';
import { TypeInferenceEngine } from './type-inference';

/** Cantidad de filas para preview */
const PREVIEW_ROW_COUNT = 10;
/** Mínimo de columnas para considerar como tabla */
const MIN_COLUMNS = 1;
/** Filas vacías consecutivas para separar tablas */
const EMPTY_ROW_THRESHOLD = 2;

interface RawTableRegion {
  sheetName: string;
  startRow: number;
  startCol: number;
  endRow: number;
  endCol: number;
  headers: string[];
  rows: unknown[][];
}

export class ExcelParser {
  private typeEngine: TypeInferenceEngine;

  constructor() {
    this.typeEngine = new TypeInferenceEngine();
  }

  /**
   * Parsea un archivo Excel completo y detecta todas las tablas.
   * Usa streaming para archivos grandes.
   */
  async parseFile(
    filePath: string,
    onProgress?: (msg: string, percent: number) => void
  ): Promise<DetectedTable[]> {
    const ext = path.extname(filePath).toLowerCase();
    if (ext !== '.xlsx' && ext !== '.xls' && ext !== '.xlsm') {
      throw new Error(`Formato no soportado: ${ext}. Use .xlsx, .xls o .xlsm`);
    }

    onProgress?.('Abriendo archivo...', 0);

    let tables: DetectedTable[];
    if (ext === '.xls') {
      tables = await this.parseWithSheetJs(filePath, onProgress);
    } else {
      // Para archivos muy grandes, usamos streaming
      const fileStats = await this.getFileSize(filePath);
      const useStreaming = fileStats > 50 * 1024 * 1024; // >50MB

      try {
        if (useStreaming) {
          tables = await this.parseWithStreaming(filePath, onProgress);
        } else {
          tables = await this.parseWithFullLoad(filePath, onProgress);
        }
      } catch (error: unknown) {
        if (!this.shouldUseSheetJsFallback(error)) {
          throw error;
        }

        onProgress?.('Reintentando con parser alternativo...', 5);
        tables = await this.parseWithSheetJs(filePath, onProgress);
      }
    }

    onProgress?.('Análisis completado', 100);
    return tables;
  }

  /**
   * Parseo completo en memoria (para archivos < 50MB)
   */
  private async parseWithFullLoad(
    filePath: string,
    onProgress?: (msg: string, percent: number) => void
  ): Promise<DetectedTable[]> {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(filePath);

    const allTables: DetectedTable[] = [];
    const tableNameRegistry = new Map<string, number>();
    const totalSheets = workbook.worksheets.length;

    for (let i = 0; i < totalSheets; i++) {
      const worksheet = workbook.worksheets[i];
      const sheetName = worksheet.name;
      const pct = Math.round(((i + 1) / totalSheets) * 80);
      onProgress?.(`Analizando hoja: ${sheetName}`, pct);

      // Extraer todas las filas como arrays
      const rawRows: unknown[][] = [];
      worksheet.eachRow({ includeEmpty: true }, (row, rowNumber) => {
        const values: unknown[] = [];
        row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
          values[colNumber - 1] = this.extractCellValue(cell);
        });
        rawRows[rowNumber - 1] = values;
      });

      // Detectar regiones de tablas
      const regions = this.detectTableRegions(rawRows, sheetName);

      // Convertir regiones a DetectedTable con análisis de tipos
      for (const region of regions) {
        const table = this.analyzeRegion(region, filePath, tableNameRegistry);
        if (table) {
          allTables.push(table);
        }
      }
    }

    onProgress?.('Finalizando análisis de tipos...', 90);
    return allTables;
  }

  /**
   * Parseo streaming para archivos grandes (>50MB)
   */
  private async parseWithStreaming(
    filePath: string,
    onProgress?: (msg: string, percent: number) => void
  ): Promise<DetectedTable[]> {
    const allTables: DetectedTable[] = [];
    const tableNameRegistry = new Map<string, number>();

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
      worksheets: 'emit',
      sharedStrings: 'cache',
      hyperlinks: 'ignore',
      styles: 'ignore',
    });

    let sheetIndex = 0;

    for await (const worksheetReader of workbook) {
      const sheetName = (worksheetReader as any).name || `Sheet${sheetIndex + 1}`;
      onProgress?.(`Leyendo hoja (streaming): ${sheetName}`, 10 + sheetIndex * 5);

      const rawRows: unknown[][] = [];
      let rowIndex = 0;

      for await (const row of worksheetReader) {
        const values: unknown[] = [];
        if (row && typeof row === 'object' && 'eachCell' in row) {
          (row as ExcelJS.Row).eachCell({ includeEmpty: true }, (cell, colNumber) => {
            values[colNumber - 1] = this.extractCellValue(cell);
          });
        }
        rawRows[rowIndex] = values;
        rowIndex++;

        // Reportar progreso cada 10k filas
        if (rowIndex % 10000 === 0) {
          onProgress?.(`Leyendo ${sheetName}: ${rowIndex.toLocaleString()} filas...`, 20);
        }
      }

      const regions = this.detectTableRegions(rawRows, sheetName);
      for (const region of regions) {
        const table = this.analyzeRegion(region, filePath, tableNameRegistry);
        if (table) {
          allTables.push(table);
        }
      }

      sheetIndex++;
    }

    return allTables;
  }

  /**
   * Parseo alternativo con SheetJS.
   * Se usa para .xls y como fallback cuando ExcelJS falla en algunos archivos.
   */
  private async parseWithSheetJs(
    filePath: string,
    onProgress?: (msg: string, percent: number) => void
  ): Promise<DetectedTable[]> {
    const workbook = XLSX.readFile(filePath, {
      cellDates: true,
      dense: true,
    });

    const allTables: DetectedTable[] = [];
  const tableNameRegistry = new Map<string, number>();
    const totalSheets = workbook.SheetNames.length;

    for (let i = 0; i < totalSheets; i++) {
      const sheetName = workbook.SheetNames[i];
      const sheet = workbook.Sheets[sheetName];
      const pct = Math.round(((i + 1) / Math.max(totalSheets, 1)) * 80);
      onProgress?.(`Analizando hoja: ${sheetName}`, pct);

      const rawRows = this.getRawRowsFromSheetJsSheet(sheet);
      const regions = this.detectTableRegions(rawRows, sheetName);

      for (const region of regions) {
        const table = this.analyzeRegion(region, filePath, tableNameRegistry);
        if (table) {
          allTables.push(table);
        }
      }
    }

    onProgress?.('Finalizando análisis de tipos...', 90);
    return allTables;
  }

  /**
   * Detecta regiones de tablas en una hoja.
   * Busca bloques separados por filas completamente vacías.
   */
  private detectTableRegions(rawRows: unknown[][], sheetName: string): RawTableRegion[] {
    const regions: RawTableRegion[] = [];

    if (rawRows.length === 0) return regions;

    // Encontrar el ancho máximo
    let maxCols = 0;
    for (const row of rawRows) {
      if (row) maxCols = Math.max(maxCols, row.length);
    }

    // Normalizar todas las filas al mismo ancho
    const normalized = rawRows.map(row => {
      const r = row || [];
      const padded: unknown[] = [];
      for (let c = 0; c < maxCols; c++) {
        padded[c] = r[c] ?? null;
      }
      return padded;
    });

    // Detectar bloques verticales (separados por filas vacías)
    let blockStart: number | null = null;
    let emptyCount = 0;
    const blocks: { start: number; end: number }[] = [];

    for (let r = 0; r < normalized.length; r++) {
      const isEmpty = this.isRowEmpty(normalized[r]);

      if (isEmpty) {
        emptyCount++;
        if (emptyCount >= EMPTY_ROW_THRESHOLD && blockStart !== null) {
          blocks.push({ start: blockStart, end: r - emptyCount });
          blockStart = null;
        }
      } else {
        emptyCount = 0;
        if (blockStart === null) {
          blockStart = r;
        }
      }
    }

    // Último bloque
    if (blockStart !== null) {
      blocks.push({ start: blockStart, end: normalized.length - 1 });
    }

    // Para cada bloque vertical, detectar sub-tablas horizontales
    for (const block of blocks) {
      const subTables = this.detectHorizontalTables(
        normalized,
        block.start,
        block.end,
        maxCols,
        sheetName
      );
      regions.push(...subTables);
    }

    return regions;
  }

  /**
   * Dentro de un bloque vertical, detecta si hay tablas lado a lado
   * separadas por columnas vacías.
   */
  private detectHorizontalTables(
    rows: unknown[][],
    startRow: number,
    endRow: number,
    maxCols: number,
    sheetName: string
  ): RawTableRegion[] {
    // Encontrar columnas que están completamente vacías en este rango
    const colHasData: boolean[] = new Array(maxCols).fill(false);

    for (let r = startRow; r <= endRow; r++) {
      for (let c = 0; c < maxCols; c++) {
        if (rows[r][c] !== null && rows[r][c] !== undefined && rows[r][c] !== '') {
          colHasData[c] = true;
        }
      }
    }

    // Encontrar rangos contiguos de columnas con datos
    const colRanges: { start: number; end: number }[] = [];
    let rangeStart: number | null = null;

    for (let c = 0; c < maxCols; c++) {
      if (colHasData[c]) {
        if (rangeStart === null) rangeStart = c;
      } else {
        if (rangeStart !== null) {
          colRanges.push({ start: rangeStart, end: c - 1 });
          rangeStart = null;
        }
      }
    }
    if (rangeStart !== null) {
      colRanges.push({ start: rangeStart, end: maxCols - 1 });
    }

    // Si solo hay un rango de columnas, es una sola tabla
    // Si hay múltiples rangos separados por 2+ columnas vacías, son tablas separadas
    const mergedRanges = this.mergeCloseRanges(colRanges, 1);

    const regions: RawTableRegion[] = [];

    for (const colRange of mergedRanges) {
      const colCount = colRange.end - colRange.start + 1;
      if (colCount < MIN_COLUMNS) continue;

      // Extraer la primera fila no vacía como headers
      const headerRow = rows[startRow];
      const headers: string[] = [];
      for (let c = colRange.start; c <= colRange.end; c++) {
        const val = headerRow[c];
        headers.push(val != null ? String(val).trim() : `Column_${c + 1}`);
      }

      // Verificar que los headers no sean todos numéricos (probablemente datos, no headers)
      const allNumericHeaders = headers.every(h => /^\d+(\.\d+)?$/.test(h));

      let dataStartRow = startRow;
      let effectiveHeaders = headers;

      if (allNumericHeaders) {
        // No hay headers reales, generar nombres
        effectiveHeaders = headers.map((_, i) => `Column_${i + 1}`);
      } else {
        dataStartRow = startRow + 1;
      }

      // Extraer filas de datos
      const dataRows: unknown[][] = [];
      for (let r = dataStartRow; r <= endRow; r++) {
        const row: unknown[] = [];
        for (let c = colRange.start; c <= colRange.end; c++) {
          row.push(rows[r][c]);
        }
        // No incluir filas completamente vacías
        if (!this.isRowEmpty(row)) {
          dataRows.push(row);
        }
      }

      if (dataRows.length === 0) continue;

      regions.push({
        sheetName,
        startRow: startRow,
        startCol: colRange.start,
        endRow: endRow,
        endCol: colRange.end,
        headers: effectiveHeaders,
        rows: dataRows,
      });
    }

    return regions;
  }

  /**
   * Fusiona rangos de columnas que están separados por solo 1 columna vacía
   */
  private mergeCloseRanges(
    ranges: { start: number; end: number }[],
    maxGap: number
  ): { start: number; end: number }[] {
    if (ranges.length <= 1) return ranges;

    const merged: { start: number; end: number }[] = [{ ...ranges[0] }];

    for (let i = 1; i < ranges.length; i++) {
      const last = merged[merged.length - 1];
      const current = ranges[i];

      if (current.start - last.end <= maxGap + 1) {
        last.end = current.end;
      } else {
        merged.push({ ...current });
      }
    }

    return merged;
  }

  /**
   * Analiza una región y genera una DetectedTable con inferencia de tipos
   */
  private analyzeRegion(
    region: RawTableRegion,
    filePath: string,
    tableNameRegistry: Map<string, number>
  ): DetectedTable | null {
    if (region.rows.length === 0 || region.headers.length === 0) return null;

    const columns: ColumnDefinition[] = [];

    for (let colIdx = 0; colIdx < region.headers.length; colIdx++) {
      const colValues = region.rows.map(row => row[colIdx]);
      const analysis = this.typeEngine.analyzeColumn(
        region.headers[colIdx],
        colValues
      );
      columns.push(analysis);
    }

    // Generar preview rows
    const previewRows: Record<string, unknown>[] = [];
    const previewCount = Math.min(PREVIEW_ROW_COUNT, region.rows.length);
    for (let r = 0; r < previewCount; r++) {
      const rowObj: Record<string, unknown> = {};
      for (let c = 0; c < region.headers.length; c++) {
        rowObj[columns[c].sqlName] = region.rows[r][c];
      }
      previewRows.push(rowObj);
    }

    // Generar nombre de tabla
    const tableName = this.buildUniqueTableName(filePath, region.sheetName, tableNameRegistry);

    return {
      id: this.buildTableId(filePath, region),
      tableName,
      sourceFilePath: filePath,
      sourceFileName: path.basename(filePath),
      sheetName: region.sheetName,
      startRow: region.startRow,
      startCol: region.startCol,
      endRow: region.endRow,
      endCol: region.endCol,
      columns,
      rowCount: region.rows.length,
      previewRows,
    };
  }

  /**
   * Genera un nombre de tabla único por archivo y hoja.
   */
  private buildUniqueTableName(
    filePath: string,
    sheetName: string,
    tableNameRegistry: Map<string, number>
  ): string {
    const fileBaseName = this.sanitizeTableName(path.basename(filePath, path.extname(filePath)));
    const sheetBaseName = this.sanitizeTableName(sheetName);

    let baseName = sheetBaseName || fileBaseName || 'table_unnamed';
    if (fileBaseName && sheetBaseName && fileBaseName !== sheetBaseName) {
      baseName = `${fileBaseName}_${sheetBaseName}`;
    }

    const count = (tableNameRegistry.get(baseName) ?? 0) + 1;
    tableNameRegistry.set(baseName, count);

    return count === 1 ? baseName : `${baseName}_${count}`;
  }

  /**
   * Genera un identificador estable para una tabla detectada.
   */
  private buildTableId(filePath: string, region: RawTableRegion): string {
    const fileBaseName = this.sanitizeTableName(path.basename(filePath, path.extname(filePath)));
    const sheetBaseName = this.sanitizeTableName(region.sheetName);
    return `${fileBaseName}_${sheetBaseName}_${region.startRow}_${region.startCol}_${region.endRow}_${region.endCol}`;
  }

  /**
   * Extrae el valor de una celda de ExcelJS manejando todos los tipos
   */
  private extractCellValue(cell: ExcelJS.Cell): unknown {
    if (!cell || cell.value === null || cell.value === undefined) return null;

    const val = cell.value;

    // Manejar fórmulas
    if (typeof val === 'object' && val !== null) {
      if ('result' in val) {
        return (val as { result: unknown }).result;
      }
      if ('richText' in val) {
        const richText = (val as { richText: { text: string }[] }).richText;
        return richText.map(r => r.text).join('');
      }
      if ('text' in val && 'hyperlink' in val) {
        return (val as { text: string }).text;
      }
      if (val instanceof Date) {
        return val;
      }
      // Shared formula
      if ('sharedFormula' in val) {
        return (val as { result?: unknown }).result ?? null;
      }
    }

    return val;
  }

  /**
   * Verifica si una fila está completamente vacía
   */
  private isRowEmpty(row: unknown[]): boolean {
    return row.every(v => v === null || v === undefined || v === '');
  }

  /**
   * Sanitiza un nombre para usarlo como nombre de tabla SQL
   */
  private sanitizeTableName(name: string): string {
    return name
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '') // Eliminar acentos
      .replace(/[^a-zA-Z0-9_]/g, '_')   // Solo alfanuméricos y underscore
      .replace(/_+/g, '_')               // Eliminar underscores múltiples
      .replace(/^_|_$/g, '')             // Eliminar underscores al inicio/fin
      .toLowerCase()
      .substring(0, 64)                  // Límite de longitud
      || 'table_unnamed';
  }

  /**
   * Decide si conviene reintentar con SheetJS ante errores conocidos de ExcelJS.
   */
  private shouldUseSheetJsFallback(error: unknown): boolean {
    if (!(error instanceof Error)) return false;

    const message = error.message.toLowerCase();
    return message.includes('then is not a function')
      || message.includes('end of central directory')
      || message.includes('unsupported zip')
      || message.includes('corrupt');
  }

  /**
   * Convierte una hoja de SheetJS en una matriz de filas/columnas.
   */
  private getRawRowsFromSheetJsSheet(sheet: XLSX.WorkSheet): unknown[][] {
    const rows = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      raw: true,
      defval: null,
      blankrows: true,
    }) as unknown[];

    return rows.map(row => Array.isArray(row) ? row : []);
  }

  /**
   * Obtiene el tamaño del archivo
   */
  private async getFileSize(filePath: string): Promise<number> {
    const stats = await stat(filePath);
    return stats.size;
  }

  /**
   * Obtiene las filas raw de una tabla detectada para la migración.
   * Usa streaming para tablas grandes.
   */
  async getTableRows(
    filePath: string,
    table: DetectedTable,
    onChunk: (rows: Record<string, unknown>[], chunkIndex: number) => Promise<void>,
    chunkSize: number = 5000
  ): Promise<number> {
    const ext = path.extname(filePath).toLowerCase();

    if (ext === '.xls') {
      return this.getTableRowsWithSheetJs(filePath, table, onChunk, chunkSize);
    }

    try {
      return await this.getTableRowsWithExcelJs(filePath, table, onChunk, chunkSize);
    } catch (error: unknown) {
      if (!this.shouldUseSheetJsFallback(error)) {
        throw error;
      }

      return this.getTableRowsWithSheetJs(filePath, table, onChunk, chunkSize);
    }
  }

  /**
   * Lee filas usando ExcelJS.
   */
  private async getTableRowsWithExcelJs(
    filePath: string,
    table: DetectedTable,
    onChunk: (rows: Record<string, unknown>[], chunkIndex: number) => Promise<void>,
    chunkSize: number = 5000
  ): Promise<number> {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(filePath);

    const worksheet = workbook.getWorksheet(table.sheetName);
    if (!worksheet) throw new Error(`Hoja "${table.sheetName}" no encontrada`);

    let buffer: Record<string, unknown>[] = [];
    let totalRows = 0;
    let chunkIndex = 0;

    // Determinar la fila de inicio de datos
    const headersAreNumeric = table.columns.every(c =>
      c.originalName === c.sqlName && /^column_\d+$/.test(c.sqlName)
    );
    const dataStartRow = table.startRow + (headersAreNumeric ? 0 : 1);

    worksheet.eachRow({ includeEmpty: false }, (row, rowNumber) => {
      const adjustedRow = rowNumber - 1; // 0-indexed

      if (adjustedRow < dataStartRow || adjustedRow > table.endRow) return;

      const rowObj: Record<string, unknown> = {};
      let hasData = false;

      for (let i = 0; i < table.columns.length; i++) {
        const colIndex = table.startCol + i + 1; // ExcelJS es 1-indexed
        const cell = row.getCell(colIndex);
        const value = this.extractCellValue(cell);
        rowObj[table.columns[i].sqlName] = value;
        if (value !== null && value !== undefined && value !== '') {
          hasData = true;
        }
      }

      if (hasData) {
        buffer.push(rowObj);
        totalRows++;
      }
    });

    // Procesar el buffer en chunks
    for (let i = 0; i < buffer.length; i += chunkSize) {
      const chunk = buffer.slice(i, i + chunkSize);
      await onChunk(chunk, chunkIndex);
      chunkIndex++;
    }

    return totalRows;
  }

  /**
   * Lee filas usando SheetJS.
   */
  private async getTableRowsWithSheetJs(
    filePath: string,
    table: DetectedTable,
    onChunk: (rows: Record<string, unknown>[], chunkIndex: number) => Promise<void>,
    chunkSize: number = 5000
  ): Promise<number> {
    const workbook = XLSX.readFile(filePath, {
      cellDates: true,
      dense: true,
    });

    const sheet = workbook.Sheets[table.sheetName];
    if (!sheet) throw new Error(`Hoja "${table.sheetName}" no encontrada`);

    const rawRows = this.getRawRowsFromSheetJsSheet(sheet);
    const headersAreNumeric = table.columns.every(c =>
      c.originalName === c.sqlName && /^column_\d+$/.test(c.sqlName)
    );
    const dataStartRow = table.startRow + (headersAreNumeric ? 0 : 1);

    let buffer: Record<string, unknown>[] = [];
    let totalRows = 0;
    let chunkIndex = 0;

    for (let rowIndex = dataStartRow; rowIndex <= table.endRow; rowIndex++) {
      const sourceRow = rawRows[rowIndex] ?? [];
      const rowObj: Record<string, unknown> = {};
      let hasData = false;

      for (let i = 0; i < table.columns.length; i++) {
        const value = sourceRow[table.startCol + i] ?? null;
        rowObj[table.columns[i].sqlName] = value;
        if (value !== null && value !== undefined && value !== '') {
          hasData = true;
        }
      }

      if (!hasData) continue;

      buffer.push(rowObj);
      totalRows++;

      if (buffer.length >= chunkSize) {
        await onChunk(buffer, chunkIndex);
        chunkIndex++;
        buffer = [];
      }
    }

    if (buffer.length > 0) {
      await onChunk(buffer, chunkIndex);
    }

    return totalRows;
  }
}
