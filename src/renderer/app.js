// ============================================================
// ExSQL Migrator - Frontend Application
// ============================================================
// Maneja la UI y comunica con el main process via preload bridge

(() => {
  'use strict';

  // ---- State ----
  let currentStep = 1;
  let selectedDialect = 'mysql';
  let connectionTested = false;
  let tables = [];
  let parsedFiles = [];
  let sqlTables = [];
  let lastQueryResult = null;

  const defaultPorts = { mysql: 3306, postgresql: 5432, mssql: 1433 };
  const defaultUsers = { mysql: 'root', postgresql: 'postgres', mssql: 'sa' };

  // ---- DOM Elements ----
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // Steps
  const stepPanels = $$('.step-panel');
  const stepIndicators = $$('.steps-bar .step');

  // Step 1
  const btnSelectFile = $('#btn-select-file');
  const uploadZone = $('#upload-zone');
  const parseProgress = $('#parse-progress');
  const parseProgressFill = $('#parse-progress-fill');
  const parseProgressText = $('#parse-progress-text');

  // Step 2
  const tablesGrid = $('#tables-grid');
  const fileInfo = $('#file-info');
  const tablesSummary = $('#tables-summary');
  const btnToConnection = $('#btn-to-connection');
  const btnBackToFile = $('#btn-back-to-file');

  // Step 3
  const dialectBtns = $$('.dialect-btn');
  const connHost = $('#conn-host');
  const connPort = $('#conn-port');
  const connDatabase = $('#conn-database');
  const connUser = $('#conn-user');
  const connPassword = $('#conn-password');
  const connInstance = $('#conn-instance');
  const btnTestConnection = $('#btn-test-connection');
  const connectionStatus = $('#connection-status');
  const btnStartMigration = $('#btn-start-migration');
  const btnStartMigrationLabel = $('#btn-start-migration-label');
  const btnPreviewDDL = $('#btn-preview-ddl');
  const btnBackToTables = $('#btn-back-to-tables');

  // Options
  const optBatchSize = $('#opt-batch-size');
  const optIfExists = $('#opt-if-exists');
  const optPrefix = $('#opt-prefix');
  const optIndexes = $('#opt-indexes');
  const optTransactions = $('#opt-transactions');
  const optLog = $('#opt-log');

  // Step 4
  const statTables = $('#stat-tables');
  const statRows = $('#stat-rows');
  const statTime = $('#stat-time');
  const statEta = $('#stat-eta');
  const migrationPhase = $('#migration-phase');
  const migrationPercent = $('#migration-percent');
  const migrationProgressFill = $('#migration-progress-fill');
  const migrationCurrentTable = $('#migration-current-table');
  const migrationLog = $('#migration-log');
  const errorCount = $('#error-count');
  const btnCancelMigration = $('#btn-cancel-migration');
  const btnNewMigration = $('#btn-new-migration');
  const migrationResults = $('#migration-results');
  const resultsCard = $('#results-card');
  const resultsTitle = $('#results-title');
  const resultsSummary = $('#results-summary');
  const resultsDetails = $('#results-details');
  const btnOpenQueryExplorer = $('#btn-open-query-explorer');

  // Modal
  const ddlModal = $('#ddl-modal');
  const ddlPreviewContent = $('#ddl-preview-content');
  const btnCloseModal = $('#btn-close-modal');
  const btnCloseModal2 = $('#btn-close-modal-2');
  const btnCopyDDL = $('#btn-copy-ddl');
  const sqlExplorerModal = $('#sql-explorer-modal');
  const btnCloseQueryModal = $('#btn-close-query-modal');
  const btnCloseQueryModal2 = $('#btn-close-query-modal-2');
  const btnRefreshSqlTables = $('#btn-refresh-sql-tables');
  const sqlTableList = $('#sql-table-list');
  const sqlQueryEditor = $('#sql-query-editor');
  const btnRunQuery = $('#btn-run-query');
  const sqlQueryStatus = $('#sql-query-status');
  const sqlResultSummary = $('#sql-result-summary');
  const sqlResultsTable = $('#sql-results-table');
  const sqlChartType = $('#sql-chart-type');
  const sqlChartX = $('#sql-chart-x');
  const sqlChartY = $('#sql-chart-y');
  const btnRenderChart = $('#btn-render-chart');
  const sqlChartCanvas = $('#sql-chart-canvas');
  const sqlChartEmpty = $('#sql-chart-empty');

  // ---- Navigation ----
  function goToStep(step) {
    currentStep = step;

    stepPanels.forEach((panel) => {
      panel.classList.remove('active');
    });
    $(`#step-${step}`).classList.add('active');

    stepIndicators.forEach((ind) => {
      const s = parseInt(ind.dataset.step);
      ind.classList.remove('active', 'completed');
      if (s === step) ind.classList.add('active');
      else if (s < step) ind.classList.add('completed');
    });
  }

  if (!connUser.value.trim()) {
    connUser.value = defaultUsers[selectedDialect] || '';
  }

  if (optIfExists) {
    optIfExists.addEventListener('change', updateStartActionLabel);
    updateStartActionLabel();
  }

  // ---- Step 1: File Selection ----
  async function handleSelectFile() {
    const filePaths = await window.exsql.selectFile();
    if (!Array.isArray(filePaths) || filePaths.length === 0) return;

    // Show progress
    uploadZone.classList.add('hidden');
    parseProgress.classList.remove('hidden');

    // Listen for parse progress
    const removeListener = window.exsql.onParseProgress((data) => {
      parseProgressFill.style.width = `${data.percent}%`;
      parseProgressText.textContent = data.message;
    });

    try {
      const result = await window.exsql.parseExcel(filePaths);
      removeListener();

      if (result.success) {
        tables = result.tables;
        parsedFiles = result.files || [];
        renderTables(result.tables, parsedFiles);
        goToStep(2);
      } else {
        alert(`Error: ${result.error}`);
        resetStep1();
      }
    } catch (err) {
      removeListener();
      alert(`Error inesperado: ${err.message}`);
      resetStep1();
    }
  }

  btnSelectFile.addEventListener('click', handleSelectFile);

  // Ribbon buttons
  const btnSelectFileRibbon = $('#btn-select-file-ribbon');
  const btnPreviewDDLRibbon = $('#btn-preview-ddl-ribbon');
  const btnTestConnRibbon = $('#btn-test-conn-ribbon');
  const btnOpenQueryRibbon = $('#btn-open-query-ribbon');

  if (btnSelectFileRibbon) btnSelectFileRibbon.addEventListener('click', handleSelectFile);
  if (btnPreviewDDLRibbon) btnPreviewDDLRibbon.addEventListener('click', () => {
    if (btnPreviewDDL) btnPreviewDDL.click();
  });
  if (btnTestConnRibbon) btnTestConnRibbon.addEventListener('click', () => {
    if (btnTestConnection) btnTestConnection.click();
  });
  if (btnOpenQueryRibbon) btnOpenQueryRibbon.addEventListener('click', openSqlExplorer);
  if (btnOpenQueryExplorer) btnOpenQueryExplorer.addEventListener('click', openSqlExplorer);

  function resetStep1() {
    uploadZone.classList.remove('hidden');
    parseProgress.classList.add('hidden');
    parseProgressFill.style.width = '0%';
  }

  // ---- Step 2: Table Review ----
  function renderTables(tables, files) {
    const totalFiles = files.length || 1;
    const totalSize = files.reduce((sum, file) => sum + (file.fileSize || 0), 0);

    if (totalFiles === 1 && files[0]) {
      fileInfo.textContent = `${files[0].fileName} — ${tables.length} tabla${tables.length !== 1 ? 's' : ''} detectada${tables.length !== 1 ? 's' : ''}`;
    } else {
      fileInfo.textContent = `${totalFiles} archivos Excel — ${tables.length} tablas detectadas`;
    }

    const totalRows = tables.reduce((sum, t) => sum + t.rowCount, 0);
    const totalCols = tables.reduce((sum, t) => sum + t.columns.length, 0);
    tablesSummary.textContent = `${totalFiles} archivo${totalFiles !== 1 ? 's' : ''} · ${totalRows.toLocaleString()} filas totales · ${totalCols} columnas · ${formatBytes(totalSize)}`;

    tablesGrid.innerHTML = tables.map((table) => `
      <div class="table-card" data-table-id="${table.id}">
        <div class="table-card-header">
          <h3>${escapeHtml(table.tableName)}</h3>
          <span class="badge">${table.sheetName}</span>
        </div>
        <div class="table-card-meta">
          <span>${escapeHtml(table.sourceFileName)}</span>
          <span>${table.rowCount.toLocaleString()} filas</span>
          <span>${table.columns.length} columnas</span>
          <span>Fila ${table.startRow + 1} → ${table.endRow + 1}</span>
        </div>
        <div class="table-card-columns">
          <div class="col-list">
            ${table.columns.map((col) => `
              <span class="col-tag">
                ${escapeHtml(col.sqlName)}<span class="type">${col.dataType}${col.dataType === 'VARCHAR' ? `(${col.maxLength})` : ''}</span>
              </span>
            `).join('')}
          </div>
        </div>
      </div>
    `).join('');
  }

  btnToConnection.addEventListener('click', () => goToStep(3));
  btnBackToFile.addEventListener('click', () => {
    resetStep1();
    goToStep(1);
  });
  btnBackToTables.addEventListener('click', () => goToStep(2));

  // ---- Step 3: Connection ----
  dialectBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      dialectBtns.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      selectedDialect = btn.dataset.dialect;

      // Update port
      connPort.value = defaultPorts[selectedDialect] || 3306;

      if (!connUser.value.trim()) {
        connUser.value = defaultUsers[selectedDialect] || '';
      }

      // Toggle instance field
      connInstance.disabled = selectedDialect !== 'mssql';

      // Reset connection status
      connectionTested = false;
      connectionStatus.textContent = '';
      connectionStatus.className = 'connection-status';
      btnStartMigration.disabled = true;
    });
  });

  btnTestConnection.addEventListener('click', async () => {
    connectionStatus.textContent = 'Conectando...';
    connectionStatus.className = 'connection-status';
    btnTestConnection.disabled = true;

    const config = getConnectionConfig();
    const validationError = validateConnectionConfig(config);

    if (validationError) {
      connectionStatus.textContent = validationError;
      connectionStatus.className = 'connection-status error';
      connectionTested = false;
      btnStartMigration.disabled = true;
      btnTestConnection.disabled = false;
      return;
    }

    try {
      const result = await window.exsql.testConnection(config);

      if (result.success) {
        connectionStatus.textContent = `${result.message}${result.serverVersion ? ` (v${result.serverVersion})` : ''}`;
        connectionStatus.className = 'connection-status success';
        connectionTested = true;
        btnStartMigration.disabled = false;
      } else {
        connectionStatus.textContent = result.message;
        connectionStatus.className = 'connection-status error';
        connectionTested = false;
        btnStartMigration.disabled = true;
      }
    } catch (err) {
      connectionStatus.textContent = `Error: ${err.message}`;
      connectionStatus.className = 'connection-status error';
    }

    btnTestConnection.disabled = false;
  });

  // DDL Preview
  btnPreviewDDL.addEventListener('click', async () => {
    const config = getConnectionConfig();
    const validationError = validateConnectionConfig(config);

    if (validationError) {
      alert(validationError);
      goToStep(3);
      return;
    }

    const options = getMigrationOptions();

    try {
      const result = await window.exsql.previewDDL(config, options);
      if (result.success) {
        ddlPreviewContent.textContent = result.ddl.join('\n\n-- ========================\n\n');
        ddlModal.classList.remove('hidden');
      } else {
        alert(result.error);
      }
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  });

  btnCloseModal.addEventListener('click', () => ddlModal.classList.add('hidden'));
  btnCloseModal2.addEventListener('click', () => ddlModal.classList.add('hidden'));
  btnCopyDDL.addEventListener('click', () => {
    navigator.clipboard.writeText(ddlPreviewContent.textContent);
    btnCopyDDL.textContent = 'Copiado!';
    setTimeout(() => { btnCopyDDL.textContent = 'Copiar SQL'; }, 2000);
  });

  if (btnCloseQueryModal) btnCloseQueryModal.addEventListener('click', closeSqlExplorer);
  if (btnCloseQueryModal2) btnCloseQueryModal2.addEventListener('click', closeSqlExplorer);
  if (btnRefreshSqlTables) btnRefreshSqlTables.addEventListener('click', () => loadDatabaseTables());
  if (btnRunQuery) btnRunQuery.addEventListener('click', handleRunReadOnlyQuery);
  if (btnRenderChart) btnRenderChart.addEventListener('click', renderChartFromControls);
  if (sqlChartType) sqlChartType.addEventListener('change', renderChartFromControls);
  if (sqlChartX) sqlChartX.addEventListener('change', renderChartFromControls);
  if (sqlChartY) sqlChartY.addEventListener('change', renderChartFromControls);

  if (sqlQueryEditor) {
    sqlQueryEditor.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        handleRunReadOnlyQuery();
      }
    });
  }

  if (sqlTableList) {
    sqlTableList.addEventListener('click', (event) => {
      const target = event.target.closest('.sql-table-item');
      if (!target) return;

      const { tableName } = target.dataset;
      if (!tableName) return;

      sqlQueryEditor.value = buildPreviewQuery(tableName);
      setSqlQueryStatus(`Consulta preparada para la tabla ${tableName}.`, 'info');
      sqlQueryEditor.focus();
    });
  }

  if (sqlExplorerModal) {
    sqlExplorerModal.addEventListener('click', (event) => {
      if (event.target === sqlExplorerModal) {
        closeSqlExplorer();
      }
    });
  }

  // ---- Step 4: Migration ----
  btnStartMigration.addEventListener('click', async () => {
    if (!connectionTested) {
      alert('Primero prueba la conexión');
      return;
    }

    goToStep(4);
    migrationLog.innerHTML = '<div class="log-entry info">Iniciando migración...</div>';
    migrationResults.classList.add('hidden');
    btnCancelMigration.classList.remove('hidden');
    btnNewMigration.classList.add('hidden');

    const config = getConnectionConfig();
    const options = getMigrationOptions();

    // Listen for progress
    const removeProgressListener = window.exsql.onMigrationProgress((progress) => {
      updateMigrationUI(progress);
    });

    const removeCompleteListener = window.exsql.onMigrationComplete((result) => {
      removeProgressListener();
      removeCompleteListener();
      showResults(result);
    });

    const removeErrorListener = window.exsql.onMigrationError((error) => {
      removeProgressListener();
      removeCompleteListener();
      removeErrorListener();
      addLogEntry(`Error fatal: ${error}`, 'error');
    });

    try {
      await window.exsql.startMigration(config, options);
    } catch (err) {
      addLogEntry(`Error: ${err.message}`, 'error');
    }
  });

  btnCancelMigration.addEventListener('click', async () => {
    await window.exsql.cancelMigration();
    addLogEntry('Cancelación solicitada...', 'warning');
    btnCancelMigration.disabled = true;
  });

  btnNewMigration.addEventListener('click', () => {
    tables = [];
    parsedFiles = [];
    sqlTables = [];
    lastQueryResult = null;
    connectionTested = false;
    closeSqlExplorer();
    resetStep1();
    goToStep(1);
  });

  function updateMigrationUI(progress) {
    // Stats
    statTables.textContent = `${progress.tablesCompleted} / ${progress.tablesTotal}`;
    statRows.textContent = progress.rowsInserted.toLocaleString();
    statTime.textContent = formatDuration(progress.elapsedMs);
    statEta.textContent = progress.estimatedRemainingMs > 0
      ? formatDuration(progress.estimatedRemainingMs)
      : '--:--';

    // Phase labels
    const phaseLabels = {
      parsing: 'Analizando archivo...',
      analyzing: 'Analizando tipos...',
      creating_tables: 'Creando tablas...',
      inserting_data: optIfExists.value === 'sync' ? 'Sincronizando datos...' : 'Insertando datos...',
      creating_indexes: 'Creando índices...',
      done: 'Completado',
      error: 'Error',
    };
    migrationPhase.textContent = phaseLabels[progress.phase] || progress.phase;

    // Progress bar
    const percent = progress.rowsTotal > 0
      ? Math.round((progress.rowsInserted / progress.rowsTotal) * 100)
      : 0;
    migrationPercent.textContent = `${percent}%`;
    migrationProgressFill.style.width = `${percent}%`;

    // Current table
    if (progress.currentTable) {
      migrationCurrentTable.textContent = `Tabla: ${progress.currentTable} (batch ${progress.currentBatch}/${progress.totalBatches})`;
    }

    // Errors
    const errCount = progress.errors.length;
    errorCount.textContent = `${errCount} error${errCount !== 1 ? 'es' : ''}`;
    errorCount.className = errCount > 0 ? 'badge has-errors' : 'badge';

    // Log new errors
    if (progress.errors.length > 0) {
      const lastError = progress.errors[progress.errors.length - 1];
      addLogEntry(`[${lastError.table}] ${lastError.message}`, 'error');
    }

    // Warnings
    if (progress.warnings.length > 0) {
      const lastWarning = progress.warnings[progress.warnings.length - 1];
      addLogEntry(lastWarning, 'warning');
    }
  }

  function showResults(result) {
    migrationResults.classList.remove('hidden');
    btnCancelMigration.classList.add('hidden');
    btnNewMigration.classList.remove('hidden');

    if (result.success) {
      resultsCard.className = 'results-card success';
      resultsTitle.textContent = 'Migración Completada Exitosamente';
      const summaryParts = [`${result.tablesCreated} tablas creadas`];
      if (result.totalRowsInserted > 0) summaryParts.push(`${result.totalRowsInserted.toLocaleString()} insertadas`);
      if (result.totalRowsUpdated > 0) summaryParts.push(`${result.totalRowsUpdated.toLocaleString()} actualizadas`);
      if (result.totalRowsDeleted > 0) summaryParts.push(`${result.totalRowsDeleted.toLocaleString()} eliminadas`);
      if (result.totalDuplicateRows > 0) summaryParts.push(`${result.totalDuplicateRows.toLocaleString()} duplicadas detectadas`);
      resultsSummary.textContent = `${summaryParts.join(' · ')} · ${formatDuration(result.duration)}`;
      addLogEntry(`Migración completada en ${formatDuration(result.duration)}`, 'success');
    } else {
      resultsCard.className = 'results-card failure';
      resultsTitle.textContent = 'Migración con Errores';
      const summaryParts = [];
      if (result.totalRowsInserted > 0) summaryParts.push(`${result.totalRowsInserted.toLocaleString()} insertadas`);
      if (result.totalRowsUpdated > 0) summaryParts.push(`${result.totalRowsUpdated.toLocaleString()} actualizadas`);
      if (result.totalRowsDeleted > 0) summaryParts.push(`${result.totalRowsDeleted.toLocaleString()} eliminadas`);
      if (result.totalDuplicateRows > 0) summaryParts.push(`${result.totalDuplicateRows.toLocaleString()} duplicadas detectadas`);
      summaryParts.push(`${result.totalErrors} errores`);
      resultsSummary.textContent = summaryParts.join(' · ');
    }

    // Render table details
    if (result.tables && result.tables.length > 0) {
      const diagnostics = [];
      for (const table of result.tables) {
        for (const warning of table.warnings || []) {
          diagnostics.push({ table: table.name, type: 'Advertencia', message: warning });
        }
        for (const suggestion of table.suggestions || []) {
          diagnostics.push({ table: table.name, type: 'Sugerencia', message: suggestion });
        }
      }

      resultsDetails.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Archivo</th>
              <th>Tabla</th>
              <th>Insertadas</th>
              <th>Actualizadas</th>
              <th>Omitidas</th>
              <th>Duplicados</th>
              <th>Eliminadas</th>
              <th>Columnas</th>
              <th>Errores</th>
            </tr>
          </thead>
          <tbody>
            ${result.tables.map((t) => `
              <tr>
                <td>${escapeHtml(t.sourceFileName || '')}</td>
                <td style="color: var(--accent)">${escapeHtml(t.name)}</td>
                <td>${t.rowsInserted.toLocaleString()}</td>
                <td>${(t.rowsUpdated || 0).toLocaleString()}</td>
                <td>${(t.rowsSkipped || 0).toLocaleString()}</td>
                <td>${(t.duplicateRows || 0).toLocaleString()}</td>
                <td>${(t.deletedRows || 0).toLocaleString()}</td>
                <td>${t.columnsCreated}</td>
                <td style="color: ${t.errors.length > 0 ? 'var(--error)' : 'var(--success)'}">${t.errors.length}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        ${diagnostics.length > 0 ? `
          <div class="results-notes">
            <h4>Diagnóstico y sugerencias</h4>
            <ul>
              ${diagnostics.map((item) => `
                <li>
                  <strong>${escapeHtml(item.type)}</strong> en ${escapeHtml(item.table)}: ${escapeHtml(item.message)}
                </li>
              `).join('')}
            </ul>
          </div>
        ` : ''}
      `;
    }
  }

  // ---- SQL Explorer ----
  async function openSqlExplorer() {
    const config = getConnectionConfig();
    const validationError = validateConnectionConfig(config);

    if (validationError) {
      alert(validationError);
      goToStep(3);
      return;
    }

    sqlExplorerModal.classList.remove('hidden');

    if (!sqlQueryEditor.value.trim()) {
      sqlQueryEditor.value = 'SELECT 1 AS ejemplo';
    }

    await loadDatabaseTables(config);
  }

  function closeSqlExplorer() {
    if (sqlExplorerModal) {
      sqlExplorerModal.classList.add('hidden');
    }
  }

  async function loadDatabaseTables(config = getConnectionConfig()) {
    const validationError = validateConnectionConfig(config);
    if (validationError) {
      setSqlQueryStatus(validationError, 'error');
      renderSqlTableList([]);
      return;
    }

    setSqlQueryStatus('Cargando tablas del esquema...', 'info');
    renderSqlTableList(null);

    try {
      const result = await window.exsql.listDatabaseTables(config);

      if (!result.success) {
        sqlTables = [];
        renderSqlTableList([]);
        setSqlQueryStatus(result.error || 'No se pudieron cargar las tablas.', 'error');
        return;
      }

      sqlTables = result.tables || [];
      renderSqlTableList(sqlTables);
      setSqlQueryStatus(`Se cargaron ${sqlTables.length} tabla${sqlTables.length !== 1 ? 's' : ''} desde ${config.database}.`, 'success');
    } catch (err) {
      sqlTables = [];
      renderSqlTableList([]);
      setSqlQueryStatus(`Error cargando tablas: ${err.message}`, 'error');
    }
  }

  function renderSqlTableList(tableList) {
    if (!sqlTableList) return;

    if (tableList === null) {
      sqlTableList.innerHTML = '<div class="sql-empty-state">Cargando tablas...</div>';
      return;
    }

    if (!tableList.length) {
      sqlTableList.innerHTML = '<div class="sql-empty-state">No hay tablas disponibles en el esquema actual.</div>';
      return;
    }

    sqlTableList.innerHTML = tableList.map((table) => `
      <button class="sql-table-item" data-table-name="${escapeHtml(table.name)}">
        <strong>${escapeHtml(table.name)}</strong>
        <small>Clic para preparar una consulta base</small>
      </button>
    `).join('');
  }

  async function handleRunReadOnlyQuery() {
    const config = getConnectionConfig();
    const validationError = validateConnectionConfig(config);

    if (validationError) {
      setSqlQueryStatus(validationError, 'error');
      goToStep(3);
      return;
    }

    const query = sqlQueryEditor.value.trim();
    if (!query) {
      setSqlQueryStatus('Escribe una consulta SQL de solo lectura antes de ejecutar.', 'error');
      return;
    }

    setSqlQueryStatus('Ejecutando consulta...', 'info');

    try {
      const response = await window.exsql.runReadOnlyQuery(config, query, 500);

      if (!response.success) {
        setSqlQueryStatus(response.error || 'La consulta no se pudo ejecutar.', 'error');
        return;
      }

      lastQueryResult = response.result;
      renderQueryResults(lastQueryResult);
      populateChartSelectors(lastQueryResult);

      const rowLabel = lastQueryResult.rowCount === 1 ? 'fila' : 'filas';
      const truncatedMessage = lastQueryResult.truncated
        ? ` Mostrando solo las primeras ${lastQueryResult.rows.length}.`
        : '';
      setSqlQueryStatus(`Consulta ejecutada correctamente: ${lastQueryResult.rowCount} ${rowLabel} devueltas.${truncatedMessage}`, lastQueryResult.truncated ? 'warning' : 'success');
    } catch (err) {
      setSqlQueryStatus(`Error ejecutando la consulta: ${err.message}`, 'error');
    }
  }

  function renderQueryResults(result) {
    if (!sqlResultsTable || !sqlResultSummary) return;

    const columns = result.columns || [];
    const rows = result.rows || [];

    sqlResultSummary.textContent = result.truncated
      ? `${rows.length} de ${result.rowCount} filas`
      : `${result.rowCount} fila${result.rowCount !== 1 ? 's' : ''}`;

    if (!columns.length) {
      sqlResultsTable.innerHTML = '<div class="sql-empty-state">La consulta no devolvió columnas visibles.</div>';
      clearChart('La consulta no devolvió columnas para graficar.');
      return;
    }

    if (!rows.length) {
      sqlResultsTable.innerHTML = '<div class="sql-empty-state">La consulta se ejecutó correctamente, pero no devolvió filas.</div>';
      clearChart('La consulta no devolvió filas para crear un gráfico.');
      return;
    }

    sqlResultsTable.innerHTML = `
      <table>
        <thead>
          <tr>
            ${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              ${columns.map((column) => `<td>${escapeHtml(formatCellValue(row[column]))}</td>`).join('')}
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }

  function populateChartSelectors(result) {
    if (!sqlChartX || !sqlChartY || !btnRenderChart) return;

    const columns = result.columns || [];
    const numericColumns = columns.filter((column) =>
      result.rows.some((row) => Number.isFinite(coerceNumericValue(row[column])))
    );

    sqlChartX.innerHTML = columns.length
      ? columns.map((column) => `<option value="${escapeHtml(column)}">${escapeHtml(column)}</option>`).join('')
      : '<option value="">Sin columnas</option>';

    sqlChartY.innerHTML = numericColumns.length
      ? numericColumns.map((column) => `<option value="${escapeHtml(column)}">${escapeHtml(column)}</option>`).join('')
      : '<option value="">Sin columnas numéricas</option>';

    if (columns.length > 0) {
      sqlChartX.value = columns[0];
    }

    if (numericColumns.length > 0) {
      sqlChartY.value = numericColumns.find((column) => column !== sqlChartX.value) || numericColumns[0];
    }

    btnRenderChart.disabled = !(columns.length > 0 && numericColumns.length > 0);

    if (btnRenderChart.disabled) {
      clearChart('La consulta necesita al menos una columna de categorías y una columna numérica para generar un gráfico.');
      return;
    }

    renderChartFromControls();
  }

  function renderChartFromControls() {
    if (!lastQueryResult || !lastQueryResult.rows || !lastQueryResult.rows.length) {
      clearChart('Ejecuta una consulta antes de intentar crear un gráfico.');
      return;
    }

    const xColumn = sqlChartX.value;
    const yColumn = sqlChartY.value;

    if (!xColumn || !yColumn) {
      clearChart('Selecciona una columna para el eje X y una columna numérica para el eje Y.');
      return;
    }

    const points = lastQueryResult.rows
      .map((row) => ({
        label: formatChartLabel(row[xColumn]),
        value: coerceNumericValue(row[yColumn]),
      }))
      .filter((point) => Number.isFinite(point.value))
      .slice(0, 40);

    if (!points.length) {
      clearChart('No se encontraron valores numéricos válidos en la columna seleccionada para el eje Y.');
      return;
    }

    sqlChartEmpty.classList.add('hidden');
    drawChartOnCanvas(sqlChartCanvas, points, sqlChartType.value, xColumn, yColumn);
  }

  function clearChart(message) {
    if (!sqlChartCanvas || !sqlChartEmpty) return;

    const ctx = sqlChartCanvas.getContext('2d');
    if (ctx) {
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, sqlChartCanvas.width || 0, sqlChartCanvas.height || 0);
    }

    sqlChartEmpty.textContent = message;
    sqlChartEmpty.classList.remove('hidden');
  }

  function drawChartOnCanvas(canvas, points, type, xLabel, yLabel) {
    const context = canvas.getContext('2d');
    if (!context) return;

    const bounds = canvas.getBoundingClientRect();
    const width = Math.max(Math.round(bounds.width || 820), 640);
    const height = Math.max(Math.round(bounds.height || 340), 320);
    const deviceScale = window.devicePixelRatio || 1;

    canvas.width = width * deviceScale;
    canvas.height = height * deviceScale;
    context.setTransform(deviceScale, 0, 0, deviceScale, 0, 0);
    context.clearRect(0, 0, width, height);

    const padding = { top: 28, right: 18, bottom: 72, left: 62 };
    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;
    const minValue = Math.min(...points.map((point) => point.value), 0);
    const maxValue = Math.max(...points.map((point) => point.value), 0);
    const valueRange = maxValue - minValue || 1;
    const baselineY = padding.top + plotHeight - ((0 - minValue) / valueRange) * plotHeight;
    const visibleLabelStep = Math.max(1, Math.ceil(points.length / 8));

    context.fillStyle = '#ffffff';
    context.fillRect(0, 0, width, height);

    context.strokeStyle = '#d8dee7';
    context.lineWidth = 1;
    for (let index = 0; index <= 5; index++) {
      const y = padding.top + (plotHeight / 5) * index;
      context.beginPath();
      context.moveTo(padding.left, y);
      context.lineTo(width - padding.right, y);
      context.stroke();
    }

    context.strokeStyle = '#6d7f93';
    context.lineWidth = 1.2;
    context.beginPath();
    context.moveTo(padding.left, padding.top);
    context.lineTo(padding.left, padding.top + plotHeight);
    context.lineTo(width - padding.right, padding.top + plotHeight);
    context.stroke();

    const scaleY = (value) => padding.top + plotHeight - ((value - minValue) / valueRange) * plotHeight;
    const stepX = points.length > 1 ? plotWidth / points.length : plotWidth;

    if (type === 'line') {
      context.strokeStyle = '#2b5797';
      context.lineWidth = 2;
      context.beginPath();

      points.forEach((point, index) => {
        const x = padding.left + stepX * index + stepX / 2;
        const y = scaleY(point.value);
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.stroke();

      context.fillStyle = '#4a83c8';
      points.forEach((point, index) => {
        const x = padding.left + stepX * index + stepX / 2;
        const y = scaleY(point.value);
        context.beginPath();
        context.arc(x, y, 3.5, 0, Math.PI * 2);
        context.fill();
      });
    } else {
      const barWidth = Math.max(10, (plotWidth / Math.max(points.length, 1)) * 0.62);
      context.fillStyle = '#4a83c8';

      points.forEach((point, index) => {
        const centerX = padding.left + stepX * index + stepX / 2;
        const x = centerX - barWidth / 2;
        const y = point.value >= 0 ? scaleY(point.value) : baselineY;
        const barHeight = Math.abs(scaleY(point.value) - baselineY);
        context.fillRect(x, y, barWidth, Math.max(barHeight, 1));
      });
    }

    context.strokeStyle = '#9aa8b8';
    context.beginPath();
    context.moveTo(padding.left, baselineY);
    context.lineTo(width - padding.right, baselineY);
    context.stroke();

    context.fillStyle = '#4a4a4a';
    context.font = '11px Segoe UI';
    context.textAlign = 'right';
    context.textBaseline = 'middle';
    for (let index = 0; index <= 5; index++) {
      const value = maxValue - (valueRange / 5) * index;
      const y = padding.top + (plotHeight / 5) * index;
      context.fillText(formatAxisNumber(value), padding.left - 8, y);
    }

    context.textAlign = 'center';
    context.textBaseline = 'top';
    points.forEach((point, index) => {
      if (index % visibleLabelStep !== 0) return;
      const x = padding.left + stepX * index + stepX / 2;
      const label = shortenLabel(point.label, 14);
      context.save();
      context.translate(x, height - padding.bottom + 18);
      context.rotate(-Math.PI / 4);
      context.fillText(label, 0, 0);
      context.restore();
    });

    context.fillStyle = '#2b5797';
    context.font = 'bold 12px Segoe UI';
    context.textAlign = 'center';
    context.textBaseline = 'top';
    context.fillText(`${yLabel} por ${xLabel}`, width / 2, 8);
  }

  // ---- Helpers ----
  function getConnectionConfig() {
    return {
      dialect: selectedDialect,
      host: connHost.value.trim() || 'localhost',
      port: parseInt(connPort.value) || defaultPorts[selectedDialect],
      database: connDatabase.value.trim(),
      user: connUser.value.trim(),
      password: connPassword.value,
      instanceName: connInstance.value.trim() || undefined,
      ssl: false,
      connectionTimeout: 30000,
    };
  }

  function validateConnectionConfig(config) {
    if (!config.host) {
      return 'Debes ingresar el host o servidor de la base de datos.';
    }

    if (!Number.isFinite(config.port) || config.port <= 0) {
      return 'Debes ingresar un puerto válido.';
    }

    if (!config.database) {
      return 'Debes ingresar el nombre de la base de datos destino.';
    }

    if (!config.user) {
      return `Debes ingresar el usuario de la base de datos. Para ${config.dialect === 'mysql' ? 'MySQL normalmente es root' : config.dialect === 'postgresql' ? 'PostgreSQL normalmente es postgres' : 'SQL Server suele ser sa o un usuario SQL válido'}.`;
    }

    return null;
  }

  function setSqlQueryStatus(message, type = 'info') {
    if (!sqlQueryStatus) return;
    sqlQueryStatus.textContent = message;
    sqlQueryStatus.className = `sql-query-status ${type}`;
  }

  function getMigrationOptions() {
    return {
      batchSize: parseInt(optBatchSize.value),
      ifTableExists: optIfExists.value,
      tablePrefix: optPrefix.value || '',
      createIndexes: optIndexes.checked,
      useTransactions: optTransactions.checked,
      createMigrationLog: optLog.checked,
      maxErrors: 100,
      encoding: 'utf-8',
    };
  }

  function updateStartActionLabel() {
    if (!btnStartMigrationLabel || !optIfExists) return;

    btnStartMigrationLabel.textContent = optIfExists.value === 'sync'
      ? 'Sincronizar Cambios'
      : 'Iniciar Migración';
  }

  function addLogEntry(message, type = 'info') {
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    const time = new Date().toLocaleTimeString();
    entry.textContent = `[${time}] ${message}`;
    migrationLog.appendChild(entry);
    migrationLog.scrollTop = migrationLog.scrollHeight;
  }

  function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (minutes > 0) {
      return `${minutes}:${secs.toString().padStart(2, '0')}`;
    }
    return `0:${secs.toString().padStart(2, '0')}`;
  }

  function formatBytes(bytes) {
    if (!bytes || bytes <= 0) return '0 B';

    const units = ['B', 'KB', 'MB', 'GB'];
    let value = bytes;
    let unitIndex = 0;

    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }

    const precision = unitIndex === 0 ? 0 : 1;
    return `${value.toFixed(precision)} ${units[unitIndex]}`;
  }

  function buildPreviewQuery(tableName) {
    const safeTableName = quoteIdentifierForDialect(tableName);

    if (selectedDialect === 'mssql') {
      return `SELECT TOP 200 *\nFROM ${safeTableName};`;
    }

    return `SELECT *\nFROM ${safeTableName}\nLIMIT 200;`;
  }

  function quoteIdentifierForDialect(name) {
    if (selectedDialect === 'mysql') {
      return `\`${String(name).replace(/\`/g, '``')}\``;
    }

    if (selectedDialect === 'mssql') {
      return `[${String(name).replace(/\]/g, ']]')}]`;
    }

    return `"${String(name).replace(/"/g, '""')}"`;
  }

  function formatCellValue(value) {
    if (value === null || value === undefined || value === '') return 'NULL';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  function formatChartLabel(value) {
    if (value === null || value === undefined || value === '') return '(vacío)';
    return String(value);
  }

  function coerceNumericValue(value) {
    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : NaN;
    }

    if (typeof value === 'string') {
      const normalized = value.replace(/,/g, '').trim();
      if (!normalized) return NaN;
      const parsed = Number(normalized);
      return Number.isFinite(parsed) ? parsed : NaN;
    }

    return NaN;
  }

  function formatAxisNumber(value) {
    if (!Number.isFinite(value)) return '';

    if (Math.abs(value) >= 1000000) {
      return `${(value / 1000000).toFixed(1)}M`;
    }

    if (Math.abs(value) >= 1000) {
      return `${(value / 1000).toFixed(1)}K`;
    }

    return Number.isInteger(value) ? String(Math.round(value)) : value.toFixed(2);
  }

  function shortenLabel(value, maxLength) {
    return value.length > maxLength ? `${value.slice(0, maxLength - 1)}…` : value;
  }

  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

})();
