if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

const SOURCES_API_BASE = '/api/sources';

let currentSources = [];
let selectedFile = null;
let uploadedMetadata = null;
let statusPolling = null;
let editingSourceId = null;
let editingSource = null;

const tableSelectionState = {
    modal: document.getElementById('tableSelectionModal'),
    listEl: document.getElementById('tableSelectionList'),
    hintEl: document.getElementById('tableSelectionHint'),
    currentSource: null,
    schemaData: [],
    selectedTables: new Set(),
    loading: false,
    error: null
};

const sourceTypeSelect = document.getElementById('sourceType');
const dbConfigSection = document.getElementById('dbConfigSection');
const fileConfigSection = document.getElementById('fileConfigSection');
const csvDelimiterSection = document.getElementById('csvDelimiterSection');
const csvDelimiterInput = document.getElementById('csvDelimiter');
const dropzone = document.getElementById('fileDropzone');
const fileInput = document.getElementById('fileInput');
const fileNameLabel = document.getElementById('fileName');
const dbTestButton = document.getElementById('testDbConnection');
const dbTestResult = document.getElementById('dbTestResult');

setupEventListeners();
loadSources();

function setupEventListeners() {
    document.getElementById('logoutBtn').addEventListener('click', () => {
        clearToken();
        window.location.href = '/';
    });

    document.addEventListener('click', (event) => {
        if (!event.target.closest('.action-menu')) {
            closeAllActionMenus();
        }
    });

    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            closeAllActionMenus();
        }
    });

    document.getElementById('addSourceBtn').addEventListener('click', () => openSourceModal());

    document.getElementById('refreshBtn').addEventListener('click', loadSources);

    document.getElementById('addSourceForm').addEventListener('submit', handleSaveSource);

    if (sourceTypeSelect) {
        sourceTypeSelect.addEventListener('change', handleTypeChange);
    }

    if (dropzone) {
        dropzone.addEventListener('click', () => fileInput && fileInput.click());
        dropzone.addEventListener('dragover', (event) => {
            event.preventDefault();
            dropzone.classList.add('dragging');
        });
        dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragging'));
        dropzone.addEventListener('drop', (event) => {
            event.preventDefault();
            dropzone.classList.remove('dragging');
            const file = event.dataTransfer.files[0];
            if (file) {
                setSelectedFile(file);
            }
        });
    }

    if (fileInput) {
        fileInput.addEventListener('change', (event) => {
            const file = event.target.files[0];
            if (file) {
                setSelectedFile(file);
            }
        });
    }

    if (dbTestButton) {
        dbTestButton.addEventListener('click', async () => {
            try {
                const config = buildDatabaseConfig();
                await api.post(`${SOURCES_API_BASE}/test-connection`, {
                    type: 'DB',
                    config
                });
                dbTestResult.textContent = 'Connection successful.';
                dbTestResult.style.color = 'var(--success-color)';
            } catch (error) {
                dbTestResult.textContent = error.message || 'Connection failed.';
                dbTestResult.style.color = 'var(--error-color)';
            }
        });
    }

    const skipTables = document.getElementById('skipTableSelection');
    const closeTables = document.getElementById('closeTableSelection');
    const saveTables = document.getElementById('saveTableSelection');
    [skipTables, closeTables].forEach(btn => {
        if (btn) {
            btn.addEventListener('click', closeTableSelectionModal);
        }
    });
    if (saveTables) {
        saveTables.addEventListener('click', saveTableSelection);
    }
}

function handleTypeChange(event) {
    const value = event?.target?.value ?? sourceTypeSelect?.value ?? '';
    dbConfigSection.style.display = value === 'DB' ? 'block' : 'none';
    fileConfigSection.style.display = value === 'CSV' ? 'block' : 'none';
    if (csvDelimiterSection) {
        csvDelimiterSection.style.display = value === 'CSV' ? 'block' : 'none';
    }
    if (fileNameLabel && value !== 'CSV') {
        fileNameLabel.textContent = '';
    }
}

function resetConfigSections() {
    dbConfigSection.style.display = 'none';
    fileConfigSection.style.display = 'none';
    if (csvDelimiterSection) {
        csvDelimiterSection.style.display = 'none';
    }
}

function openSourceModal(source = null) {
    const form = document.getElementById('addSourceForm');
    const modal = document.getElementById('addSourceModal');
    const titleEl = modal?.querySelector('.modal-header h2');
    const submitBtn = form?.querySelector('button[type="submit"]');

    closeAllActionMenus();
    form?.reset();
    resetConfigSections();

    editingSourceId = source?.id || null;
    editingSource = source || null;
    selectedFile = null;
    uploadedMetadata = null;

    if (fileNameLabel) {
        fileNameLabel.textContent = '';
    }
    if (dbTestResult) {
        dbTestResult.textContent = '';
    }

    document.getElementById('sourceName').value = source?.name || '';
    document.getElementById('sourceRole').value = source?.role || 'SOURCE';

    if (sourceTypeSelect) {
        sourceTypeSelect.value = source?.type || '';
        handleTypeChange();
    }

    if (source?.type === 'DB' && source.config) {
        document.getElementById('dbHost').value = source.config.host || '';
        document.getElementById('dbPort').value = source.config.port || '5432';
        document.getElementById('dbName').value = source.config.database || '';
        document.getElementById('dbUser').value = source.config.username || '';
        document.getElementById('dbPassword').value = source.config.password || '';
    }

    if (source?.type === 'CSV' && source.config) {
        const delimiter = source.config.delimiter || ',';
        if (csvDelimiterInput) {
            csvDelimiterInput.value = delimiter;
        }
        if (fileNameLabel) {
            const existingName = source.config.displayFilename || source.config.fileName || source.config.originalFilename;
            fileNameLabel.textContent = existingName
                ? `Using existing file: ${existingName}`
                : 'Existing CSV file will be reused.';
        }
    }

    if (titleEl) {
        titleEl.textContent = source ? 'Edit Source' : 'Create Source';
    }
    if (submitBtn) {
        submitBtn.textContent = source ? 'Save Changes' : 'Create Source';
    }

    modal.style.display = 'flex';
}

function setSelectedFile(file) {
    selectedFile = file;
    uploadedMetadata = null;
    if (fileNameLabel) {
        fileNameLabel.textContent = `${file.name} (${Math.round(file.size / 1024)} KB)`;
    }
}

async function handleSaveSource(event) {
    event.preventDefault();

    const name = document.getElementById('sourceName').value.trim();
    const type = document.getElementById('sourceType').value;
    const role = document.getElementById('sourceRole').value || 'SOURCE';

    if (!name) {
        toast.error('Source name is required');
        return;
    }
    if (!type) {
        toast.error('Source type is required');
        return;
    }

    let config = editingSource?.config ? { ...editingSource.config } : {};
    try {
        if (type === 'DB') {
            config = buildDatabaseConfig();
        } else if (type === 'CSV') {
            const usingExistingCsv = editingSourceId && editingSource?.type === 'CSV' && !selectedFile;
            if (usingExistingCsv) {
                config = {
                    ...(editingSource?.config || {}),
                    delimiter: (csvDelimiterInput?.value || ',').trim() || ','
                };
            } else {
                const fileConfig = await ensureFileUploaded(type, name);
                config = { ...fileConfig };
            }
        }
    } catch (error) {
        toast.error(error.message || 'Invalid configuration');
        return;
    }

    const submitBtn = event.target.querySelector('button[type="submit"]');
    submitBtn.disabled = true;
    const loadingLabel = editingSourceId ? 'Saving...' : 'Creating...';
    submitBtn.innerHTML = `<span class="spinner"></span> ${loadingLabel}`;

    try {
        const payload = { name, type, role, config };
        if (editingSourceId) {
            await api.put(`${SOURCES_API_BASE}/${editingSourceId}`, payload);
            toast.success('Source updated successfully');
        } else {
            const created = await api.post(SOURCES_API_BASE, payload);
            toast.success('Source created successfully');
            openTableSelectionModal(created);
        }
        closeAddSourceModal();
        await loadSources();
    } catch (error) {
        const message = editingSourceId ? 'Failed to update source' : 'Failed to create source';
        toast.error(error.message || message);
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = editingSourceId ? 'Save Changes' : 'Create Source';
    }
}

function openTableSelectionModal(source) {
    if (!tableSelectionState.modal || !source || source.type !== 'DB') {
        return;
    }
    tableSelectionState.currentSource = source;
    tableSelectionState.loading = true;
    tableSelectionState.error = null;
    tableSelectionState.schemaData = [];
    tableSelectionState.selectedTables = new Set();
    if (tableSelectionState.hintEl) {
        tableSelectionState.hintEl.textContent = 'Loading tables…';
    }
    renderTableSelectionList();
    tableSelectionState.modal.style.display = 'flex';
    fetchTablesForSource(source.id);
}

function closeTableSelectionModal() {
    if (tableSelectionState.modal) {
        tableSelectionState.modal.style.display = 'none';
    }
    tableSelectionState.currentSource = null;
}

async function fetchTablesForSource(sourceId) {
    tableSelectionState.loading = true;
    tableSelectionState.error = null;
    renderTableSelectionList();
    try {
        const tables = await api.get(`${SOURCES_API_BASE}/${sourceId}/schema`);
        tableSelectionState.schemaData = groupTablesBySchema(tables || []);
        tableSelectionState.selectedTables = preselectAllTables(tableSelectionState.schemaData);
    } catch (error) {
        tableSelectionState.error = error.message || 'Failed to load tables';
    } finally {
        tableSelectionState.loading = false;
        renderTableSelectionList();
    }
}

function groupTablesBySchema(tables) {
    const grouped = new Map();
    tables.forEach(entry => {
        const schema = entry.schema || '(default)';
        if (!grouped.has(schema)) {
            grouped.set(schema, []);
        }
        grouped.get(schema).push(entry.tableName);
    });
    return Array.from(grouped.entries()).map(([schema, tableNames]) => ({ schema, tables: tableNames.sort() }));
}

function preselectAllTables(schemaData) {
    const selected = new Set();
    schemaData.forEach(group => {
        group.tables.forEach(table => selected.add(buildTableKey(group.schema, table)));
    });
    return selected;
}

function buildTableKey(schema, table) {
    return `${schema || ''}|${table}`;
}

function renderTableSelectionList() {
    const container = tableSelectionState.listEl;
    if (!container) {
        return;
    }
    container.innerHTML = '';
    if (tableSelectionState.loading) {
        container.innerHTML = '<div class="state-message">Loading tables…</div>';
        return;
    }
    if (tableSelectionState.error) {
        container.innerHTML = `<div class="state-message error-box">${tableSelectionState.error}</div>`;
        if (tableSelectionState.hintEl) {
            tableSelectionState.hintEl.textContent = 'Unable to load tables for this source.';
        }
        return;
    }
    if (!tableSelectionState.schemaData.length) {
        container.innerHTML = '<div class="state-message">No tables found.</div>';
        if (tableSelectionState.hintEl) {
            tableSelectionState.hintEl.textContent = 'No tables available to select.';
        }
        return;
    }

    tableSelectionState.schemaData.forEach((group, idx) => {
        const groupEl = document.createElement('div');
        groupEl.className = 'schema-group';

        const header = document.createElement('div');
        header.className = 'schema-header';
        const schemaId = `schema-${idx}`;
        const schemaLabel = document.createElement('label');
        schemaLabel.setAttribute('for', schemaId);
        schemaLabel.textContent = group.schema;
        const schemaCheckbox = document.createElement('input');
        schemaCheckbox.type = 'checkbox';
        schemaCheckbox.id = schemaId;
        const allSelected = group.tables.every(t => tableSelectionState.selectedTables.has(buildTableKey(group.schema, t)));
        schemaCheckbox.checked = allSelected;
        schemaCheckbox.addEventListener('change', (event) => toggleSchemaSelection(group, event.target.checked));
        header.appendChild(schemaLabel);
        header.appendChild(schemaCheckbox);
        groupEl.appendChild(header);

        const tableList = document.createElement('div');
        tableList.className = 'table-list';
        group.tables.forEach((tableName, tableIdx) => {
            const row = document.createElement('label');
            row.className = 'table-row';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `table-${idx}-${tableIdx}`;
            checkbox.checked = tableSelectionState.selectedTables.has(buildTableKey(group.schema, tableName));
            checkbox.addEventListener('change', (event) => toggleTableSelection(group.schema, tableName, event.target.checked));
            const text = document.createElement('span');
            text.textContent = tableName;
            row.appendChild(checkbox);
            row.appendChild(text);
            tableList.appendChild(row);
        });
        groupEl.appendChild(tableList);
        container.appendChild(groupEl);
    });

    if (tableSelectionState.hintEl) {
        const total = tableSelectionState.selectedTables.size;
        tableSelectionState.hintEl.textContent = `${total} table${total === 1 ? '' : 's'} selected`;
    }
}

function toggleSchemaSelection(group, checked) {
    group.tables.forEach(table => {
        const key = buildTableKey(group.schema, table);
        if (checked) {
            tableSelectionState.selectedTables.add(key);
        } else {
            tableSelectionState.selectedTables.delete(key);
        }
    });
    renderTableSelectionList();
}

function toggleTableSelection(schema, table, checked) {
    const key = buildTableKey(schema, table);
    if (checked) {
        tableSelectionState.selectedTables.add(key);
    } else {
        tableSelectionState.selectedTables.delete(key);
    }
    renderTableSelectionList();
}

async function saveTableSelection() {
    if (!tableSelectionState.currentSource) {
        closeTableSelectionModal();
        return;
    }
    const tables = Array.from(tableSelectionState.selectedTables).map(key => {
        const [schema, table] = key.split('|');
        const payload = { table };
        if (schema) {
            payload.schema = schema;
            payload.alias = `${schema}.${table}`;
        } else {
            payload.alias = table;
        }
        return payload;
    });

    const baseConfig = tableSelectionState.currentSource.config ? { ...tableSelectionState.currentSource.config } : {};
    baseConfig.tables = tables;

    try {
        await api.put(`${SOURCES_API_BASE}/${tableSelectionState.currentSource.id}`, { config: baseConfig });
        toast.success('Table selection saved');
        closeTableSelectionModal();
        await loadSources();
    } catch (error) {
        toast.error(error.message || 'Failed to save selection');
    }
}

function buildDatabaseConfig() {
    const host = document.getElementById('dbHost').value.trim();
    const port = document.getElementById('dbPort').value.trim();
    const database = document.getElementById('dbName').value.trim();
    const username = document.getElementById('dbUser').value.trim();
    const password = document.getElementById('dbPassword').value;

    if (!host || !database || !username || !password) {
        throw new Error('All database fields are required');
    }

    return {
        host,
        port,
        database,
        username,
        password
    };
}

async function ensureFileUploaded(type, name) {
    if (!selectedFile) {
        throw new Error('Please select a file to upload.');
    }
    if (uploadedMetadata && uploadedMetadata.originalFilename === selectedFile.name) {
        return uploadedMetadata.config || {};
    }

    const formData = new FormData();
    formData.append('file', selectedFile);
    const sourceKey = sanitizeForUpload(name || selectedFile.name);
    formData.append('sourceKey', `${sourceKey}-${Date.now()}`);
    if (type === 'CSV') {
        const delimiter = (csvDelimiterInput?.value || ',').trim() || ',';
        formData.append('delimiter', delimiter);
    }
    formData.append('encoding', 'UTF-8');

    const endpoint = '/api/upload/csv';
    const response = await api.postMultipart(endpoint, formData);
    uploadedMetadata = response;
    toast.success('File uploaded successfully');
    return response.config || {};
}

function sanitizeForUpload(value) {
    return (value || 'source')
        .toLowerCase()
        .replace(/[^a-z0-9_-]/g, '-')
        .replace(/-+/g, '-')
        .substring(0, 40);
}

function closeAddSourceModal() {
    document.getElementById('addSourceModal').style.display = 'none';
    document.getElementById('addSourceForm').reset();
    resetConfigSections();
    editingSourceId = null;
    editingSource = null;
    selectedFile = null;
    uploadedMetadata = null;
    if (fileNameLabel) {
        fileNameLabel.textContent = '';
    }
    if (dbTestResult) {
        dbTestResult.textContent = '';
    }
    const modalTitle = document.querySelector('#addSourceModal .modal-header h2');
    const submitBtn = document.querySelector('#addSourceForm button[type="submit"]');
    if (modalTitle) {
        modalTitle.textContent = 'Create Source';
    }
    if (submitBtn) {
        submitBtn.textContent = 'Create Source';
    }
}

function closeStatusModal() {
    document.getElementById('statusModal').style.display = 'none';
    if (statusPolling) {
        clearInterval(statusPolling);
        statusPolling = null;
    }
}

async function loadSources() {
    const container = document.getElementById('sourcesTable');
    container.innerHTML = `
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
    `;

    try {
        currentSources = await api.get(`${SOURCES_API_BASE}?role=SOURCE`);
        renderSources();
    } catch (error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <p>Unable to load sources</p>
                <button class="btn btn-primary btn-sm" onclick="loadSources()">Retry</button>
            </div>
        `;
    }
}

function renderSources() {
    const container = document.getElementById('sourcesTable');

    if (!Array.isArray(currentSources) || currentSources.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📁</div>
                <p>No sources configured yet</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Updated</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                ${currentSources.map(renderSourceRow).join('')}
            </tbody>
        </table>
    `;
}

function renderSourceRow(source) {
    const encodedName = encodeURIComponent(source.name || '');
    return `
        <tr>
            <td><strong>${escapeHtml(source.name)}</strong></td>
            <td><span class="badge badge-info">${escapeHtml(source.type)}</span></td>
            <td>${getStatusBadge(source.status)}</td>
            <td>${formatLastSync(source)}</td>
            <td class="actions-cell">
                <div class="action-menu">
                    <button class="btn-icon" type="button" onclick="toggleActionMenu(event)">
                        <span aria-hidden="true">⋯</span>
                        <span class="sr-only">Open actions for ${escapeHtml(source.name)}</span>
                    </button>
                    <div class="action-dropdown">
                        <button class="action-item" type="button" onclick="startEditSource('${source.id}', event)">Edit</button>
                        <button class="action-item action-danger" type="button" onclick="deleteSource('${source.id}', '${encodedName}')">Delete</button>
                    </div>
                </div>
            </td>
        </tr>
    `;
}

function startEditSource(sourceId, event) {
    event?.stopPropagation();
    const source = currentSources.find(src => src.id === sourceId);
    if (!source) {
        toast.error('Source not found');
        return;
    }
    openSourceModal(source);
}

function getStatusBadge(status) {
    if (!status) {
        return '<span class="badge badge-secondary">Unknown</span>';
    }
    const badges = {
        'ACTIVE': '<span class="badge badge-success">Active</span>',
        'PAUSED': '<span class="badge badge-warning">Paused</span>'
    };
    return badges[status] || `<span class="badge badge-secondary">${escapeHtml(status)}</span>`;
}

function formatLastSync(source) {
    if (!source || !source.updatedAt) {
        return '<span style="color: var(--text-secondary);">Never</span>';
    }
    try {
        const date = new Date(source.updatedAt);
        if (Number.isNaN(date.getTime())) {
            return '<span style="color: var(--text-secondary);">Never</span>';
        }
        return `<span style="color: var(--text-secondary);">${date.toLocaleString()}</span>`;
    } catch (error) {
        return '<span style="color: var(--text-secondary);">Never</span>';
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text ?? '';
    return div.innerHTML;
}

async function ingestSource(sourceId) {
    closeAllActionMenus();
    try {
        await api.post(`${SOURCES_API_BASE}/${sourceId}/refresh`);
        toast.success('Ingestion started');
        loadSources();
    } catch (error) {
        toast.error(error.message || 'Failed to start ingestion');
    }
}

async function viewStatus(sourceId) {
    closeAllActionMenus();
    document.getElementById('statusModal').style.display = 'flex';
    const statusContent = document.getElementById('statusContent');
    statusContent.innerHTML = `
        <div class="skeleton-text skeleton"></div>
        <div class="skeleton-text skeleton"></div>
        <div class="skeleton-text skeleton"></div>
    `;

    try {
        const runs = await api.get(`${SOURCES_API_BASE}/${sourceId}/runs`);
        if (!runs || runs.length === 0) {
            statusContent.innerHTML = '<div class="empty-state">No ingestion runs yet.</div>';
            return;
        }
        const latest = runs[0];
        const processed = latest.rowsStored ?? latest.rowsRead ?? 0;
        const endedAt = latest.endedAt ? new Date(latest.endedAt).toLocaleString() : 'In progress';
        statusContent.innerHTML = `
            <div><strong>Status:</strong> ${escapeHtml(latest.runStatus)}</div>
            <div><strong>Records Processed:</strong> ${processed}</div>
            <div><strong>Started:</strong> ${latest.startedAt ? new Date(latest.startedAt).toLocaleString() : 'N/A'}</div>
            <div><strong>Ended:</strong> ${endedAt}</div>
            ${latest.errorMessage ? `<div class="alert alert-error">${escapeHtml(latest.errorMessage)}</div>` : ''}
        `;
    } catch (error) {
        statusContent.innerHTML = '<div class="alert alert-error">Failed to load run status.</div>';
    }
}

async function deleteSource(sourceId, encodedName = '') {
    closeAllActionMenus();
    const sourceName = decodeURIComponent(encodedName || '');
    const label = sourceName ? `"${sourceName}"` : 'this source';
    const confirmed = window.confirm(`Are you sure you want to delete ${label}? This action cannot be undone.`);
    if (!confirmed) {
        return;
    }

    try {
        await api.delete(`${SOURCES_API_BASE}/${sourceId}`);
        toast.success('Source deleted');
        loadSources();
    } catch (error) {
        toast.error(error.message || 'Failed to delete source');
    }
}

function toggleActionMenu(event) {
    event.preventDefault();
    event.stopPropagation();
    const menu = event.currentTarget.closest('.action-menu');
    if (!menu) {
        return;
    }
    const isOpen = menu.classList.contains('open');
    closeAllActionMenus();
    if (!isOpen) {
        menu.classList.add('open');
    }
}

function closeAllActionMenus() {
    document.querySelectorAll('.action-menu.open').forEach(menu => menu.classList.remove('open'));
}
