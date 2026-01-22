if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

const SOURCES_API_BASE = '/api/sources';
const CONNECTIONS_API_BASE = '/api/connections';
const RELATION_OPTIONS = [
    {
        value: 'LOAD',
        label: 'Load (default)',
        description: 'Ingest data as-is from the source into the platform or a destination.'
    },
    {
        value: 'MERGE',
        label: 'Merge',
        description: 'Blend records from multiple feeds, resolving overlaps into a unified dataset.'
    },
    {
        value: 'LOOKUP',
        label: 'Lookup',
        description: 'Treat the source as a reference table that enriches other datasets during ingestion.'
    },
    {
        value: 'SYNC',
        label: 'Sync',
        description: 'Continuously synchronize changes between the source and destination systems.'
    },
    {
        value: 'TRANSFORM',
        label: 'Transform',
        description: 'Apply calculated mappings or cleansing routines before persisting the records.'
    }
];

let availableSources = [];
let availableDestinations = [];
let currentConnections = [];

const sourceSelect = document.getElementById('connectionSource');
const destinationSelect = document.getElementById('connectionDestination');
const relationSelect = document.getElementById('connectionRelation');
const relationDescriptionsList = document.getElementById('relationDescriptions');
const relationToggleButton = document.getElementById('relationToggle');

let showAllRelationDescriptions = false;

const tableSelectionOverlay = document.getElementById('tableSelectionOverlay');
const tableSelectionContent = document.getElementById('tableSelectionContent');
const tableSelectionStatus = document.getElementById('tableSelectionStatus');
const confirmTableSelectionBtn = document.getElementById('confirmTableSelection');
const cancelTableSelectionBtn = document.getElementById('cancelTableSelection');
const closeTableSelectionBtn = document.getElementById('closeTableSelection');

let currentTableSchema = [];
let pendingConnection = null;
let schemaCollapseState = {};
let stageTwoMode = false;

initializeRelationField();
setupConnectionListeners();
loadConnectorOptions();
loadConnections();

function setupConnectionListeners() {
    document.getElementById('logoutBtn').addEventListener('click', () => {
        clearToken();
        window.location.href = '/';
    });

    document.getElementById('refreshConnections').addEventListener('click', () => {
        loadConnectorOptions();
        loadConnections();
    });

    document.getElementById('connectionForm').addEventListener('submit', handleCreateConnection);

    if (confirmTableSelectionBtn) {
        confirmTableSelectionBtn.addEventListener('click', handleConfirmTableSelection);
    }
    if (cancelTableSelectionBtn) {
        cancelTableSelectionBtn.addEventListener('click', closeTableSelectionModal);
    }
    if (closeTableSelectionBtn) {
        closeTableSelectionBtn.addEventListener('click', closeTableSelectionModal);
    }
    if (tableSelectionOverlay) {
        tableSelectionOverlay.addEventListener('click', (event) => {
            if (event.target === tableSelectionOverlay) {
                closeTableSelectionModal();
            }
        });
    }
    if (tableSelectionContent) {
        tableSelectionContent.addEventListener('change', handleTableSelectionChange);
        tableSelectionContent.addEventListener('click', handleTableSelectionClick);
    }
}

async function loadConnectorOptions() {
    try {
        availableSources = await api.get(`${SOURCES_API_BASE}?role=SOURCE`);
        renderSourceOptions();
    } catch (error) {
        toast.error('Failed to load sources');
        sourceSelect.innerHTML = '<option value="">Unable to load sources</option>';
    }

    try {
        availableDestinations = await api.get(`${SOURCES_API_BASE}?role=DESTINATION`);
        renderDestinationOptions();
    } catch (error) {
        toast.error('Failed to load destinations');
        destinationSelect.innerHTML = '<option value="">Unable to load destinations</option>';
    }
}

function renderSourceOptions() {
    if (!Array.isArray(availableSources) || availableSources.length === 0) {
        sourceSelect.innerHTML = '<option value="">No sources available</option>';
        return;
    }
    sourceSelect.innerHTML = ['<option value="">Select source...</option>']
        .concat(availableSources.map(source =>
            `<option value="${source.id}">${formatConnectorOptionLabel(source)}</option>`
        ))
        .join('');
}

function renderDestinationOptions() {
    if (!Array.isArray(availableDestinations) || availableDestinations.length === 0) {
        destinationSelect.innerHTML = '<option value="">No destinations (optional)</option>';
        return;
    }
    destinationSelect.innerHTML = ['<option value="">No destination</option>']
        .concat(availableDestinations.map(destination =>
            `<option value="${destination.id}">${formatConnectorOptionLabel(destination)}</option>`
        ))
        .join('');
}

function initializeRelationField() {
    if (!relationSelect) {
        return;
    }
    relationSelect.innerHTML = RELATION_OPTIONS.map(option => `<option value="${option.value}">${escapeHtml(option.label)}</option>`).join('');
    relationSelect.value = RELATION_OPTIONS[0]?.value || '';
    renderRelationDescriptions();

    if (relationToggleButton) {
        relationToggleButton.addEventListener('click', toggleRelationDescriptions);
        relationToggleButton.classList.toggle('hidden', RELATION_OPTIONS.length <= 2);
    }
}

function renderRelationDescriptions() {
    if (!relationDescriptionsList) {
        return;
    }
    const itemsToShow = showAllRelationDescriptions ? RELATION_OPTIONS : RELATION_OPTIONS.slice(0, 2);
    relationDescriptionsList.innerHTML = itemsToShow.map(option => `
        <li class="relation-help-item">
            <strong>${escapeHtml(option.label)}</strong>
            <span>${escapeHtml(option.description)}</span>
        </li>
    `).join('');

    if (relationToggleButton) {
        const remaining = Math.max(RELATION_OPTIONS.length - 2, 0);
        relationToggleButton.textContent = showAllRelationDescriptions
            ? 'Show fewer relation types'
            : `Show all relation types${remaining ? ` (+${remaining} more)` : ''}`;
        relationToggleButton.setAttribute('aria-expanded', showAllRelationDescriptions.toString());
    }
}

function toggleRelationDescriptions() {
    showAllRelationDescriptions = !showAllRelationDescriptions;
    renderRelationDescriptions();
}

async function handleCreateConnection(event) {
    event.preventDefault();

    const sourceId = sourceSelect.value;
    const destinationId = destinationSelect.value;
    const relationValue = relationSelect ? relationSelect.value : '';

    if (!sourceId) {
        toast.error('Source is required');
        return;
    }

    const selectedSource = Array.isArray(availableSources)
        ? availableSources.find((src) => src && (src.id === sourceId || String(src.id) === String(sourceId)))
        : null;

    const payload = {
        sourceId,
        destinationId: destinationId || null,
        relation: relationValue || 'LOAD'
    };

    const submitButton = document.querySelector('#connectionForm button[type="submit"]');
    if (submitButton) {
        submitButton.disabled = true;
        submitButton.innerHTML = '<span class="spinner"></span> Saving...';
    }

    try {
        const response = await api.post(CONNECTIONS_API_BASE, payload);
        toast.success('Connection saved');

        const isDbSource = selectedSource && String(selectedSource.type).toUpperCase() === 'DB';
        if (isDbSource && response && response.id) {
            pendingConnection = {
                connectionId: response.id,
                sourceId,
                destinationId: destinationId || null,
                relation: relationValue || 'LOAD',
                selectedSource
            };
            stageTwoMode = true;
            openTableSelectionModal(selectedSource, true);
        } else {
            loadConnections();
        }
    } catch (error) {
        toast.error(error.message || 'Failed to create connection');
    } finally {
        if (submitButton) {
            submitButton.disabled = false;
            submitButton.textContent = 'Start Connection';
        }
        document.getElementById('connectionForm').reset();
    }
}

async function loadConnections() {
    const container = document.getElementById('connectionsTable');
    container.innerHTML = `
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
    `;

    try {
        currentConnections = await api.get(CONNECTIONS_API_BASE);
        renderConnections();
    } catch (error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <p>Unable to load connections</p>
                <button class="btn btn-primary btn-sm" onclick="loadConnections()">Retry</button>
            </div>
        `;
    }
}

function renderConnections() {
    const container = document.getElementById('connectionsTable');

    if (!Array.isArray(currentConnections) || currentConnections.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🔗</div>
                <p>No connections yet</p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>Source</th>
                    <th>Destination</th>
                    <th>Relation</th>
                    <th>Selection</th>
                    <th>Created By</th>
                    <th>Created At</th>
                </tr>
            </thead>
            <tbody>
                ${currentConnections.map(renderConnectionRow).join('')}
            </tbody>
        </table>
    `;
}

function openTableSelectionModal(selectedSource, isSecondStage = false) {
    if (!tableSelectionOverlay) {
        return;
    }
    tableSelectionOverlay.classList.remove('hidden');
    currentTableSchema = [];
    schemaCollapseState = {};
    renderTableSelectionLoading();
    stageTwoMode = Boolean(isSecondStage);
    if (tableSelectionStatus) {
        tableSelectionStatus.textContent = stageTwoMode
            ? 'Connection established. Pick the tables and columns to ingest next.'
            : 'Loading schema...';
    }
    if (confirmTableSelectionBtn) {
        confirmTableSelectionBtn.disabled = true;
        confirmTableSelectionBtn.innerHTML = '<span class="spinner"></span>';
    }
    const sourceId = pendingConnection ? pendingConnection.sourceId : null;
    loadTableSchema(sourceId, selectedSource);
}

async function loadTableSchema(sourceId, selectedSource) {
    if (!sourceId) {
        renderTableSelection([]);
        finalizeSchemaLoad();
        return;
    }
    try {
        let schema = [];
        const source = selectedSource || (Array.isArray(availableSources)
            ? availableSources.find((item) => item && (item.id === sourceId || String(item.id) === String(sourceId)))
            : null);

        if (source && String(source.type).toUpperCase() === 'DB') {
            schema = await api.post(`${SOURCES_API_BASE}/discover-schema`, {
                type: 'DB',
                config: source.config || {}
            });
        } else {
            schema = await api.get(`${SOURCES_API_BASE}/${sourceId}/schema`);
        }
        currentTableSchema = Array.isArray(schema) ? schema : [];
        renderTableSelection(currentTableSchema);
        if (tableSelectionStatus) {
            if (currentTableSchema.length === 0) {
                tableSelectionStatus.textContent = 'No tables detected. The connection will ingest all available data.';
            } else {
                tableSelectionStatus.textContent = 'Select the tables and columns you want to ingest.';
            }
        }
    } catch (error) {
        console.error('Failed to load schema', error);
        currentTableSchema = [];
        renderTableSelection([]);
        if (tableSelectionStatus) {
            tableSelectionStatus.textContent = 'Unable to load schema. You can continue with the default ingestion.';
        }
        toast.error(error.message || 'Failed to load available tables');
    } finally {
        finalizeSchemaLoad();
    }
}

function finalizeSchemaLoad() {
    if (confirmTableSelectionBtn) {
        confirmTableSelectionBtn.disabled = false;
        confirmTableSelectionBtn.textContent = stageTwoMode ? 'Save Selection' : 'Start Connection';
    }
}

function renderTableSelectionLoading() {
    if (!tableSelectionContent) {
        return;
    }
    tableSelectionContent.innerHTML = `
        <div class="table-selection-item" style="text-align: center;">
            <span class="spinner"></span>
        </div>
    `;
}

function renderTableSelection(schema) {
    if (!tableSelectionContent) {
        return;
    }
    if (!Array.isArray(schema) || schema.length === 0) {
        tableSelectionContent.innerHTML = `
            <div class="table-selection-item text-muted" style="text-align: center;">
                No table metadata available for this source.
            </div>
        `;
        return;
    }

    const grouped = schema.reduce((acc, table, index) => {
        const schemaName = table && table.schema ? table.schema : 'Default schema';
        if (!acc[schemaName]) {
            acc[schemaName] = [];
        }
        acc[schemaName].push({ table, index });
        return acc;
    }, {});

    tableSelectionContent.innerHTML = Object.entries(grouped).map(([schemaName, tables]) => {
        const schemaKey = schemaName;
        const schemaKeyAttr = escapeHtml(schemaKey);
        const isCollapsed = Boolean(schemaCollapseState[schemaKey]);
        const tableCount = tables.length;
        const schemaLabel = escapeHtml(schemaName);

        const tablesMarkup = tables.map(({ table, index }) => {
            const tableNameRaw = table && (table.tableName || table.name) ? (table.tableName || table.name) : `Table ${index + 1}`;
            const columns = Array.isArray(table && table.columns) ? table.columns : [];
            const columnMarkup = columns.length > 0
                ? columns.map(column => {
                    const columnNameRaw = column && (column.name || column.columnName) ? (column.name || column.columnName) : 'column';
                    const columnTypeRaw = column && (column.dataType || column.type) ? (column.dataType || column.type) : '';
                    const columnName = escapeHtml(columnNameRaw);
                    const columnType = columnTypeRaw ? escapeHtml(columnTypeRaw) : '';
                    return `
                        <label class="column-option">
                            <input type="checkbox" class="column-checkbox" data-table-index="${index}" data-schema="${schemaKeyAttr}" value="${columnName}" checked>
                            <span>${columnName}${columnType ? ` <span class="column-type">${columnType}</span>` : ''}</span>
                        </label>
                    `;
                }).join('')
                : '<span class="text-muted">No column metadata available</span>';

            return `
                <div class="table-selection-item">
                    <div class="table-selection-header">
                        <label class="table-name">
                            <input type="checkbox" class="table-checkbox" data-table-index="${index}" data-schema="${schemaKeyAttr}" checked>
                            <span>${escapeHtml(tableNameRaw)}</span>
                        </label>
                        <span class="column-count">${columns.length} column${columns.length === 1 ? '' : 's'}</span>
                    </div>
                    <div class="column-list" data-columns-for="${index}">
                        ${columnMarkup}
                    </div>
                </div>
            `;
        }).join('');

        return `
            <div class="schema-group" data-schema="${schemaKeyAttr}">
                <div class="schema-header">
                    <button type="button" class="schema-toggle" data-schema="${schemaKeyAttr}" aria-expanded="${(!isCollapsed).toString()}">
                        <span class="toggle-icon">${isCollapsed ? '▶' : '▼'}</span>
                    </button>
                    <label class="schema-label">
                        <input type="checkbox" class="schema-checkbox" data-schema="${schemaKeyAttr}" checked>
                        <span>${schemaLabel}</span>
                    </label>
                    <span class="schema-count">${tableCount} table${tableCount === 1 ? '' : 's'}</span>
                </div>
                <div class="schema-body ${isCollapsed ? 'collapsed' : ''}" data-schema-body="${schemaKeyAttr}">
                    ${tablesMarkup}
                </div>
            </div>
        `;
    }).join('');

    synchronizeSelectionStates();
}

function collectSelectedTables() {
    if (!Array.isArray(currentTableSchema) || currentTableSchema.length === 0) {
        return [];
    }
    const selections = [];
    currentTableSchema.forEach((table, index) => {
        const columns = Array.from(document.querySelectorAll(`.column-checkbox[data-table-index="${index}"]`))
            .filter(checkbox => checkbox.checked)
            .map(checkbox => checkbox.value)
            .filter(value => value && value.trim().length > 0);
        selections.push({
            tableName: (table && (table.tableName || table.name)) || `Table ${index + 1}`,
            schema: table && table.schema ? table.schema : null,
            columns
        });
    });
    return selections;
}

function handleTableSelectionClick(event) {
    const toggleButton = event.target.closest('.schema-toggle');
    if (toggleButton) {
        const schemaKey = toggleButton.dataset.schema;
        const isCollapsed = !schemaCollapseState[schemaKey];
        schemaCollapseState[schemaKey] = isCollapsed;
        const body = tableSelectionContent.querySelector(`[data-schema-body="${escapeSelectorValue(schemaKey)}"]`);
        if (body) {
            body.classList.toggle('collapsed', isCollapsed);
        }
        toggleButton.setAttribute('aria-expanded', (!isCollapsed).toString());
        const icon = toggleButton.querySelector('.toggle-icon');
        if (icon) {
            icon.textContent = isCollapsed ? '▶' : '▼';
        }
    }
}

function handleTableSelectionChange(event) {
    const target = event.target;
    if (!target || !(target instanceof HTMLInputElement)) {
        return;
    }

    if (target.classList.contains('schema-checkbox')) {
        toggleSchemaSelection(target.dataset.schema, target.checked);
    } else if (target.classList.contains('table-checkbox')) {
        toggleTableSelection(Number(target.dataset.tableIndex), target.checked, target.dataset.schema);
    } else if (target.classList.contains('column-checkbox')) {
        updateTableCheckboxState(Number(target.dataset.tableIndex));
        updateSchemaCheckboxState(target.dataset.schema);
    }
}

function toggleSchemaSelection(schemaKey, isChecked) {
    const schemaSelector = escapeSelectorValue(schemaKey);
    const tableCheckboxes = tableSelectionContent.querySelectorAll(`.table-checkbox[data-schema="${schemaSelector}"]`);
    tableCheckboxes.forEach(checkbox => {
        checkbox.checked = isChecked;
        toggleTableSelection(Number(checkbox.dataset.tableIndex), isChecked, schemaKey, false);
    });
    updateSchemaCheckboxState(schemaKey);
}

function toggleTableSelection(tableIndex, isChecked, schemaKey, syncSchema = true) {
    const columnCheckboxes = tableSelectionContent.querySelectorAll(`.column-checkbox[data-table-index="${tableIndex}"]`);
    columnCheckboxes.forEach(columnCheckbox => {
        columnCheckbox.checked = isChecked;
    });
    updateTableCheckboxState(tableIndex);
    if (syncSchema) {
        updateSchemaCheckboxState(schemaKey);
    }
}

function updateTableCheckboxState(tableIndex) {
    const tableCheckbox = tableSelectionContent.querySelector(`.table-checkbox[data-table-index="${tableIndex}"]`);
    const columnCheckboxes = Array.from(tableSelectionContent.querySelectorAll(`.column-checkbox[data-table-index="${tableIndex}"]`));
    if (!tableCheckbox || columnCheckboxes.length === 0) {
        return;
    }
    const allChecked = columnCheckboxes.every(checkbox => checkbox.checked);
    const anyChecked = columnCheckboxes.some(checkbox => checkbox.checked);
    tableCheckbox.checked = allChecked;
    tableCheckbox.indeterminate = !allChecked && anyChecked;
}

function updateSchemaCheckboxState(schemaKey) {
    const schemaSelector = escapeSelectorValue(schemaKey);
    const schemaCheckbox = tableSelectionContent.querySelector(`.schema-checkbox[data-schema="${schemaSelector}"]`);
    const tableCheckboxes = Array.from(tableSelectionContent.querySelectorAll(`.table-checkbox[data-schema="${schemaSelector}"]`));
    if (!schemaCheckbox || tableCheckboxes.length === 0) {
        return;
    }
    const allChecked = tableCheckboxes.every(checkbox => checkbox.checked);
    const anyChecked = tableCheckboxes.some(checkbox => checkbox.checked || checkbox.indeterminate);
    schemaCheckbox.checked = allChecked;
    schemaCheckbox.indeterminate = !allChecked && anyChecked;
}

function synchronizeSelectionStates() {
    const schemaCheckboxes = Array.from(tableSelectionContent.querySelectorAll('.schema-checkbox'));
    schemaCheckboxes.forEach(schemaCheckbox => {
        updateSchemaCheckboxState(schemaCheckbox.dataset.schema);
    });
    const tableCheckboxes = Array.from(tableSelectionContent.querySelectorAll('.table-checkbox'));
    tableCheckboxes.forEach(tableCheckbox => {
        updateTableCheckboxState(Number(tableCheckbox.dataset.tableIndex));
    });
}

function escapeSelectorValue(value) {
    if (typeof CSS !== 'undefined' && CSS.escape) {
        return CSS.escape(value || '');
    }
    return (value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

async function handleConfirmTableSelection() {
    if (!pendingConnection) {
        closeTableSelectionModal();
        return;
    }

    const selections = collectSelectedTables();
    const destinationIdForDownload = pendingConnection.destinationId;

    if (confirmTableSelectionBtn) {
        confirmTableSelectionBtn.disabled = true;
        confirmTableSelectionBtn.innerHTML = '<span class="spinner"></span> Saving...';
    }

    try {
        if (pendingConnection.connectionId) {
            await api.post(`${CONNECTIONS_API_BASE}/${pendingConnection.connectionId}/table-selection`, {
                tableSelections: selections
            });
            toast.success('Table selection saved and ingestion started');
        } else {
            await api.post(CONNECTIONS_API_BASE, {
                sourceId: pendingConnection.sourceId,
                destinationId: pendingConnection.destinationId,
                relation: pendingConnection.relation,
                tableSelections: selections
            });
            toast.success('Connection started');
        }
        closeTableSelectionModal(true);
        loadConnections();
        await maybeDownloadDestinationFile(destinationIdForDownload);
    } catch (error) {
        toast.error(error.message || 'Failed to start connection');
        if (confirmTableSelectionBtn) {
            confirmTableSelectionBtn.disabled = false;
            confirmTableSelectionBtn.textContent = stageTwoMode ? 'Save Selection' : 'Start Connection';
        }
    }
}

function closeTableSelectionModal(resetPending = true) {
    if (tableSelectionOverlay) {
        tableSelectionOverlay.classList.add('hidden');
    }
    if (confirmTableSelectionBtn) {
        confirmTableSelectionBtn.disabled = false;
        confirmTableSelectionBtn.textContent = stageTwoMode ? 'Save Selection' : 'Start Connection';
    }
    if (tableSelectionStatus) {
        tableSelectionStatus.textContent = 'Choose the tables and columns you want to ingest from this source.';
    }
    if (tableSelectionContent) {
        tableSelectionContent.innerHTML = '';
    }
    currentTableSchema = [];
    if (resetPending) {
        pendingConnection = null;
    }
    stageTwoMode = false;
}

function renderConnectionRow(connection) {
    return `
        <tr>
            <td>${escapeHtml(connection.sourceName || connection.sourceId || 'N/A')}</td>
            <td>${escapeHtml(connection.destinationName || connection.destinationId || 'N/A')}</td>
            <td>${escapeHtml(connection.relation || 'LOAD')}</td>
            <td>${formatTableSelections(connection.tableSelections)}</td>
            <td>${escapeHtml(connection.createdBy || 'system')}</td>
            <td>${formatTimestamp(connection.createdAt)}</td>
        </tr>
    `;
}

function formatTableSelections(selections) {
    if (!Array.isArray(selections) || selections.length === 0) {
        return '<span class="text-muted">All tables</span>';
    }
    return selections.map(selection => {
        const tableNameRaw = selection && (selection.tableName || selection.table) ? (selection.tableName || selection.table) : 'N/A';
        const schema = selection && selection.schema ? selection.schema : null;
        const qualified = schema ? `${escapeHtml(schema)}.${escapeHtml(tableNameRaw)}` : escapeHtml(tableNameRaw);
        const columns = selection && Array.isArray(selection.columns) ? selection.columns : [];
        const columnsContent = columns.length > 0
            ? `<div class="connection-table-columns">${columns.map(column => escapeHtml(column)).join(', ')}</div>`
            : '<div class="connection-table-columns text-muted">All columns</div>';
        return `
            <div class="connection-table-selection">
                <div class="connection-table-name">${qualified}</div>
                ${columnsContent}
            </div>
        `;
    }).join('');
}

function formatTimestamp(value) {
    if (!value) {
        return '<span style="color: var(--text-secondary);">N/A</span>';
    }
    try {
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            return '<span style="color: var(--text-secondary);">N/A</span>';
        }
        return `<span style="color: var(--text-secondary);">${date.toLocaleString()}</span>`;
    } catch (error) {
        return '<span style="color: var(--text-secondary);">N/A</span>';
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text ?? '';
    return div.innerHTML;
}

function formatConnectorOptionLabel(connector) {
    const name = escapeHtml(connector && connector.name ? connector.name : 'Unnamed');
    const rawType = connector && connector.type ? connector.type : '';
    const type = rawType ? escapeHtml(String(rawType).replace(/_/g, ' ')) : 'Unknown type';
    return `${name} · ${type}`;
}

async function maybeDownloadDestinationFile(destinationId) {
    if (!destinationId) {
        return;
    }

    const destination = Array.isArray(availableDestinations)
        ? availableDestinations.find(item => item && item.id === destinationId)
        : null;

    if (!destination) {
        return;
    }

    const type = (destination.type || '').toUpperCase();
    if (type !== 'CSV' && type !== 'JSON') {
        return;
    }

    try {
        const endpoint = `${SOURCES_API_BASE}/${encodeURIComponent(destination.id)}/download`;
        const blob = await api.download(endpoint);
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = buildDestinationFilename(destination, type);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Failed to download destination file', error);
        toast.error('Destination file download failed');
    }
}

function buildDestinationFilename(destination, type) {
    const configuredName = destination && destination.config && typeof destination.config.displayFilename === 'string'
        ? destination.config.displayFilename.trim()
        : '';

    if (configuredName) {
        return sanitizeFilename(configuredName);
    }

    const baseName = destination && typeof destination.name === 'string' && destination.name.trim()
        ? destination.name.trim()
        : 'destination-output';
    return `${sanitizeFilename(baseName)}.${type.toLowerCase()}`;
}

function sanitizeFilename(name) {
    return name.replace(/[\\/:*?"<>|]/g, '-');
}
