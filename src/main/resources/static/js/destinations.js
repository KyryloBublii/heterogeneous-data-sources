if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

const DESTINATION_ROLE = 'DESTINATION';
const SOURCES_API_BASE = '/api/sources';

let currentDestinations = [];
let destinationStatusPolling = null;

const destinationTypeSelect = document.getElementById('destinationType');
const destinationDbConfig = document.getElementById('destinationDbConfig');
const destinationFileConfig = document.getElementById('destinationFileConfig');
const destDbTestButton = document.getElementById('testDestinationConnection');
const destDbTestResult = document.getElementById('destDbTestResult');

setupDestinationListeners();
loadDestinations();

function setupDestinationListeners() {
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

    document.getElementById('addDestinationBtn').addEventListener('click', () => {
        document.getElementById('addDestinationForm').reset();
        destinationDbConfig.style.display = 'none';
        destinationFileConfig.style.display = 'none';
        if (destDbTestResult) {
            destDbTestResult.textContent = '';
        }
        document.getElementById('addDestinationModal').style.display = 'flex';
    });

    document.getElementById('refreshBtn').addEventListener('click', loadDestinations);
    document.getElementById('addDestinationForm').addEventListener('submit', handleCreateDestination);

    if (destinationTypeSelect) {
        destinationTypeSelect.addEventListener('change', handleDestinationTypeChange);
    }

    if (destDbTestButton) {
        destDbTestButton.addEventListener('click', async () => {
            try {
                const config = buildDestinationDatabaseConfig();
                await api.post(`${SOURCES_API_BASE}/test-connection`, {
                    type: 'DB',
                    config
                });
                destDbTestResult.textContent = 'Connection successful.';
                destDbTestResult.style.color = 'var(--success-color)';
            } catch (error) {
                destDbTestResult.textContent = error.message || 'Connection failed.';
                destDbTestResult.style.color = 'var(--error-color)';
            }
        });
    }
}

function handleDestinationTypeChange(event) {
    const value = event.target.value;
    destinationDbConfig.style.display = value === 'DB' ? 'block' : 'none';
    destinationFileConfig.style.display = value === 'CSV' || value === 'JSON' ? 'block' : 'none';
}

async function handleCreateDestination(event) {
    event.preventDefault();

    const name = document.getElementById('destinationName').value.trim();
    const type = document.getElementById('destinationType').value;
    const additionalConfigText = document.getElementById('destinationConfig').value.trim();
    const fileNameInput = document.getElementById('destinationFileName');
    const fileName = fileNameInput ? fileNameInput.value.trim() : '';

    if (!name) {
        toast.error('Destination name is required');
        return;
    }
    if (!type) {
        toast.error('Destination type is required');
        return;
    }

    let additionalConfig = {};
    if (additionalConfigText) {
        try {
            additionalConfig = JSON.parse(additionalConfigText);
        } catch (error) {
            toast.error('Invalid JSON in additional configuration');
            return;
        }
    }

    let config = {};
    try {
        if (type === 'DB') {
            config = buildDestinationDatabaseConfig();
        } else if (type === 'CSV' || type === 'JSON') {
            if (fileName) {
                config.fileName = fileName;
            }
        }
    } catch (error) {
        toast.error(error.message || 'Invalid configuration');
        return;
    }

    config = { ...config, ...additionalConfig };

    const submitBtn = event.target.querySelector('button[type="submit"]');
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="spinner"></span> Creating...';

    try {
        await api.post(SOURCES_API_BASE, {
            name,
            type,
            role: DESTINATION_ROLE,
            config
        });
        toast.success('Destination created successfully');
        closeAddDestinationModal();
        loadDestinations();
    } catch (error) {
        toast.error(error.message || 'Failed to create destination');
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Create Destination';
    }
}

function buildDestinationDatabaseConfig() {
    const host = document.getElementById('destDbHost').value.trim();
    const port = document.getElementById('destDbPort').value.trim();
    const database = document.getElementById('destDbName').value.trim();
    const username = document.getElementById('destDbUser').value.trim();
    const password = document.getElementById('destDbPassword').value;

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

function closeAddDestinationModal() {
    document.getElementById('addDestinationModal').style.display = 'none';
    document.getElementById('addDestinationForm').reset();
    destinationDbConfig.style.display = 'none';
    destinationFileConfig.style.display = 'none';
    if (destDbTestResult) {
        destDbTestResult.textContent = '';
    }
}

function closeDestinationStatusModal() {
    document.getElementById('destinationStatusModal').style.display = 'none';
    if (destinationStatusPolling) {
        clearInterval(destinationStatusPolling);
        destinationStatusPolling = null;
    }
}

async function loadDestinations() {
    const container = document.getElementById('destinationsTable');
    container.innerHTML = `
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
    `;

    try {
        currentDestinations = await api.get(`${SOURCES_API_BASE}?role=${DESTINATION_ROLE}`);
        renderDestinations();
    } catch (error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <p>Unable to load destinations</p>
                <button class="btn btn-primary btn-sm" onclick="loadDestinations()">Retry</button>
            </div>
        `;
    }
}

function renderDestinations() {
    const container = document.getElementById('destinationsTable');

    if (!Array.isArray(currentDestinations) || currentDestinations.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🎯</div>
                <p>No destinations configured yet</p>
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
                ${currentDestinations.map(renderDestinationRow).join('')}
            </tbody>
        </table>
    `;
}

function renderDestinationRow(destination) {
    const encodedName = encodeURIComponent(destination.name || '');
    return `
        <tr>
            <td><strong>${escapeHtml(destination.name)}</strong></td>
            <td><span class="badge badge-info">${escapeHtml(destination.type)}</span></td>
            <td>${getStatusBadge(destination.status)}</td>
            <td>${formatLastSync(destination)}</td>
            <td class="actions-cell">
                <div class="action-menu">
                    <button class="btn-icon" type="button" onclick="toggleActionMenu(event)">
                        <span aria-hidden="true">⋯</span>
                        <span class="sr-only">Open actions for ${escapeHtml(destination.name)}</span>
                    </button>
                    <div class="action-dropdown">
                        <button class="action-item" type="button" onclick="viewDestinationStatus('${destination.id}')">Status</button>
                        <button class="action-item" type="button" onclick="ingestDestination('${destination.id}')">Start</button>
                        <button class="action-item action-danger" type="button" onclick="deleteDestination('${destination.id}', '${encodedName}')">Delete</button>
                    </div>
                </div>
            </td>
        </tr>
    `;
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

function formatLastSync(destination) {
    if (!destination || !destination.updatedAt) {
        return '<span style="color: var(--text-secondary);">Never</span>';
    }
    try {
        const date = new Date(destination.updatedAt);
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

async function ingestDestination(destinationId) {
    closeAllActionMenus();
    try {
        await api.post(`${SOURCES_API_BASE}/${destinationId}/refresh`);
        toast.success('Ingestion started');
        loadDestinations();
    } catch (error) {
        toast.error(error.message || 'Failed to start ingestion');
    }
}

async function viewDestinationStatus(destinationId) {
    closeAllActionMenus();
    document.getElementById('destinationStatusModal').style.display = 'flex';
    const statusContent = document.getElementById('destinationStatusContent');
    statusContent.innerHTML = `
        <div class="skeleton-text skeleton"></div>
        <div class="skeleton-text skeleton"></div>
        <div class="skeleton-text skeleton"></div>
    `;

    try {
        const runs = await api.get(`${SOURCES_API_BASE}/${destinationId}/runs`);
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

async function deleteDestination(destinationId, encodedName = '') {
    closeAllActionMenus();
    const destinationName = decodeURIComponent(encodedName || '');
    const label = destinationName ? `"${destinationName}"` : 'this destination';
    const confirmed = window.confirm(`Are you sure you want to delete ${label}? This action cannot be undone.`);
    if (!confirmed) {
        return;
    }

    try {
        await api.delete(`${SOURCES_API_BASE}/${destinationId}`);
        toast.success('Destination deleted');
        loadDestinations();
    } catch (error) {
        toast.error(error.message || 'Failed to delete destination');
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
