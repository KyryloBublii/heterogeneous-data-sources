if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

const DATASETS_API_BASE = '/api/datasets';
let currentDatasets = [];
let editingDatasetId = null;

const datasetsTable = document.getElementById('datasetsTable');
const addDatasetModal = document.getElementById('addDatasetModal');

setupEventListeners();
loadDatasets();

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

    document.getElementById('addDatasetBtn').addEventListener('click', () => openDatasetModal());

    document.getElementById('refreshDatasets').addEventListener('click', loadDatasets);
    document.getElementById('addDatasetForm').addEventListener('submit', handleSaveDataset);
}

function closeAddDatasetModal() {
    addDatasetModal.style.display = 'none';
    document.getElementById('addDatasetForm').reset();
    editingDatasetId = null;
    document.querySelector('#addDatasetModal h2').textContent = 'Create Dataset';
    document.querySelector('#addDatasetModal button[type="submit"]').textContent = 'Create Dataset';
}

function openDatasetModal(dataset) {
    const titleEl = document.querySelector('#addDatasetModal h2');
    const submitBtn = document.querySelector('#addDatasetModal button[type="submit"]');
    const nameInput = document.getElementById('datasetName');
    const descInput = document.getElementById('datasetDescription');

    if (dataset) {
        editingDatasetId = dataset.id;
        nameInput.value = dataset.name || '';
        descInput.value = dataset.description || '';
        titleEl.textContent = 'Edit Dataset';
        submitBtn.textContent = 'Save Changes';
    } else {
        editingDatasetId = null;
        document.getElementById('addDatasetForm').reset();
        titleEl.textContent = 'Create Dataset';
        submitBtn.textContent = 'Create Dataset';
    }

    addDatasetModal.style.display = 'flex';
}

async function loadDatasets() {
    datasetsTable.innerHTML = `
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
    `;

    try {
        currentDatasets = await api.get(DATASETS_API_BASE);
        renderDatasets();
    } catch (error) {
        datasetsTable.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <p>Unable to load datasets</p>
                <button class="btn btn-primary btn-sm" onclick="loadDatasets()">Retry</button>
            </div>
        `;
    }
}

function renderDatasets() {
    if (!Array.isArray(currentDatasets) || currentDatasets.length === 0) {
        datasetsTable.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🗂️</div>
                <p>No datasets created yet</p>
            </div>
        `;
        return;
    }

    datasetsTable.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Records</th>
                    <th>Last Updated</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                ${currentDatasets.map(renderDatasetRow).join('')}
            </tbody>
        </table>
    `;

    enableDatasetRowNavigation();
}

function renderDatasetRow(dataset) {
    const recordCount = Number(dataset.recordCount || 0).toLocaleString();
    const updated = dataset.lastUpdated ? formatDate(dataset.lastUpdated) : 'Not available';
    const encodedName = encodeURIComponent(dataset.name || '');
    const status = dataset.status ? dataset.status.toString() : 'ACTIVE';
    const statusLabel = status.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

    return `
        <tr class="dataset-row" data-dataset-id="${dataset.id}" tabindex="0">
            <td>
                <strong>${escapeHtml(dataset.name)}</strong>
                <div class="field-hint">${escapeHtml(dataset.description || 'Managed dataset')}</div>
            </td>
            <td>${escapeHtml(statusLabel)}</td>
            <td>${recordCount}</td>
            <td>${updated}</td>
            <td class="actions-cell">
                <div class="action-menu">
                    <button class="btn-icon" type="button" onclick="toggleActionMenu(event)">
                        <span aria-hidden="true">⋯</span>
                        <span class="sr-only">Open actions for ${escapeHtml(dataset.name)}</span>
                    </button>
                    <div class="action-dropdown">
                        <button class="action-item" type="button" onclick="startEditDataset(${dataset.id}, event)">Edit</button>
                        <button class="action-item action-danger" type="button" onclick="deleteDataset(${dataset.id}, '${encodedName}', event)">Delete</button>
                    </div>
                </div>
            </td>
        </tr>
    `;
}

function enableDatasetRowNavigation() {
    const rows = datasetsTable.querySelectorAll('.dataset-row');

    rows.forEach(row => {
        const datasetId = row.getAttribute('data-dataset-id');
        if (!datasetId) {
            return;
        }

        row.addEventListener('click', () => openDataset(datasetId));
        row.addEventListener('keydown', event => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                openDataset(datasetId);
            }
        });
    });
}

async function handleSaveDataset(event) {
    event.preventDefault();

    const name = document.getElementById('datasetName').value.trim();
    const description = document.getElementById('datasetDescription').value.trim();
    const submitBtn = event.target.querySelector('button[type="submit"]');

    if (!name) {
        toast.error('Dataset name is required');
        return;
    }

    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="spinner"></span> Saving...';

    try {
        if (editingDatasetId) {
            await api.put(`${DATASETS_API_BASE}/${editingDatasetId}`, { name, description });
            toast.success('Dataset updated successfully');
        } else {
            await api.post(DATASETS_API_BASE, { name, description });
            toast.success('Dataset created successfully');
        }
        closeAddDatasetModal();
        await loadDatasets();
    } catch (error) {
        const defaultMessage = editingDatasetId ? 'Failed to update dataset' : 'Failed to create dataset';
        toast.error(error.message || defaultMessage);
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = editingDatasetId ? 'Save Changes' : 'Create Dataset';
    }
}

function openDataset(datasetId, event) {
    event?.stopPropagation();
    window.location.href = `/dataset?datasetId=${datasetId}`;
}

function toggleActionMenu(event) {
    event.stopPropagation();
    const menu = event.currentTarget.closest('.action-menu');
    const isOpen = menu.classList.contains('open');
    closeAllActionMenus();
    if (!isOpen) {
        menu.classList.add('open');
    }
}

function closeAllActionMenus() {
    document.querySelectorAll('.action-menu.open').forEach(menu => menu.classList.remove('open'));
}

function startEditDataset(datasetId, event) {
    event?.stopPropagation();
    closeAllActionMenus();
    const dataset = currentDatasets.find(item => String(item.id) === String(datasetId));
    if (!dataset) {
        toast.error('Dataset not found');
        return;
    }
    openDatasetModal(dataset);
}

async function deleteDataset(datasetId, encodedName = '', event) {
    event?.stopPropagation();
    closeAllActionMenus();
    const datasetName = decodeURIComponent(encodedName || '');
    const label = datasetName ? `"${datasetName}"` : 'this dataset';
    if (!confirm(`Are you sure you want to delete ${label}? This cannot be undone.`)) {
        return;
    }

    try {
        await api.delete(`${DATASETS_API_BASE}/${datasetId}`);
        toast.success('Dataset deleted');
        await loadDatasets();
    } catch (error) {
        toast.error(error.message || 'Failed to delete dataset');
    }
}

function formatDate(isoDate) {
    try {
        return new Date(isoDate).toLocaleString();
    } catch (error) {
        return 'Not available';
    }
}

function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    return text
        .toString()
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}
