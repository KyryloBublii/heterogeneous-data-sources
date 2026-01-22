if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

let datasets = [];
let currentDataset = null;
let currentDatasetName = null;
let currentData = [];
let currentPage = 0;
let pageSize = 50;
let chart = null;
let fieldDisplayMap = {};
let fieldOrder = [];
let chartField = '';
let chartLimit = 5;

function buildDatasetFilename(name) {
    const base = name && name.trim() ? name.trim() : 'dataset';
    const safe = base.replace(/[^a-zA-Z0-9-_]+/g, '-');
    return `${safe}.csv`;
}

document.getElementById('logoutBtn').addEventListener('click', () => {
    clearToken();
    window.location.href = '/';
});

document.getElementById('datasetSelect').addEventListener('change', async (e) => {
    currentDataset = e.target.value;
    const selected = datasets.find(ds => String(ds.id) === currentDataset);
    currentDatasetName = selected?.name || null;

    if (currentDatasetName) {
        document.getElementById('applyFilterBtn').disabled = false;
        await loadDatasetFields();
        await loadData();
    } else {
        document.getElementById('applyFilterBtn').disabled = true;
        document.getElementById('exportBtn').disabled = true;
    }
});



document.getElementById('applyFilterBtn').addEventListener('click', applyFilter);

document.getElementById('exportBtn').addEventListener('click', exportData);

document.getElementById('chartField').addEventListener('change', (e) => {
    chartField = e.target.value;
    renderChart();
});

document.getElementById('chartLimit').addEventListener('change', (e) => {
    chartLimit = parseInt(e.target.value, 10) || 5;
    renderChart();
});

async function loadDatasets() {
    try {
        datasets = await api.get('/api/datasets');
        const select = document.getElementById('datasetSelect');

        if (datasets.length === 0) {
            select.innerHTML = '<option value="">No datasets available</option>';
            return;
        }

        select.innerHTML = '<option value="">Choose a dataset to analyze...</option>' +
            datasets.map(ds => `<option value="${escapeHtml(String(ds.id))}" data-name="${escapeHtml(ds.name)}">${escapeHtml(ds.name)}</option>`).join('');
    } catch (error) {
        toast.error('Failed to load datasets');
    }
}

async function loadDatasetFields() {
    fieldDisplayMap = {};
    fieldOrder = [];

    if (!currentDataset) return;

    try {
        const fields = await api.get(`/api/datasets/${currentDataset}/fields`);

        fields.forEach(field => {
            const displayName = field.name || field.uid || field.id;
            if (field.uid) fieldDisplayMap[field.uid] = displayName;
            if (field.id) fieldDisplayMap[String(field.id)] = displayName;
            if (field.name) fieldDisplayMap[field.name] = displayName;
        });

        fieldOrder = fields
            .filter(f => f.position !== null && f.position !== undefined)
            .sort((a, b) => (a.position ?? Number.MAX_VALUE) - (b.position ?? Number.MAX_VALUE))
            .map(f => String(f.uid || f.id || f.name));
    } catch (error) {
        console.error('Failed to load dataset fields', error);
    }
}

async function loadData() {
    if (!currentDataset) return;

    const container = document.getElementById('resultsTable');

    container.innerHTML = `
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
        <div class="skeleton-table-row skeleton"></div>
    `;

    try {
        let endpoint = `/data/${currentDataset}`;

        currentData = await api.get(endpoint);

        const fields = currentData.length > 0 ? getOrderedFields(Object.keys(currentData[0])) : [];

        updateChartFieldOptions(fields);
        document.getElementById('exportBtn').disabled = currentData.length === 0;
        document.getElementById('recordCount').textContent = `${currentData.length} records`;

        renderTable();
        renderChart();
    } catch (error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <p>Unable to load data</p>
                <button class="btn btn-primary btn-sm" onclick="loadData()">Retry</button>
            </div>
        `;
    }
}

function renderTable() {
    const container = document.getElementById('resultsTable');

    if (currentData.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <p>No data found</p>
            </div>
        `;
        return;
    }

    const fields = getOrderedFields(Object.keys(currentData[0]));
    const start = currentPage * pageSize;
    const end = Math.min(start + pageSize, currentData.length);
    const pageData = currentData.slice(start, end);

    container.innerHTML = `
        <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        ${fields.map(field => `<th>${escapeHtml(getFieldDisplayName(field))}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${pageData.map(row => `
                        <tr>
                            ${fields.map(field => `<td>${escapeHtml(String(row[field] || ''))}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;

    const totalPages = Math.ceil(currentData.length / pageSize);
    if (totalPages > 1) {
        document.getElementById('pagination').style.display = 'block';
        document.getElementById('prevBtn').disabled = currentPage === 0;
        document.getElementById('nextBtn').disabled = currentPage >= totalPages - 1;
        document.getElementById('pageInfo').textContent = `Page ${currentPage + 1} of ${totalPages}`;

        document.getElementById('prevBtn').onclick = () => {
            currentPage--;
            renderTable();
        };

        document.getElementById('nextBtn').onclick = () => {
            currentPage++;
            renderTable();
        };
    } else {
        document.getElementById('pagination').style.display = 'none';
    }
}

function renderChart() {
    if (!currentData || currentData.length === 0) {
        document.getElementById('chartSection').style.display = 'none';
        return;
    }

    const fields = getOrderedFields(Object.keys(currentData[0]));

    if (!chartField || !fields.includes(chartField)) {
        chartField = fields[0];
        document.getElementById('chartField').value = chartField;
    }

    const frequencyMap = new Map();
    currentData.forEach(row => {
        const rawValue = row[chartField];
        const label = rawValue === null || rawValue === undefined || rawValue === '' ? 'Empty' : String(rawValue);
        frequencyMap.set(label, (frequencyMap.get(label) || 0) + 1);
    });

    const sorted = Array.from(frequencyMap.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, chartLimit || 5);

    if (sorted.length === 0) {
        document.getElementById('chartSection').style.display = 'none';
        return;
    }

    const labels = sorted.map(entry => entry[0]);
    const values = sorted.map(entry => entry[1]);

    document.getElementById('chartSection').style.display = 'block';

    if (chart) {
        chart.destroy();
    }

    const ctx = document.getElementById('dataChart').getContext('2d');

    chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: `Frequency of ${getFieldDisplayName(chartField)}`,
                data: values,
                backgroundColor: 'rgba(79, 70, 229, 0.6)',
                borderColor: 'rgb(79, 70, 229)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    },
                    title: {
                        display: true,
                        text: 'Frequency'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: getFieldDisplayName(chartField)
                    }
                }
            }
        }
    });
}

function updateChartFieldOptions(fields) {
    const select = document.getElementById('chartField');
    if (!select) return;

    select.innerHTML = '<option value="">Select field</option>' +
        fields.map(field => `<option value="${escapeHtml(field)}">${escapeHtml(getFieldDisplayName(field))}</option>`).join('');

    const hasExisting = chartField && fields.includes(chartField);
    chartField = hasExisting ? chartField : fields[0] || '';
    select.value = chartField;

    const container = document.getElementById('chartFieldContainer');
    const limitContainer = document.getElementById('chartLimitContainer');
    container.style.display = fields.length > 0 ? 'block' : 'none';
    limitContainer.style.display = fields.length > 0 ? 'block' : 'none';
}

function getOrderedFields(rawFields) {
    if (!Array.isArray(rawFields) || rawFields.length === 0) return [];

    if (!fieldOrder || fieldOrder.length === 0) {
        return rawFields;
    }

    const ordered = [];
    fieldOrder.forEach(key => {
        if (rawFields.includes(key)) {
            ordered.push(key);
        }
    });

    rawFields.forEach(field => {
        if (!ordered.includes(field)) {
            ordered.push(field);
        }
    });

    return ordered;
}

function getFieldDisplayName(fieldKey) {
    if (!fieldKey) return '';
    return fieldDisplayMap[fieldKey] || fieldKey;
}

function applyFilter() {
    currentPage = 0;
    loadData();
}

async function exportData() {
    if (!currentDataset) return;

    const btn = document.getElementById('exportBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Exporting...';

    try {
        const endpoint = `/api/datasets/${currentDataset}/export/download?format=csv`;

        const blob = await api.download(endpoint);

        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = buildDatasetFilename(currentDatasetName);
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        toast.success('Data exported successfully');
    } catch (error) {
        toast.error('Export failed');
    } finally {
        btn.disabled = false;
        btn.innerHTML = 'Export CSV';
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text || '';
    return div.innerHTML;
}

loadDatasets();
