(() => {
    const params = new URLSearchParams(window.location.search);
    const datasetId = params.get('datasetId') || params.get('id');
    if (!datasetId) {
        alert('Dataset id required');
        return;
    }

    async function loadPreview() {
        const res = await fetch(`/api/datasets/${datasetId}/raw/preview?limit=100`);
        const data = await res.json();
        document.getElementById('rawDatasetName').textContent = data.datasetName || '';
        populateTableSelect(data.tables || []);
        renderTables(data.tables || []);
    }

    function populateTableSelect(tables) {
        const select = document.getElementById('tableSelect');
        select.innerHTML = '';
        const allOpt = document.createElement('option');
        allOpt.value = 'ALL';
        allOpt.textContent = 'All tables';
        select.appendChild(allOpt);
        tables.forEach(t => {
            const opt = document.createElement('option');
            opt.value = t.name;
            opt.textContent = t.name;
            select.appendChild(opt);
        });
    }

    function renderTables(tables) {
        const container = document.getElementById('rawTableContainer');
        container.innerHTML = '';
        const selected = document.getElementById('tableSelect').value;
        const visible = selected === 'ALL' ? tables : tables.filter(t => t.name === selected);
        if (!visible.length) {
            container.classList.add('empty-state');
            container.textContent = 'No data available for preview';
            return;
        }
        container.classList.remove('empty-state');
        visible.forEach(table => {
            const section = document.createElement('div');
            section.className = 'table-section';
            const title = document.createElement('h3');
            title.textContent = table.name;
            section.appendChild(title);
            const headers = table.columns;
            const rows = table.rows.map(r => headers.map(h => r[h] ?? ''));
            const tbl = buildTable(headers, rows);
            section.appendChild(tbl);
            container.appendChild(section);
        });
    }

    function buildTable(headers, rows) {
        const table = document.createElement('table');
        const thead = document.createElement('thead');
        const trHead = document.createElement('tr');
        headers.forEach(h => {
            const th = document.createElement('th');
            th.textContent = h;
            trHead.appendChild(th);
        });
        thead.appendChild(trHead);
        table.appendChild(thead);
        const tbody = document.createElement('tbody');
        rows.forEach(row => {
            const tr = document.createElement('tr');
            row.forEach(cell => {
                const td = document.createElement('td');
                td.textContent = cell;
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        return table;
    }

    function bindActions() {
        document.getElementById('refreshRaw').addEventListener('click', loadPreview);
        document.getElementById('tableSelect').addEventListener('change', () => {
            // re-render based on selection
            loadPreview();
        });
        document.getElementById('backToDataset').addEventListener('click', () => {
            window.location.href = `/dataset?datasetId=${datasetId}`;
        });
    }

    bindActions();
    loadPreview();
})();
