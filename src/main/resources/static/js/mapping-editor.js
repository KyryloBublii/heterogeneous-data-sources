(() => {
  if (!requireAuth()) {
    return;
  }

  const params = new URLSearchParams(window.location.search);
  const datasetId = params.get('datasetId') || params.get('id');
  const grid = document.getElementById('mapping-grid');
  const selector = document.getElementById('table-selector');
  const datasetTitle = document.getElementById('editor-dataset-name');

  if (!datasetId) {
    grid.innerHTML = '<p class="muted">Missing datasetId.</p>';
    return;
  }

  document.getElementById('back-to-dataset').href = `/dataset?datasetId=${datasetId}`;

  document.getElementById('save-mapping').addEventListener('click', async () => {
    try {
      const payload = buildSavePayload();
      await api.post(`/api/datasets/${datasetId}/mapping-editor/save`, payload);
      window.location.href = `/dataset?datasetId=${datasetId}`;
    } catch (err) {
      const message = err?.message || 'Failed to save mapping configuration';
      alert(message);
      console.error('Failed to save mapping editor payload', err);
    }
  });

  document.getElementById('refresh-preview').addEventListener('click', () => loadData());
  selector.addEventListener('change', () => renderGrid(currentData));

  let currentData = null;

  async function loadData() {
    const data = await api.get(`/api/datasets/${datasetId}/mapping-editor/data?limit=200`);
    currentData = data;
    datasetTitle.textContent = data.datasetName ?? 'Dataset';
    populateSelector(data.tables || []);
    renderGrid(data);
  }

  function populateSelector(tables) {
    selector.innerHTML = '';
    const allOpt = document.createElement('option');
    allOpt.value = 'ALL';
    allOpt.textContent = 'All tables';
    selector.appendChild(allOpt);
    tables.forEach((tbl) => {
      const opt = document.createElement('option');
      opt.value = tbl.name;
      opt.textContent = tbl.name;
      selector.appendChild(opt);
    });
  }

  function renderGrid(data) {
    if (!data || !data.tables || data.tables.length === 0) {
      grid.innerHTML = '<p class="muted">No raw data available. Ingest sources first.</p>';
      return;
    }
    const selected = selector.value || 'ALL';
    const tables = selected === 'ALL' ? data.tables : data.tables.filter((t) => t.name === selected);
    const rows = [];
    tables.forEach((tbl) => {
      const sample = tbl.rows?.[0]?.values || {};
      (tbl.columns || []).forEach((col, idx) => {
        if (col === '__table__') {
          return;
        }
        rows.push({
          table: tbl.name,
          column: col,
          rawColumn: col,
          sample: sample[col] ?? '',
          unified: col,
          include: true,
          order: idx + 1,
        });
      });
    });

    grid.innerHTML = '';
    const table = document.createElement('table');
    table.className = 'table';
    const thead = document.createElement('thead');
    thead.innerHTML = '<tr><th>Column</th><th>Sample value</th><th>Unified column name</th><th>Include</th></tr>';
    table.appendChild(thead);
    const tbody = document.createElement('tbody');

    rows.forEach((row) => {
      const tr = document.createElement('tr');
      tr.dataset.table = row.table;
      tr.dataset.column = row.rawColumn;

      const tdCol = document.createElement('td');
      tdCol.textContent = row.column;
      const tdSample = document.createElement('td');
      tdSample.textContent = truncate(row.sample);
      const tdUnified = document.createElement('td');
      const input = document.createElement('input');
      input.type = 'text';
      input.value = row.unified;
      input.className = 'unified-field-input';
      input.placeholder = 'Enter unified field name...';
      input.setAttribute('aria-label', `Unified field name for ${row.column}`);
      input.title = `Map "${row.column}" to a unified field name`;
      tdUnified.appendChild(input);
      const tdInclude = document.createElement('td');
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.checked = true;
      tdInclude.appendChild(checkbox);

      tr.append(tdCol, tdSample, tdUnified, tdInclude);
      tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    grid.appendChild(table);
  }

  function buildSavePayload() {
    const rows = Array.from(grid.querySelectorAll('tbody tr'));
    const included = rows.filter((row) => row.querySelector('input[type="checkbox"]').checked);
    const mappings = included.map((row) => ({
      datasetFieldName: row.querySelector('input[type="text"]').value.trim() || row.dataset.column,
      table: row.dataset.table,
      column: row.dataset.column,
    }));
    const uniqueNames = Array.from(new Set(mappings.map((m) => m.datasetFieldName)));
    const fields = uniqueNames.map((name, idx) => ({
      name,
      dtype: 'TEXT',
      required: false,
      position: idx + 1,
    }));
    return { fields, mappings };
  }

  function truncate(val) {
    if (val === null || val === undefined) return '';
    const str = val.toString();
    return str.length > 60 ? str.substring(0, 57) + '…' : str;
  }

  loadData();
})();
