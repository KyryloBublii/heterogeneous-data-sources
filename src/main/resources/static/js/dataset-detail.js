(() => {
    if (!requireAuth()) {
        return;
    }

    const params = new URLSearchParams(window.location.search);
    const datasetId = params.get('datasetId') || params.get('id');
    if (!datasetId) {
        alert('Dataset id is required');
        return;
    }

    const state = {
        fields: [],
        sources: [],
        mappings: [],
        availableSources: [],
        pipeline: null,
        mappingPreview: null
    };

    let selectedFile = null;
    let uploadedMetadata = null;
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

    async function loadPipelineStatus() {
        const data = await api.get(`/api/datasets/${datasetId}/pipeline`);
        state.pipeline = data;
        if (data.datasetName) {
            document.title = data.datasetName;
        }
        document.getElementById('datasetTitle').textContent = data.datasetName || 'Dataset';
        document.getElementById('datasetDescription').textContent = data.description || '';

        const transformButtons = [
            document.getElementById('startTransformBtn')
        ];
        transformButtons.forEach(btn => {
            if (!btn) return;
            btn.disabled = !data.hasMappings || !data.hasIngestion;
            btn.title = btn.disabled ? 'Complete mapping after ingestion before transforming' : '';
        });

        const mappingDisabled = !data.hasIngestion;
        [document.getElementById('addFieldBtn'), document.getElementById('addMappingBtn')]
            .filter(Boolean)
            .forEach(btn => {
                btn.disabled = mappingDisabled;
                btn.title = mappingDisabled ? 'Run ingestion first (stage 1)' : '';
            });

        const exportBtn = document.getElementById('openExportModalBtn');
        if (exportBtn) {
            exportBtn.disabled = !data.readyForExport;
            exportBtn.title = exportBtn.disabled ? 'Complete transformation before exporting' : '';
        }

        updatePipelineSteps(data);
        updateStageSections();
        if (data.hasIngestion) {
            await ensureMappingPreview();
        }
    }

    function updatePipelineSteps(status) {
        const steps = [
            { key: 'sources', completed: status.hasIngestion },
            { key: 'mappings', completed: status.hasMappings },
            { key: 'transform', completed: status.hasTransformation },
            { key: 'export', completed: status.readyForExport }
        ];
        let activeAssigned = false;
        steps.forEach((step, index) => {
            const el = document.querySelector(`.pipeline-step[data-step="${step.key}"]`);
            if (!el) return;
            el.classList.toggle('completed', step.completed);
            el.classList.toggle('active', false);
            const badge = el.querySelector('.step-index');
            if (badge) {
                badge.textContent = step.completed ? '✔' : (index + 1).toString();
            }
            if (!step.completed && !activeAssigned) {
                el.classList.add('active');
                activeAssigned = true;
            }
        });
        if (!activeAssigned && steps.length) {
            const lastStep = document.querySelector(`.pipeline-step[data-step="${steps[steps.length - 1].key}"]`);
            if (lastStep) {
                lastStep.classList.add('active');
            }
        }
    }

    function updateStageSections() {
        const hasIngestion = Boolean(state.pipeline?.hasIngestion);
        const hasTransformation = Boolean(state.pipeline?.hasTransformation);
        const activeStage = !hasIngestion ? 'sources' : (!hasTransformation ? 'mapping' : 'export');

        ['sources', 'mapping', 'export'].forEach(stageKey => {
            toggleStage(stageKey, stageKey === activeStage);
        });

        const pipelineCard = document.getElementById('pipelineCard');
        if (pipelineCard) {
            pipelineCard.style.display = activeStage === 'export' ? 'none' : '';
        }

        const mappingGrid = document.getElementById('inlineMappingGrid');
        if (!hasIngestion && mappingGrid) {
            mappingGrid.textContent = 'Ingest sources to preview raw tables.';
            mappingGrid.classList.add('empty-state');
        }
    }

    function toggleStage(key, visible) {
        document.querySelectorAll(`[data-stage-section="${key}"]`).forEach(el => {
            el.classList.toggle('visible', Boolean(visible));
        });
    }

    async function ensureMappingPreview() {
        if (!state.pipeline?.hasIngestion) {
            return;
        }
        await loadInlineMappingPreview();
    }

    async function loadInlineMappingPreview() {
        const grid = document.getElementById('inlineMappingGrid');
        if (!grid) return;
        try {
            const data = await api.get(`/api/datasets/${datasetId}/mapping-editor/data?limit=200`);
            state.mappingPreview = data;
            populateInlineSelector(data.tables || []);
            renderInlineMappingGrid(data);
        } catch (error) {
            grid.textContent = error.message || 'Failed to load mapping preview.';
            grid.classList.add('empty-state');
        }
    }

    function populateInlineSelector(tables) {
        const selector = document.getElementById('inlineTableSelector');
        if (!selector) return;
        selector.innerHTML = '';
        const allOpt = document.createElement('option');
        allOpt.value = 'ALL';
        allOpt.textContent = 'All tables';
        selector.appendChild(allOpt);
        tables.forEach(tbl => {
            const opt = document.createElement('option');
            opt.value = tbl.name;
            opt.textContent = tbl.name;
            selector.appendChild(opt);
        });
    }

    function renderInlineMappingGrid(data) {
        const grid = document.getElementById('inlineMappingGrid');
        const selector = document.getElementById('inlineTableSelector');
        if (!grid) return;

        if (!data || !data.tables || data.tables.length === 0) {
            grid.textContent = 'No raw data available. Ingest sources first.';
            grid.classList.add('empty-state');
            return;
        }

        grid.classList.remove('empty-state');
        const selected = selector?.value || 'ALL';
        const tables = selected === 'ALL' ? data.tables : data.tables.filter(t => t.name === selected);
        const previous = collectInlineGridState();
        const rows = [];
        tables.forEach(tbl => {
            const sample = tbl.rows?.[0]?.values || {};
            (tbl.columns || []).forEach((col, idx) => {
                if (col === '__table__') return;
                const key = `${tbl.name}|${col}`;
                const prior = previous.get(key) || {};
                rows.push({
                    table: tbl.name,
                    column: col,
                    sample: sample[col] ?? '',
                    unified: prior.unified || col,
                    include: prior.include ?? true,
                    order: idx + 1,
                });
            });
        });

        grid.innerHTML = '';
        const table = document.createElement('table');
        table.className = 'table inline-preview-table';
        const thead = document.createElement('thead');
        thead.innerHTML = '<tr><th>Column</th><th>Sample value</th><th>Table</th><th>Unified column name</th><th>Include?</th></tr>';
        table.appendChild(thead);
        const tbody = document.createElement('tbody');
        rows.forEach(row => {
            const tr = document.createElement('tr');
            tr.dataset.table = row.table;
            tr.dataset.column = row.column;

            const tdColumn = document.createElement('td');
            tdColumn.textContent = row.column;

            const tdSample = document.createElement('td');
            tdSample.textContent = inlineTruncate(row.sample);

            const tdTable = document.createElement('td');
            tdTable.textContent = row.table;

            const tdUnified = document.createElement('td');
            const unifiedInput = document.createElement('input');
            unifiedInput.type = 'text';
            unifiedInput.className = 'unified-field-input';
            unifiedInput.value = row.unified;
            unifiedInput.placeholder = 'Enter unified field name...';
            unifiedInput.setAttribute('aria-label', `Unified field name for ${row.column}`);
            unifiedInput.title = `Map "${row.column}" to a unified field name`;
            tdUnified.appendChild(unifiedInput);

            const tdInclude = document.createElement('td');
            tdInclude.className = 'cell-center';
            const includeCheckbox = document.createElement('input');
            includeCheckbox.type = 'checkbox';
            includeCheckbox.checked = row.include;
            tdInclude.appendChild(includeCheckbox);

            tr.append(tdColumn, tdSample, tdTable, tdUnified, tdInclude);
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        grid.appendChild(table);
    }

    function collectInlineGridState() {
        const current = new Map();
        const rows = document.querySelectorAll('#inlineMappingGrid tbody tr');
        rows.forEach(tr => {
            const key = `${tr.dataset.table}|${tr.dataset.column}`;
            const unified = tr.querySelector('input[type="text"]')?.value || '';
            const include = tr.querySelector('input[type="checkbox"]')?.checked ?? true;
            current.set(key, { unified, include });
        });
        return current;
    }

    function buildInlineMappingPayload() {
        const grid = document.getElementById('inlineMappingGrid');
        const rows = Array.from(grid.querySelectorAll('tbody tr'));
        if (!rows.length) {
            throw new Error('No preview rows available. Ingest sources first.');
        }
        const included = rows.filter(row => row.querySelector('input[type="checkbox"]')?.checked);
        if (!included.length) {
            throw new Error('Select at least one column to include in the mapping.');
        }
        const mappings = included.map(row => ({
            datasetFieldName: row.querySelector('input[type="text"]').value.trim() || row.dataset.column,
            table: row.dataset.table,
            column: row.dataset.column,
        }));
        const uniqueNames = Array.from(new Set(mappings.map(m => m.datasetFieldName)));
        const fields = uniqueNames.map((name, idx) => ({
            name,
            dtype: 'TEXT',
            required: false,
            position: idx + 1,
        }));
        return { fields, mappings };
    }

    function inlineTruncate(val) {
        if (val === null || val === undefined) return '';
        const str = val.toString();
        return str.length > 60 ? `${str.substring(0, 57)}…` : str;
    }

    async function saveInlineMappingFromGrid() {
        const button = document.getElementById('inlineSaveMapping');
        const hint = document.getElementById('mappingStepStatus');
        if (!state.pipeline?.hasIngestion) {
            alert('Run ingestion first to preview and save mappings.');
            return;
        }
        try {
            if (button) {
                button.disabled = true;
                button.classList.add('btn-loading');
                button.innerHTML = '<span class="spinner"></span> Saving...';
            }
            const payload = buildInlineMappingPayload();
            await api.post(`/api/datasets/${datasetId}/mapping-editor/save`, payload);
            await loadFieldsAndMappings();
            await loadPipelineStatus();
            if (hint) {
                hint.textContent = '';
            }
        } catch (error) {
            alert(error.message || 'Failed to save inline mappings.');
            if (hint) {
                hint.textContent = 'Failed to save inline mappings.';
            }
        } finally {
            if (button) {
                button.disabled = false;
                button.classList.remove('btn-loading');
                button.textContent = 'Done';
            }
        }
    }

    async function loadSources() {
        state.sources = await api.get(`/api/sources?datasetId=${datasetId}`);
        const table = buildTable(['Name', 'Type', 'Status', 'Actions'], state.sources.map(src => ([
            src.name,
            src.type,
            src.status || 'ACTIVE',
            `<button class="btn btn-ghost btn-sm" data-source="${src.id}" data-action="inspect-source">View</button>` +
            `<button class="btn btn-ghost btn-sm" data-source="${src.id}" data-action="remove-source">Remove</button>`
        ])));
        document.getElementById('sourcesTable').innerHTML = '';
        document.getElementById('sourcesTable').appendChild(table);

        const summarySourcesLabel = document.getElementById('summarySources');
        const sourcesUsedTable = document.getElementById('sourcesUsedTable');
        if (sourcesUsedTable) {
            sourcesUsedTable.innerHTML = '';
            if (!state.sources.length) {
                const emptyState = document.createElement('div');
                emptyState.classList.add('field-hint');
                emptyState.textContent = 'No sources configured yet.';
                sourcesUsedTable.appendChild(emptyState);
                if (summarySourcesLabel) {
                    summarySourcesLabel.textContent = 'No sources yet';
                }
            } else {
                const summaryHeaders = ['Name', 'Type', 'Status'];
                const summaryRows = state.sources.map(src => [
                    src.name,
                    src.type,
                    src.status || 'ACTIVE'
                ]);
                sourcesUsedTable.appendChild(buildTable(summaryHeaders, summaryRows));
                if (summarySourcesLabel) {
                    summarySourcesLabel.textContent = `${state.sources.length} source(s)`;
                }
            }
        }

        const mappingSource = document.getElementById('mappingSource');
        mappingSource.innerHTML = '';
        state.sources.forEach(src => {
            const opt = document.createElement('option');
            opt.value = src.id;
            opt.textContent = `${src.name} (${src.type})`;
            mappingSource.appendChild(opt);
        });

        await loadAssignableSources();
    }

    async function loadAssignableSources() {
        const available = await api.get('/api/sources/available');
        const assignedIds = new Set(state.sources.map(src => src.id));
        state.availableSources = available.filter(src => !assignedIds.has(src.id));
        const selector = document.getElementById('existingSourceSelect');
        const hint = document.getElementById('assignSourceHint');
        const assignButton = document.getElementById('assignSourceBtn');

        if (!selector || !assignButton) {
            return;
        }

        selector.innerHTML = '';
        if (!state.availableSources.length) {
            const option = document.createElement('option');
            option.value = '';
            option.textContent = 'No sources available yet';
            selector.appendChild(option);
            selector.disabled = true;
            assignButton.disabled = true;
            if (hint) {
                hint.textContent = 'Create a source on the Sources page to reuse it across datasets.';
            }
            return;
        }

        selector.disabled = false;
        assignButton.disabled = false;
        state.availableSources.forEach(src => {
            const opt = document.createElement('option');
            opt.value = src.id;
            const datasetTag = src.datasetId ? ` • linked to dataset ${src.datasetId}` : '';
            opt.textContent = `${src.name} (${src.type}${datasetTag})`;
            selector.appendChild(opt);
        });
        if (hint) {
            hint.textContent = 'Assign an existing source; if it is linked elsewhere a copy will be created for this dataset.';
        }
    }

    async function loadFieldsAndMappings() {
        const [fieldsRes, mappingsRes] = await Promise.all([
            api.get(`/api/datasets/${datasetId}/fields`),
            api.get(`/api/datasets/${datasetId}/mappings`)
        ]);
        state.fields = fieldsRes;
        state.mappings = mappingsRes;
        const mappingCount = state.mappings.reduce((acc, m) => {
            acc[m.datasetFieldId] = (acc[m.datasetFieldId] || 0) + 1;
            return acc;
        }, {});

        const header = ['Field name', 'Data type', 'Required', 'Position', 'Mappings', 'Actions'];
        const rows = state.fields.map(f => ([
            f.name,
            f.dtype,
            f.isNullable ? 'No' : 'Yes',
            f.position ?? '',
            mappingCount[f.id] || 0,
            `<button class="btn btn-ghost btn-sm" data-field="${f.id}" data-action="edit-field">Edit</button>`+
            `<button class="btn btn-ghost btn-sm" data-field="${f.id}" data-action="delete-field">Delete</button>`
        ]));
        const table = buildTable(header, rows);
        document.getElementById('fieldsTable').innerHTML = '';
        document.getElementById('fieldsTable').appendChild(table);

        const mappingContainer = document.getElementById('mappingsTable');
        if (!state.mappings.length) {
            mappingContainer.classList.add('empty-state');
            mappingContainer.textContent = 'No mappings defined yet.';
        } else {
            mappingContainer.classList.remove('empty-state');
            const mappingHeader = ['Source', 'Field', 'Path', 'Transform', 'Required', 'Actions'];
            const mappingRows = state.mappings.map(m => ([
                m.sourceName || m.sourceId || '-',
                m.datasetFieldName || m.datasetFieldUid || '-',
                m.srcPath || '',
                m.transformType || 'NONE',
                m.required ? 'Yes' : 'No',
                `<button class="btn btn-ghost btn-sm" data-mapping="${m.id}" data-action="edit-mapping">Edit</button>` +
                `<button class="btn btn-ghost btn-sm" data-mapping="${m.id}" data-action="delete-mapping">Delete</button>`
            ]));
            const mappingTable = buildTable(mappingHeader, mappingRows);
            mappingContainer.innerHTML = '';
            mappingContainer.appendChild(mappingTable);
        }

        const fieldSelect = document.getElementById('mappingField');
        fieldSelect.innerHTML = '';
        state.fields.forEach(f => {
            const opt = document.createElement('option');
            opt.value = f.datasetFieldUid || f.id;
            opt.textContent = f.name;
            fieldSelect.appendChild(opt);
        });
    }

    async function loadUnifiedPreview() {
        const data = await api.get(`/api/datasets/${datasetId}/unified/preview?limit=20`);
        const container = document.getElementById('unifiedDataTable');
        if (!data.rows || data.rows.length === 0) {
            container.classList.add('empty-state');
            container.textContent = 'No unified data available yet. Define fields & mappings and run a transform.';
            return;
        }
        container.classList.remove('empty-state');
        const headers = [...data.fields.map(f => f.name), 'Actions'];
        const rows = data.rows.map(row => {
            const values = data.fields.map(f => row.values[f.id] ?? '');
            const toggle = `<button class="btn btn-ghost btn-sm" data-row="${row.id}" data-excluded="${row.isExcluded}" data-action="toggle-row">${row.isExcluded ? 'Include' : 'Exclude'}</button>`;
            return [...values, toggle];
        });
        const table = buildTable(headers, rows);
        container.innerHTML = '';
        container.appendChild(table);
    }

    async function startIngestion(event) {
        const button = event?.target || document.getElementById('startIngestionBtn');
        const hint = document.getElementById('ingestionStepStatus');
        const minDuration = 1400;
        const start = Date.now();
        if (button) {
            button.disabled = true;
            button.classList.add('btn-loading');
            button.innerHTML = '<span class="spinner"></span> Ingesting...';
        }
        if (hint) {
            hint.textContent = 'Starting ingestion and preparing raw rows...';
        }

        try {
            await apiRequest(`/api/datasets/${datasetId}/ingest`, {method: 'POST'});
            const elapsed = Date.now() - start;
            if (elapsed < minDuration) {
                await new Promise(res => setTimeout(res, minDuration - elapsed));
            }
            await loadSources();
            await loadPipelineStatus();
            if (hint) {
                hint.textContent = 'Ingestion finished. Proceed to mapping editor (stage 2)';
            }
        } catch (error) {
            alert(error.message || 'Failed to start ingestion');
            if (hint) {
                hint.textContent = 'Ingestion failed. Please try again.';
            }
        } finally {
            if (button) {
                button.disabled = false;
                button.classList.remove('btn-loading');
                button.textContent = 'Next: Start ingestion';
            }
        }
    }

    async function startTransform(event) {
        const buttons = [
            document.getElementById('startTransformBtn')
        ];
        const hint = document.getElementById('transformStepStatus');
        const mappingHint = document.getElementById('mappingStepStatus');
        const minDuration = 1500;
        const start = Date.now();
        buttons.forEach(btn => btn && (btn.disabled = true));
        if (mappingHint) {
            mappingHint.textContent = 'Starting transformation...';
        }
        if (hint) {
            hint.textContent = 'Processing unified rows...';
        }
        try {
            await apiRequest(`/api/datasets/${datasetId}/transform`, { method: 'POST' });
            const elapsed = Date.now() - start;
            if (elapsed < minDuration) {
                await new Promise(res => setTimeout(res, minDuration - elapsed));
            }
            await loadUnifiedPreview();
            await loadPipelineStatus();
            if (hint) {
                hint.textContent = 'Transformation complete. Review unified data and export.';
            }
        } catch (error) {
            alert(error.message || 'Transform failed. Check mappings and raw data relationships.');
            if (hint) {
                hint.textContent = 'Transformation failed. Fix mappings and retry.';
            }
        } finally {
            buttons.forEach(btn => btn && (btn.disabled = false));
        }
    }

    function openExportModal() {
        const modal = document.getElementById('exportModal');
        if (modal) {
            modal.style.display = 'flex';
            document.getElementById('exportStatus').textContent = '';
        }
    }

    function closeExportModal() {
        const modal = document.getElementById('exportModal');
        if (modal) {
            modal.style.display = 'none';
        }
    }

    function buildDatasetFilename(name) {
        const base = name && name.trim() ? name.trim() : 'dataset';
        const safe = base.replace(/[^a-zA-Z0-9-_]+/g, '-');
        return `${safe}.csv`;
    }

    async function exportCsvDownload() {
        const statusEl = document.getElementById('exportStatus');
        try {
            const blob = await api.download(`/api/datasets/${datasetId}/export/download?format=csv`);
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = buildDatasetFilename(state.pipeline?.datasetName);
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.URL.revokeObjectURL(url);
            if (statusEl) {
                statusEl.textContent = 'CSV downloaded';
            }
        } catch (error) {
            if (statusEl) {
                statusEl.textContent = error.message || 'Failed to download CSV';
            }
        }
    }

    function openFieldModal(fieldId) {
        const modal = document.getElementById('fieldModal');
        modal.style.display = 'flex';
        modal.dataset.fieldId = fieldId || '';
        document.getElementById('fieldModalTitle').textContent = fieldId ? 'Edit field' : 'Add field';
        if (fieldId) {
            const field = state.fields.find(f => f.id === Number(fieldId));
            document.getElementById('fieldName').value = field?.name || '';
            document.getElementById('fieldType').value = field?.dtype || 'STRING';
            document.getElementById('fieldRequired').checked = !(field?.isNullable ?? true);
            document.getElementById('fieldUnique').checked = Boolean(field?.isUnique);
            document.getElementById('fieldPosition').value = field?.position ?? 0;
        } else {
            document.getElementById('fieldForm').reset();
        }
    }

    function closeFieldModal() {
        document.getElementById('fieldModal').style.display = 'none';
    }

    function openMappingModal(mapping) {
        const modal = document.getElementById('mappingModal');
        modal.style.display = 'flex';
        modal.dataset.mappingId = mapping?.id || '';
        const transformSelect = document.getElementById('mappingTransformType');
        if (transformSelect) {
            transformSelect.value = mapping?.transformType || 'NONE';
        }
        document.getElementById('mappingSource').value = mapping?.sourceId ?? document.getElementById('mappingSource').value;
        document.getElementById('mappingField').value = mapping?.datasetFieldUid ?? document.getElementById('mappingField').value;
        document.getElementById('mappingPath').value = mapping?.srcPath || '';
        document.getElementById('mappingRequired').checked = Boolean(mapping?.required);
    }

    function closeMappingModal() {
        const modal = document.getElementById('mappingModal');
        modal.style.display = 'none';
        modal.dataset.mappingId = '';
    }

    async function saveField(event) {
        event.preventDefault();
        const payload = {
            name: document.getElementById('fieldName').value,
            dtype: document.getElementById('fieldType').value,
            isNullable: !document.getElementById('fieldRequired').checked,
            isUnique: document.getElementById('fieldUnique').checked,
            defaultExpr: null,
            position: Number(document.getElementById('fieldPosition').value || 0)
        };
        const fieldId = document.getElementById('fieldModal').dataset.fieldId;
        const method = fieldId ? 'PUT' : 'POST';
        const url = fieldId ? `/api/datasets/${datasetId}/fields/${fieldId}` : `/api/datasets/${datasetId}/fields`;
        await apiRequest(url, {method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
        closeFieldModal();
        await loadFieldsAndMappings();
        await loadPipelineStatus();
    }

    async function deleteField(fieldId) {
        await apiRequest(`/api/datasets/${datasetId}/fields/${fieldId}`, {method: 'DELETE'});
        await loadFieldsAndMappings();
    }

    async function deleteMapping(mappingId) {
        await apiRequest(`/api/datasets/${datasetId}/mappings/${mappingId}`, {method: 'DELETE'});
        await loadFieldsAndMappings();
        await loadPipelineStatus();
    }

    async function assignExistingSource(event) {
        event.preventDefault();
        const selector = document.getElementById('existingSourceSelect');
        const sourceId = selector.value;
        if (!sourceId) {
            alert('Select a source to assign');
            return;
        }
        await apiRequest(`/api/sources/${sourceId}/dataset`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({datasetId: Number(datasetId)})
        });
        await loadSources();
        await loadPipelineStatus();
    }

    async function detachSource(sourceId) {
        if (!sourceId) return;
        const confirmed = confirm('Remove this source from the dataset?');
        if (!confirmed) return;
        await apiRequest(`/api/sources/${sourceId}/dataset`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({datasetId: null})
        });
        await loadSources();
        await loadPipelineStatus();
    }

    async function saveMapping(event) {
        event.preventDefault();
        const payload = {
            sourceId: Number(document.getElementById('mappingSource').value),
            datasetFieldUid: document.getElementById('mappingField').value,
            srcPath: document.getElementById('mappingPath').value,
            transformType: document.getElementById('mappingTransformType').value,
            required: document.getElementById('mappingRequired').checked
        };
        const mappingId = document.getElementById('mappingModal').dataset.mappingId;
        const method = mappingId ? 'PUT' : 'POST';
        const url = mappingId ? `/api/datasets/${datasetId}/mappings/${mappingId}` : `/api/datasets/${datasetId}/mappings`;
        await apiRequest(url, {
            method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        closeMappingModal();
        await loadFieldsAndMappings();
        await loadPipelineStatus();
    }

    async function toggleRow(rowId, current) {
        await apiRequest(`/api/datasets/${datasetId}/unified/${rowId}/exclude`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({excluded: !current})
        });
        await loadUnifiedPreview();
    }

    function bindActions() {
        document.getElementById('startIngestionBtn').addEventListener('click', startIngestion);
        document.getElementById('startTransformBtn').addEventListener('click', startTransform);
        document.getElementById('inlineRefreshPreview').addEventListener('click', ensureMappingPreview);
        document.getElementById('inlineSaveMapping').addEventListener('click', saveInlineMappingFromGrid);
        document.getElementById('openAddSourceBtn').addEventListener('click', openAddSourceModal);
        document.getElementById('refreshSources').addEventListener('click', loadSources);
        document.getElementById('addFieldBtn').addEventListener('click', () => openFieldModal());
        document.getElementById('addMappingBtn').addEventListener('click', () => openMappingModal());
        document.getElementById('inlineTableSelector').addEventListener('change', () => renderInlineMappingGrid(state.mappingPreview));
        document.getElementById('assignSourceBtn').addEventListener('click', assignExistingSource);
        document.getElementById('fieldForm').addEventListener('submit', saveField);
        document.getElementById('mappingForm').addEventListener('submit', saveMapping);
        document.getElementById('addSourceForm').addEventListener('submit', saveSource);
        document.getElementById('openExportModalBtn').addEventListener('click', openExportModal);
        document.getElementById('exportCsvBtn').addEventListener('click', exportCsvDownload);
        document.getElementById('exportCloseBtn').addEventListener('click', closeExportModal);
        const skipTables = document.getElementById('skipTableSelection');
        const closeTables = document.getElementById('closeTableSelection');
        const saveTables = document.getElementById('saveTableSelection');
        [skipTables, closeTables].forEach(btn => btn && btn.addEventListener('click', closeTableSelectionModal));
        if (saveTables) {
            saveTables.addEventListener('click', saveTableSelection);
        }
        document.getElementById('sourcesTable').addEventListener('click', (e) => {
            const action = e.target.dataset.action;
            if (action === 'remove-source') {
                detachSource(e.target.dataset.source);
            }
            if (action === 'inspect-source') {
                window.location.href = '/sources';
            }
        });
        document.getElementById('fieldsTable').addEventListener('click', (e) => {
            const action = e.target.dataset.action;
            if (action === 'edit-field') {
                openFieldModal(e.target.dataset.field);
            }
            if (action === 'delete-field') {
                deleteField(e.target.dataset.field);
            }
        });
        document.getElementById('mappingsTable').addEventListener('click', (e) => {
            const action = e.target.dataset.action;
            const mappingId = Number(e.target.dataset.mapping);
            if (!action || Number.isNaN(mappingId)) return;
            if (action === 'edit-mapping') {
                const mapping = state.mappings.find(m => m.id === mappingId);
                if (mapping) {
                    openMappingModal(mapping);
                }
            }
            if (action === 'delete-mapping') {
                deleteMapping(mappingId);
            }
        });
        document.getElementById('unifiedDataTable').addEventListener('click', (e) => {
            if (e.target.dataset.action === 'toggle-row') {
                toggleRow(e.target.dataset.row, e.target.dataset.excluded === 'true');
            }
        });
    }

    function bindSourceModal() {
        const sourceTypeSelect = document.getElementById('sourceType');
        const dbConfigSection = document.getElementById('dbConfigSection');
        const fileConfigSection = document.getElementById('fileConfigSection');
        const csvDelimiterSection = document.getElementById('csvDelimiterSection');
        const csvDelimiterInput = document.getElementById('csvDelimiter');
        const dropzone = document.getElementById('fileDropzone');
        const fileInput = document.getElementById('fileInput');
        const testButton = document.getElementById('testDbConnection');

        if (sourceTypeSelect) {
            sourceTypeSelect.addEventListener('change', (event) => {
                const value = event.target.value;
                dbConfigSection.style.display = value === 'DB' ? 'block' : 'none';
                fileConfigSection.style.display = value === 'CSV' ? 'block' : 'none';
                if (csvDelimiterSection) {
                    csvDelimiterSection.style.display = value === 'CSV' ? 'block' : 'none';
                }
            });
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

        if (testButton) {
            testButton.addEventListener('click', async () => {
                try {
                    const config = buildDatabaseConfig();
                    await api.post('/api/sources/test-connection', {type: 'DB', config});
                    document.getElementById('dbTestResult').textContent = 'Connection successful.';
                    document.getElementById('dbTestResult').style.color = 'var(--success-color)';
                } catch (error) {
                    document.getElementById('dbTestResult').textContent = error.message || 'Connection failed.';
                    document.getElementById('dbTestResult').style.color = 'var(--error-color)';
                }
            });
        }
    }

    function openAddSourceModal() {
        document.getElementById('addSourceForm').reset();
        resetSourceModal();
        document.getElementById('addSourceModal').style.display = 'flex';
    }

    function closeAddSourceModal() {
        document.getElementById('addSourceModal').style.display = 'none';
        document.getElementById('addSourceForm').reset();
        resetSourceModal();
    }

    function resetSourceModal() {
        selectedFile = null;
        uploadedMetadata = null;
        document.getElementById('fileName').textContent = '';
        document.getElementById('dbTestResult').textContent = '';
        document.getElementById('dbConfigSection').style.display = 'none';
        document.getElementById('fileConfigSection').style.display = 'none';
        const csvDelimiterSection = document.getElementById('csvDelimiterSection');
        if (csvDelimiterSection) {
            csvDelimiterSection.style.display = 'none';
        }
    }

    function setSelectedFile(file) {
        selectedFile = file;
        uploadedMetadata = null;
        document.getElementById('fileName').textContent = `${file.name} (${Math.round(file.size / 1024)} KB)`;
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

        return {host, port, database, username, password};
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
            const delimiter = (document.getElementById('csvDelimiter')?.value || ',').trim() || ',';
            formData.append('delimiter', delimiter);
        }
        formData.append('encoding', 'UTF-8');

        const endpoint = '/api/upload/csv';
        uploadedMetadata = await api.postMultipart(endpoint, formData);
        return uploadedMetadata.config || {};
    }

    function sanitizeForUpload(value) {
        return (value || 'source')
            .toLowerCase()
            .replace(/[^a-z0-9_-]/g, '-')
            .replace(/-+/g, '-')
            .substring(0, 40);
    }

    async function saveSource(event) {
        event.preventDefault();
        const name = document.getElementById('sourceName').value.trim();
        const type = document.getElementById('sourceType').value;
        const role = document.getElementById('sourceRole').value || 'SOURCE';

        if (!name || !type) {
            alert('Source name and type are required');
            return;
        }

        let config = {};
        try {
            if (type === 'DB') {
                config = buildDatabaseConfig();
            } else if (type === 'CSV') {
                config = await ensureFileUploaded(type, name);
            }
        } catch (error) {
            alert(error.message || 'Invalid configuration');
            return;
        }

        const submitBtn = event.target.querySelector('button[type="submit"]');
        submitBtn.disabled = true;
        submitBtn.textContent = 'Creating...';

        try {
            const created = await api.post('/api/sources', {name, type, role, config, datasetId: Number(datasetId)});
            closeAddSourceModal();
            openTableSelectionModal(created);
            await loadSources();
            await loadPipelineStatus();
        } catch (error) {
            alert(error.message || 'Failed to create source');
        } finally {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Create Source';
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
            const tables = await api.get(`/api/sources/${sourceId}/schema`);
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
            await api.put(`/api/sources/${tableSelectionState.currentSource.id}`, { config: baseConfig });
            if (window.toast) {
                toast.success('Table selection saved');
            }
            closeTableSelectionModal();
            await loadSources();
        } catch (error) {
            if (window.toast) {
                toast.error(error.message || 'Failed to save selection');
            } else {
                alert(error.message || 'Failed to save selection');
            }
        }
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
                td.innerHTML = cell;
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        return table;
    }

    async function init() {
        bindActions();
        bindSourceModal();
        await loadPipelineStatus();
        await loadSources();
        await loadFieldsAndMappings();
        await loadUnifiedPreview();
    }

    window.datasetDetail = {closeFieldModal, closeMappingModal, closeAddSourceModal, closeExportModal};
    init();
})();
