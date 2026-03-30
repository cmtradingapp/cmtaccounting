document.getElementById('uploadForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const singleFiles = ['platformFile', 'equityFile', 'transactionsFile'];
    const formData = new FormData();
    const uploadSpinner = document.getElementById('uploadSpinner');

    const bankFiles = document.getElementById('bankFile').files;
    if (bankFiles.length === 0) {
        return alert('Please select at least one Bank/PSP statement.');
    }
    for (let i = 0; i < bankFiles.length; i++) {
        formData.append('bankFile', bankFiles[i]);
    }

    for (const id of singleFiles) {
        const file = document.getElementById(id).files[0];
        if (!file) {
            return alert(`Missing requirement: ${id}. Please select all input files.`);
        }
        formData.append(id, file);
    }

    // Optional opening balance file
    const obFile = document.getElementById('openingBalanceFile')?.files[0];
    if (obFile) formData.append('openingBalanceFile', obFile);

    uploadSpinner.classList.remove('hidden');
    const submitBtn = document.querySelector('#uploadForm button');
    submitBtn.style.opacity = "0.7";

    try {
        const res = await fetch('/api/upload', { method: 'POST', body: formData });
        const data = await res.json();

        if (data.status === 'success') {
            document.getElementById('uploadZone').classList.add('hidden');

            const mappingZone = document.getElementById('mappingZone');
            const mappingCards = document.getElementById('mappingCards');

            mappingZone.querySelector('.subtitle').innerText = data.message + ' — Running AI column analysis...';
            mappingCards.innerHTML = '<div style="color: #94a3b8; padding: 20px; text-align: center;"><span class="spinner"></span> Asking Claude to analyze column schemas...</div>';
            mappingCards.style.gridTemplateColumns = '1fr';
            mappingZone.classList.remove('hidden');

            // Call AI mapping endpoint — optional, falls back gracefully if unavailable
            let aiMapping = null;
            try {
                const mapRes = await fetch('/api/map-columns', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sources: data.sources })
                });
                const mapData = await mapRes.json();
                if (mapData.status === 'success') aiMapping = mapData.mapping;
            } catch (_) {
                // AI mapping is optional — silently fall back to raw column display
            }

            mappingZone.querySelector('.subtitle').innerText = data.message;
            mappingCards.innerHTML = '';

            for (const [source, info] of Object.entries(data.sources)) {
                const aiSource = aiMapping?.sources?.[source];
                const refCol = aiSource?.ref_col;
                const amtCol = aiSource?.amount_col;

                const colTags = info.columns.map(c => {
                    let style = 'background: rgba(59,130,246,0.15); color: #93c5fd;';
                    let badge = '';
                    if (refCol && c === refCol) {
                        style = 'background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid rgba(16,185,129,0.4);';
                        badge = ' <span style="font-size:10px;opacity:0.7;">🔑 join key</span>';
                    } else if (amtCol && c === amtCol) {
                        style = 'background: rgba(245,158,11,0.15); color: #fbbf24; border: 1px solid rgba(245,158,11,0.3);';
                        badge = ' <span style="font-size:10px;opacity:0.7;">💰 amount</span>';
                    }
                    return `<span style="${style} padding: 3px 8px; border-radius: 4px; font-size: 12px; font-family: monospace;">${c}${badge}</span>`;
                }).join(' ');

                const mappingRows = aiSource?.mappings
                    ? Object.entries(aiSource.mappings).slice(0, 6).map(([src, out]) =>
                        `<div style="font-size:11px; color:#64748b; font-family:monospace;">${src} → <span style="color:#a78bfa;">${out}</span></div>`
                      ).join('')
                    : '';

                mappingCards.innerHTML += `
                    <div class="mapping-card" style="flex-direction: column; align-items: flex-start; gap: 10px; border-left-color: #10b981;">
                        <div style="display: flex; justify-content: space-between; width: 100%; align-items: center;">
                            <span class="m-value" style="font-size: 15px;">${source}</span>
                            <span style="color: #64748b; font-size: 12px;">${info.filename}</span>
                        </div>
                        <div style="display: flex; flex-wrap: wrap; gap: 6px;">${colTags}</div>
                        ${mappingRows ? `<div style="border-top: 1px solid rgba(255,255,255,0.05); padding-top: 8px; width: 100%;">${mappingRows}</div>` : ''}
                    </div>
                `;
            }

            if (aiMapping?.join_explanation) {
                mappingCards.innerHTML += `
                    <div style="background: rgba(124,58,237,0.08); border: 1px solid rgba(124,58,237,0.2); border-radius: 8px; padding: 12px 16px; font-size: 13px; color: #a78bfa;">
                        <strong style="color:#c4b5fd;">AI Analysis:</strong> ${aiMapping.join_explanation}
                    </div>
                `;
            }
        } else {
            alert(data.error);
        }
    } catch (err) {
        alert("Server communication error. Ensure Python Flask is running.");
    } finally {
        uploadSpinner.classList.add('hidden');
        submitBtn.style.opacity = "1";
    }
});

document.getElementById('runReconBtn').addEventListener('click', async () => {
    const btn = document.getElementById('runReconBtn');
    btn.innerHTML = `<span class="spinner"></span> Joining Datasets...`;

    try {
        const res = await fetch('/api/reconcile', { method: 'POST' });
        const data = await res.json();

        if (data.status === 'success') {
            document.getElementById('mappingZone').classList.add('hidden');

            const s = data.summary;
            document.getElementById('joinKeyInfo').innerText = `Join: ${s.join_keys_used} | Deal No col: ${s.deal_no_column}`;
            document.getElementById('statMatched').innerText = s.total_matched.toLocaleString();
            document.getElementById('statCrmOnly').innerText = s.crm_unmatched.toLocaleString();
            document.getElementById('statBankOnly').innerText = s.bank_unmatched.toLocaleString();
            document.getElementById('statCrmTotal').innerText = s.total_crm_rows.toLocaleString();
            document.getElementById('statBankTotal').innerText = s.total_bank_rows.toLocaleString();
            document.getElementById('statFees').innerText = `$${s.unrecon_fees.toLocaleString()}`;

            document.getElementById('resultsZone').classList.remove('hidden');
        } else {
            alert("Reconciliation error: " + (data.summary?.error || "Unknown error"));
        }
    } catch (err) {
        alert("Mathematical Reconciliation failed.");
    } finally {
        btn.innerHTML = `Execute Mathematical Match <span class="arrow">→</span>`;
    }
});

// Download buttons — fetch from backend and trigger browser save
document.querySelectorAll('.btn.download').forEach(btn => {
    btn.addEventListener('click', () => {
        const isLifecycle = btn.textContent.includes('Lifecycle');
        const url = isLifecycle ? '/api/download/lifecycle' : '/api/download/balances';
        const originalText = btn.textContent;

        btn.style.opacity = "0.6";
        btn.textContent = '⏳ Generating...';

        fetch(url)
            .then(res => {
                if (!res.ok) return res.json().then(d => { throw new Error(d.error || 'Download failed'); });
                return res.blob();
            })
            .then(blob => {
                const objUrl = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = objUrl;
                a.download = isLifecycle
                    ? `Lifecycle List ${new Date().toISOString().slice(0, 10)}.xlsx`
                    : `Balances ${new Date().toISOString().slice(0, 10)}.xlsx`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(objUrl);
            })
            .catch(err => alert('Download error: ' + err.message))
            .finally(() => {
                btn.style.opacity = "1";
                btn.textContent = originalText;
            });
    });
});

// TEST button — show dataset picker, prefill uploads, then flow through Stage 2 normally
(async () => {
    const btn = document.getElementById('testBtn');
    const picker = document.getElementById('testDatasetPicker');

    // Load available datasets from server
    try {
        const res = await fetch('/api/test-datasets');
        const data = await res.json();
        if (data.status === 'success' && data.datasets.length > 0) {
            picker.innerHTML = '';
            for (const ds of data.datasets) {
                const opt = document.createElement('option');
                opt.value = ds.id;
                opt.textContent = `${ds.label}  (${ds.psp_count} PSPs)`;
                picker.appendChild(opt);
            }
        }
    } catch (_) {
        // leave default option
    }

    btn.addEventListener('click', async () => {
        const datasetId = picker.value;
        if (!datasetId) return alert('Please select a test dataset.');

        btn.textContent = '⏳ Loading test data...';
        btn.disabled = true;

        try {
            const res = await fetch('/api/test-prefill', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dataset_id: datasetId })
            });
            const data = await res.json();

            if (data.status !== 'success') {
                alert('Test load failed: ' + (data.error || JSON.stringify(data)));
                return;
            }

            // --- Populate file inputs from the manifest ---
            btn.textContent = '⏳ Fetching files...';
            const manifest = data.file_manifest || {};

            // Helper: fetch a file from uploads and return a File object
            async function fetchUploadedFile(saved, original) {
                const r = await fetch(`/api/uploads/${encodeURIComponent(saved)}`);
                const blob = await r.blob();
                return new File([blob], original, { type: blob.type });
            }

            // Single-file inputs
            for (const [inputId, key] of [
                ['platformFile',      'platformFile'],
                ['equityFile',        'equityFile'],
                ['transactionsFile',  'transactionsFile'],
                ['openingBalanceFile','openingBalanceFile'],
            ]) {
                const entry = manifest[key];
                const el = document.getElementById(inputId);
                if (!el || !entry) continue;
                const file = await fetchUploadedFile(entry.saved, entry.original);
                const dt = new DataTransfer();
                dt.items.add(file);
                el.files = dt.files;
                // Update the visible label if it exists
                const label = el.previousElementSibling;
                if (label && label.tagName === 'LABEL') {
                    label.style.color = '#34d399';
                }
            }

            // Bank files (multiple)
            const bankEntries = manifest.bankFiles || [];
            if (bankEntries.length > 0) {
                const bankEl = document.getElementById('bankFile');
                const dt = new DataTransfer();
                for (const entry of bankEntries) {
                    const file = await fetchUploadedFile(entry.saved, entry.original);
                    dt.items.add(file);
                }
                bankEl.files = dt.files;
            }

        } catch (err) {
            alert('Test load error: ' + err.message);
        } finally {
            btn.textContent = '⚡ Load Test Data';
            btn.disabled = false;
        }
    });
})();

// Live FX Rates
(async () => {
    const row = document.getElementById('fxRatesRow');
    try {
        const res = await fetch('/api/rates');
        const data = await res.json();

        if (data.status === 'success') {
            row.style.display = 'grid';
            row.style.gridTemplateColumns = 'repeat(auto-fill, minmax(90px, 1fr))';
            row.style.gap = '8px';
            row.innerHTML = '';

            const sorted = Object.entries(data.rates).sort((a, b) => a[0].localeCompare(b[0]));
            for (const [currency, rate] of sorted) {
                const formatted = rate >= 100 ? rate.toFixed(1) : rate.toFixed(4);
                row.innerHTML += `
                    <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 6px 4px; border-radius: 6px;">
                        <div style="color: #e2e8f0; font-weight: 700; font-size: 13px;">${formatted}</div>
                        <div style="color: #64748b; font-size: 10px;">${currency}</div>
                    </div>
                `;
            }

            try {
                const cryptoRes = await fetch('/api/rates/crypto');
                const cryptoData = await cryptoRes.json();
                if (cryptoData.status === 'success') {
                    row.innerHTML += `
                        <div style="grid-column: 1 / -1; border-top: 1px solid rgba(245, 158, 11, 0.3); margin-top: 4px; padding-top: 8px;">
                            <span style="color: #f59e0b; font-weight: 600; font-size: 12px;">⚡ Crypto (USD value)</span>
                        </div>
                    `;
                    const cryptoSorted = Object.entries(cryptoData.rates).sort((a, b) => a[0].localeCompare(b[0]));
                    for (const [coin, price] of cryptoSorted) {
                        const fmt = price >= 100 ? price.toLocaleString('en-US', { maximumFractionDigits: 0 }) : price.toFixed(2);
                        row.innerHTML += `
                            <div style="text-align: center; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); padding: 6px 4px; border-radius: 6px;">
                                <div style="color: #fbbf24; font-weight: 700; font-size: 13px;">$${fmt}</div>
                                <div style="color: #92400e; font-size: 10px;">${coin}</div>
                            </div>
                        `;
                    }
                }
            } catch (e) {
                row.innerHTML += `
                    <div style="grid-column: 1 / -1; text-align: center; color: #f59e0b; font-size: 11px; padding: 4px;">
                        ⚡ Crypto rates unavailable
                    </div>
                `;
            }
        } else {
            row.innerHTML = '<span style="color: #f87171; font-size: 13px;">⚠ ' + data.message + '</span>';
        }
    } catch (err) {
        row.innerHTML = '<span style="color: #f87171; font-size: 13px;">⚠ Could not reach backend. Is Flask running?</span>';
    }
})();
