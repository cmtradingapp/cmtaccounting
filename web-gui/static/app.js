function updateBankFileList() {
    const input = document.getElementById('bankFile');
    const list  = document.getElementById('bankFileList');
    const label = input.previousElementSibling;
    const files = Array.from(input.files || []);
    if (files.length === 0) {
        list.classList.add('hidden');
        if (label) label.style.color = '';
        return;
    }
    if (label) label.style.color = '#34d399';
    list.classList.remove('hidden');
    const header = `<div class="file-list-header">
        <span class="file-list-header-count">${files.length} file${files.length !== 1 ? 's' : ''}</span>
    </div>`;
    list.innerHTML = header + files.map((f, i) => {
        const ext = f.name.split('.').pop().toLowerCase();
        const extClass = ['csv','xlsx','xls'].includes(ext) ? ext : 'other';
        const stem = f.name.slice(0, f.name.lastIndexOf('.')) || f.name;
        return `<div class="file-list-item">
            <span class="file-list-idx">${String(i + 1).padStart(2, '0')}</span>
            <span class="file-list-ext ${extClass}">${ext}</span>
            <span class="file-list-name" title="${f.name}">${stem}</span>
        </div>`;
    }).join('');
}

document.getElementById('bankFile').addEventListener('change', updateBankFileList);

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

            mappingZone.querySelector('.subtitle').innerText = data.message + ' — Detecting column schema...';
            mappingCards.innerHTML = '<div style="color: #94a3b8; padding: 20px; text-align: center;"><span class="spinner"></span> Detecting column schema...</div>';
            mappingCards.style.gridTemplateColumns = '1fr';
            mappingZone.classList.remove('hidden');

            // Optional: LLM enhancement via OpenRouter (highlights join key + amount cols)
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
                // LLM unavailable — showing raw detected columns
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
                        <strong style="color:#c4b5fd;">LLM suggestion:</strong> ${aiMapping.join_explanation}
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
            const total = Math.max(s.total_crm_rows, s.total_bank_rows);
            const rate  = total > 0 ? (s.total_matched / total * 100) : 0;
            const rateStr = rate.toFixed(1) + '%';

            // Match rate hero
            const rateEl  = document.getElementById('statMatchRate');
            const barFill  = document.getElementById('reconBarFill');
            rateEl.textContent = rateStr;
            rateEl.style.color = rate >= 95 ? 'var(--success)' : rate >= 80 ? 'var(--warning)' : 'var(--danger)';
            setTimeout(() => { barFill.style.width = rate + '%'; }, 50);
            barFill.style.background = rate >= 95 ? 'var(--success)' : rate >= 80 ? 'var(--warning)' : 'var(--danger)';
            document.getElementById('reconBarMatchedLabel').textContent =
                `${s.total_matched.toLocaleString()} matched`;
            document.getElementById('reconBarUnmatchedLabel').textContent =
                `${(s.crm_unmatched + s.bank_unmatched).toLocaleString()} unmatched`;

            // Badge
            const badge = document.getElementById('resultsBadge');
            badge.textContent = rate >= 95 ? 'Clean' : rate >= 80 ? 'Review needed' : 'Issues found';
            badge.style.background = rate >= 95 ? 'rgba(16,185,129,0.2)' : rate >= 80 ? 'rgba(245,158,11,0.2)' : 'rgba(239,68,68,0.2)';
            badge.style.color = rate >= 95 ? '#34d399' : rate >= 80 ? '#fbbf24' : '#f87171';

            // CRM side
            document.getElementById('statCrmTotal').textContent   = s.total_crm_rows.toLocaleString();
            document.getElementById('statMatched').textContent     = s.total_matched.toLocaleString();
            document.getElementById('statCrmOnly').textContent     = s.crm_unmatched.toLocaleString();

            // Bank side
            document.getElementById('statBankTotal').textContent   = s.total_bank_rows.toLocaleString();
            document.getElementById('statBankMatched').textContent = s.total_matched.toLocaleString();
            document.getElementById('statBankOnly').textContent    = s.bank_unmatched.toLocaleString();

            // Unreconciled amount
            const amtBar = document.getElementById('reconAmountBar');
            const amtVal = document.getElementById('statFees');
            const fees = s.unrecon_fees;
            amtVal.textContent = '$' + Math.abs(fees).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
            if (fees === 0) amtBar.classList.add('clean');

            // Unmatched CRM breakdown by TRX type
            const breakdownWrap = document.getElementById('unmatchedBreakdown');
            const breakdownRows = document.getElementById('unmatchedBreakdownRows');
            const breakdown = s.unmatched_trx_breakdown || {};
            const breakdownEntries = Object.entries(breakdown);

            // Human-readable labels for TRX type codes
            const TRX_LABELS = {
                '2. DP': 'Deposit', '2. WD': 'Withdrawal',
                '4. Transfer': 'Transfer', '5. Bonuses': 'Bonus',
                '5. Fees/Charges': 'Fee/Charge', '5. Fee Compensation': 'Fee Comp.',
                '5. Realised Commissions': 'Commission', '5. IB Payment': 'IB Payment',
                '5. Platform Balances': 'Platform Bal.', '5. Realised Profits': 'Realised P&L',
                '5. Unrealised Profits': 'Unrealised P&L', '5. Realized Storage': 'Storage',
            };

            if (breakdownEntries.length > 0) {
                const PSP_TYPES = new Set(['2. DP', '2. WD']);
                const pspCount  = breakdownEntries.filter(([k]) => PSP_TYPES.has(k)).reduce((a, [, v]) => a + v, 0);
                const nonPspCount = s.crm_unmatched - pspCount;
                const note = pspCount === 0
                    ? `All ${s.crm_unmatched.toLocaleString()} are internal (no PSP gap)`
                    : `${pspCount.toLocaleString()} possible PSP gap · ${nonPspCount.toLocaleString()} internal`;
                document.getElementById('unmatchedBreakdownNote').textContent = note;

                breakdownRows.innerHTML = breakdownEntries.map(([type, count]) => {
                    const isPsp = PSP_TYPES.has(type);
                    const label = TRX_LABELS[type] || type;
                    return `<div class="recon-breakdown-chip${isPsp ? ' psp' : ''}" ` +
                        `data-trx="${encodeURIComponent(type)}" title="Click to inspect rows" style="cursor:pointer;">` +
                        `<span class="recon-breakdown-chip-label">${label}</span>` +
                        `<span class="recon-breakdown-chip-count">${count.toLocaleString()}</span>` +
                        `<span class="recon-breakdown-chip-caret">›</span>` +
                        `</div>`;
                }).join('');

                // Chip click → fetch and display rows
                let activeType = null;
                breakdownRows.addEventListener('click', async (e) => {
                    const chip = e.target.closest('[data-trx]');
                    if (!chip) return;
                    const trxType = decodeURIComponent(chip.dataset.trx);
                    const detail  = document.getElementById('unmatchedDetail');
                    if (activeType === trxType) {
                        // toggle off
                        detail.classList.add('hidden');
                        chip.classList.remove('active');
                        activeType = null;
                        return;
                    }
                    // deactivate previous
                    breakdownRows.querySelectorAll('.recon-breakdown-chip.active')
                        .forEach(c => c.classList.remove('active'));
                    chip.classList.add('active');
                    activeType = trxType;
                    document.getElementById('unmatchedDetailTitle').textContent = 'Loading…';
                    detail.classList.remove('hidden');

                    try {
                        const res  = await fetch(`/api/unmatched-crm?trx_type=${encodeURIComponent(trxType)}`);
                        const data = await res.json();
                        if (data.error) throw new Error(data.error);
                        const label = TRX_LABELS[trxType] || trxType;
                        document.getElementById('unmatchedDetailTitle').textContent =
                            `${label} (${trxType}) — ${data.count.toLocaleString()} unmatched CRM rows${data.count >= 500 ? '  (showing first 500)' : ''}`;

                        const cols = data.columns;
                        const rows = data.rows;
                        const thead = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>`;
                        const tbody = `<tbody>${rows.map(r =>
                            `<tr>${r.map(v => `<td title="${v ?? ''}">${v ?? '—'}</td>`).join('')}</tr>`
                        ).join('')}</tbody>`;
                        document.getElementById('unmatchedDetailTable').innerHTML = thead + tbody;
                    } catch (err) {
                        document.getElementById('unmatchedDetailTitle').textContent = `Error: ${err.message}`;
                    }
                });

                document.getElementById('unmatchedDetailClose').addEventListener('click', () => {
                    document.getElementById('unmatchedDetail').classList.add('hidden');
                    breakdownRows.querySelectorAll('.recon-breakdown-chip.active')
                        .forEach(c => c.classList.remove('active'));
                    activeType = null;
                });

                breakdownWrap.classList.remove('hidden');
            } else {
                breakdownWrap.classList.add('hidden');
            }

            // Technical details (small, at the bottom)
            document.getElementById('joinKeyInfo').textContent =
                `Join key: ${s.join_keys_used}  ·  Deal No column: ${s.deal_no_column}`;

            document.getElementById('resultsZone').classList.remove('hidden');
        } else {
            alert("Reconciliation error: " + (data.summary?.error || "Unknown error"));
        }
    } catch (err) {
        alert("Mathematical Reconciliation failed.");
    } finally {
        btn.innerHTML = `Run Reconciliation <span class="arrow">→</span>`;
    }
});

// Download buttons — fetch from backend and trigger browser save
document.querySelectorAll('.btn.download').forEach(btn => {
    btn.addEventListener('click', async () => {
        const isLifecycle = btn.querySelector('strong')?.textContent.includes('Lifecycle')
            || btn.textContent.includes('Lifecycle');
        const url = isLifecycle ? '/api/download/lifecycle' : '/api/download/balances';
        const originalHTML = btn.innerHTML;

        btn.style.opacity = "0.6";
        btn.textContent = '⏳ Generating...';

        try {
            const res = await fetch(url);
            if (!res.ok) {
                const d = await res.json();
                throw new Error(d.error || 'Download failed');
            }
            const blob = await res.blob();
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
        } catch (err) {
            alert('Download error: ' + err.message);
        } finally {
            btn.style.opacity = "1";
            btn.innerHTML = originalHTML;
        }
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
                updateBankFileList();
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
