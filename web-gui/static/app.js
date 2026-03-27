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

            mappingZone.querySelector('.subtitle').innerText = data.message;
            mappingCards.innerHTML = '';
            mappingCards.style.gridTemplateColumns = '1fr';

            for (const [source, info] of Object.entries(data.sources)) {
                const colTags = info.columns.map(c =>
                    `<span style="background: rgba(59,130,246,0.15); color: #93c5fd; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-family: monospace;">${c}</span>`
                ).join(' ');

                mappingCards.innerHTML += `
                    <div class="mapping-card" style="flex-direction: column; align-items: flex-start; gap: 10px; border-left-color: #10b981;">
                        <div style="display: flex; justify-content: space-between; width: 100%; align-items: center;">
                            <span class="m-value" style="font-size: 15px;">${source}</span>
                            <span style="color: #64748b; font-size: 12px;">${info.filename}</span>
                        </div>
                        <div style="display: flex; flex-wrap: wrap; gap: 6px;">${colTags}</div>
                    </div>
                `;
            }

            mappingZone.classList.remove('hidden');
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

// Live FX Rates
(async function loadFxRates() {
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
