const API_URL = "http://localhost:8000/api";

function formatDate(isoString) {
    if (!isoString) return "-";
    const d = new Date(isoString);
    const time = d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const date = d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });
    return `${time} (${date})`;
}

function timeAgo(isoString) {
    if (!isoString) return "";
    const seconds = Math.floor((new Date() - new Date(isoString)) / 1000);

    let interval = seconds / 3600;
    if (interval > 1) return Math.floor(interval) + "h ago";
    interval = seconds / 60;
    if (interval > 1) return Math.floor(interval) + "m ago";
    return Math.floor(seconds) + "s ago";
}

// Helper for formatting values
function formatCompactMoney(val) {
    if (val === undefined || val === null) return "-";
    return "$" + val.toLocaleString('en-US', { notation: "compact", maximumFractionDigits: 2 });
}

function formatMoney(val) {
    if (val === undefined || val === null) return "-";
    return "$" + val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPercent(val) {
    if (val === undefined || val === null) return "-";
    return (val * 100).toFixed(4) + "%";
}

function formatNum(val) {
    if (val === undefined || val === null) return "-";
    return val.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// --- Data Fetching (Dashboard) ---

async function fetchStatus() {
    try {
        // Fetch latest log entry
        const res = await fetch(`${API_URL}/view/logs?limit=1&_t=${Date.now()}`);
        if (!res.ok) throw new Error("API Error");

        const responseData = await res.json();
        const data = (responseData.data && responseData.data.length > 0) ? responseData.data[0] : null;

        const badge = document.getElementById("connection-status");
        badge.textContent = "Online";
        badge.className = "status-badge online";

        if (data) {
            document.getElementById("last-run-time").textContent = formatDate(data.execution_time) + " (" + timeAgo(data.execution_time) + ")";

            const statusEl = document.getElementById("last-run-status");
            statusEl.textContent = data.status || "UNKNOWN";
            if (data.status === "SUCCESS") statusEl.className = "value text-success";
            else if (data.status === "ERROR") statusEl.className = "value text-error";
            else statusEl.className = "value";

            document.getElementById("last-run-msg").textContent = data.message || "";
        } else {
            document.getElementById("last-run-time").textContent = "No logs yet";
            document.getElementById("last-run-status").textContent = "-";
            document.getElementById("last-run-msg").textContent = "-";
        }

    } catch (e) {
        console.error(e);
        const badge = document.getElementById("connection-status");
        badge.textContent = "Offline";
        badge.className = "status-badge offline";
    }
}

async function fetchDataFreshness() {
    try {
        const res = await fetch(`${API_URL}/market-data/latest?_t=${Date.now()}`);
        if (!res.ok) return;
        const data = await res.json();

        const tbody = document.getElementById("data-table-body");
        tbody.innerHTML = "";

        data.forEach(item => {
            const tr = document.createElement("tr");

            // Structure: {ts: "...", o:..., h:..., l:..., c:..., v:...} or null
            const ohlcv = item.OHLCV || {};
            const fund = item.Funding || { ts: null, val: null };
            const oi = item["Open Interest"] || { ts: null, val: null };

            // OHLCV Display
            let ohlcvDisplay = "-";
            if (ohlcv.c !== undefined) {
                ohlcvDisplay = `
                    <div style="font-size: 0.85em; display: grid; grid-template-columns: 1fr 1fr; gap: x; text-align: left; line-height: 1.4;">
                        <div><span class="text-dim">O:</span> ${formatMoney(ohlcv.o)}</div>
                        <div><span class="text-dim">H:</span> ${formatMoney(ohlcv.h)}</div>
                        <div><span class="text-dim">L:</span> ${formatMoney(ohlcv.l)}</div>
                        <div><span class="text-dim">C:</span> ${formatMoney(ohlcv.c)}</div>
                        <div style="grid-column: span 2;"><span class="text-dim">V:</span> ${formatCompactMoney(ohlcv.v)}</div>
                    </div>
                 `;
            }

            // Calculate OI in USD
            let oiDisplay = "-";
            if (oi.val !== null && ohlcv.c !== undefined) {
                const oiUsd = oi.val * ohlcv.c;
                oiDisplay = `
                    <span title="${formatMoney(oiUsd)}">${formatCompactMoney(oiUsd)}</span>
                    <div style="font-size: 0.85em; opacity: 0.7;">${formatNum(oi.val)} ${item.asset}</div>
                `;
            } else if (oi.val !== null) {
                oiDisplay = formatNum(oi.val);
            }

            tr.innerHTML = `
                <td><strong>${item.asset}</strong></td>
                <td>
                    <div style="margin-bottom: 5px;">${formatDate(ohlcv.ts)} <span class="text-dim">(${timeAgo(ohlcv.ts)})</span></div>
                    <div class="data-value text-accent" style="font-family: 'Courier New', monospace;">${ohlcvDisplay}</div>
                </td>
                <td>
                    <div>${formatDate(fund.ts)} <span class="text-dim">(${timeAgo(fund.ts)})</span></div>
                    <div class="data-value text-accent">${formatPercent(fund.val)}</div>
                </td>
                <td>
                    <div>${formatDate(oi.ts)} <span class="text-dim">(${timeAgo(oi.ts)})</span></div>
                    <div class="data-value text-accent">${oiDisplay}</div>
                </td>
            `;
            tbody.appendChild(tr);
        });

    } catch (e) {
        console.error(e);
    }
}

async function fetchSchedule() {
    try {
        const res = await fetch(`${API_URL}/schedule?_t=${Date.now()}`);
        const data = await res.json();

        const input = document.getElementById("schedule-interval");
        const inputTime = document.getElementById("schedule-start-time");
        const btn = document.getElementById("btn-toggle-schedule");
        const nextRun = document.getElementById("schedule-next-run");

        // Persisted config (always valid regardless of active state)
        if (data.interval_minutes) input.value = data.interval_minutes;
        inputTime.value = data.start_time || "";  // Set start time or clear if empty

        if (data.active) {
            btn.textContent = "Disable Schedule";
            btn.className = "btn-primary danger";
            nextRun.textContent = formatDate(data.next_run) + " (" + timeAgo(data.next_run) + ")";
            btn.onclick = () => disableSchedule();
        } else {
            btn.textContent = "Enable Schedule";
            btn.className = "btn-primary";
            nextRun.textContent = "Not Scheduled";
            btn.onclick = () => enableSchedule();
        }
    } catch (e) {
        console.error(e);
    }
}

async function enableSchedule() {
    const inputInt = document.getElementById("schedule-interval");
    const inputTime = document.getElementById("schedule-start-time");

    const interval = parseInt(inputInt.value);
    const startTime = inputTime.value;

    if (!interval || interval < 1) return alert("Invalid interval");

    const payload = { interval_minutes: interval, active: true };
    if (startTime) payload.start_time = startTime;

    try {
        const res = await fetch(`${API_URL}/schedule`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        if (res.ok) fetchSchedule();
    } catch (e) { console.error(e); }
}

async function disableSchedule() {
    try {
        const res = await fetch(`${API_URL}/schedule`, {
            method: "DELETE"
        });
        if (res.ok) fetchSchedule();
    } catch (e) { console.error(e); }
}

function toggleSchedule() {
    const btn = document.getElementById("btn-toggle-schedule");
    if (btn.textContent.includes("Disable")) {
        disableSchedule();
    } else {
        enableSchedule();
    }
}

async function saveScheduleSettings() {
    const inputInt = document.getElementById("schedule-interval");
    const inputTime = document.getElementById("schedule-start-time");

    const interval = parseInt(inputInt.value);
    const startTime = inputTime.value;

    if (!interval || interval < 1) return alert("Invalid interval");

    // Check if currently active based on toggle button state
    const btn = document.getElementById("btn-toggle-schedule");
    const isActive = btn.textContent.includes("Disable");

    // If active, we update with active=true (reschedule).
    // If inactive, we update with active=false (just config).
    // However, backend might need to support active=false update.
    // If backend DELETEs on disable, then POST with active=false might re-create it but set to inactive?
    // Let's assume POST supports update.

    const payload = { interval_minutes: interval, active: isActive };
    if (startTime) payload.start_time = startTime;

    try {
        const res = await fetch(`${API_URL}/schedule`, {
            method: "POST", // POST updates or creates
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        if (res.ok) {
            alert("Settings Saved");
            fetchSchedule();
        } else {
            alert("Error saving settings");
        }
    } catch (e) { console.error(e); }
}

// --- Data View Logic (Navigation) ---

let currentView = 'dashboard';
let viewState = {
    page: 1,
    limit: 50,
    asset: ''
};

function switchView(viewName) {
    currentView = viewName;

    // Update Sidebar
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    // Map view to index: dashboard=0, ohlcv=1, funding=2, oi=3, logs=4
    const indices = { 'dashboard': 0, 'ohlcv': 1, 'funding': 2, 'oi': 3, 'logs': 4 };
    const navItems = document.querySelectorAll('.nav-items .nav-item');
    if (indices[viewName] !== undefined && navItems[indices[viewName]]) {
        navItems[indices[viewName]].classList.add('active');
    }

    // Update Main Content
    document.getElementById('view-dashboard').classList.add('hidden');
    document.getElementById('view-dashboard').classList.remove('active');
    document.getElementById('view-data').classList.add('hidden');
    document.getElementById('view-data').classList.remove('active');

    if (viewName === 'dashboard') {
        document.getElementById('view-dashboard').classList.remove('hidden');
        document.getElementById('view-dashboard').classList.add('active');
        // Trigger immediate update
        fetchStatus();
        fetchDataFreshness();
        fetchSchedule();
    } else {
        document.getElementById('view-data').classList.remove('hidden');
        document.getElementById('view-data').classList.add('active');
        document.getElementById('data-view-title').textContent = getViewTitle(viewName);

        // Hide Asset filter for Logs
        const assetFilterGroup = document.getElementById('view-asset-filter').parentElement;
        if (assetFilterGroup) {
            assetFilterGroup.style.display = (viewName === 'logs') ? 'none' : 'flex';
        }

        // Reset Page
        viewState.page = 1;
        document.getElementById('page-indicator').textContent = `Page ${viewState.page}`;
        refreshDataView();
    }
}

function getViewTitle(view) {
    const titles = {
        'ohlcv': 'OHLCV Data',
        'funding': 'Funding Rates',
        'oi': 'Open Interest History',
        'logs': 'System Execution Logs'
    };
    return titles[view] || 'Data View';
}

function changePage(delta) {
    const newPage = viewState.page + delta;
    if (newPage < 1) return;
    viewState.page = newPage;
    document.getElementById('page-indicator').textContent = `Page ${viewState.page}`;
    refreshDataView();
}

async function refreshDataView() {
    if (currentView === 'dashboard') return;

    const asset = document.getElementById('view-asset-filter').value;
    const limit = document.getElementById('view-limit-filter').value;

    viewState.limit = parseInt(limit);
    viewState.asset = asset;

    const tableHead = document.getElementById('data-view-head');
    const tableBody = document.getElementById('data-view-body');
    tableBody.innerHTML = '<tr><td colspan="10" style="text-align:center; padding: 2rem;">Loading...</td></tr>';

    try {
        let url = `${API_URL}/view/${currentView}?page=${viewState.page}&limit=${viewState.limit}&_t=${Date.now()}`;
        if (asset && currentView !== 'logs') url += `&asset=${asset}`;

        const res = await fetch(url);
        if (!res.ok) throw new Error("Fetch failed");

        const json = await res.json();

        // Update Page Indicator
        if (json.pages) {
            document.getElementById('page-indicator').textContent = `Page ${viewState.page} of ${json.pages}`;
        } else {
            document.getElementById('page-indicator').textContent = `Page ${viewState.page}`;
        }

        renderTable(json.data, tableHead, tableBody);

    } catch (e) {
        tableBody.innerHTML = `<tr><td colspan="10" style="text-align:center; color: var(--error);">Error: ${e.message}</td></tr>`;
        console.error(e);
    }
}

function renderTable(data, thead, tbody) {
    thead.innerHTML = '';
    tbody.innerHTML = '';

    if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;">No data found</td></tr>';
        return;
    }

    // Headers
    const cols = Object.keys(data[0]);
    let tr = document.createElement('tr');
    cols.forEach(col => {
        let th = document.createElement('th');
        th.textContent = col.toUpperCase();
        tr.appendChild(th);
    });
    thead.appendChild(tr);

    // Rows
    data.forEach(row => {
        let tr = document.createElement('tr');
        cols.forEach(col => {
            let td = document.createElement('td');
            let val = row[col];

            // Format Timestamps
            if ((col.includes('time') || col === 'ts') && val) {
                val = new Date(val).toLocaleString();
            }

            td.textContent = val !== null ? val : '-';
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}


// --- Settings & Cleanup ---

async function loadSettings() {
    try {
        const res = await fetch(`${API_URL}/settings`);
        const data = await res.json();

        const dateInput = document.getElementById("setting-start-date");
        if (data.target_start_date) {
            dateInput.value = data.target_start_date;
        }
    } catch (e) {
        console.error("Failed to load settings", e);
    }
}

async function saveSettings() {
    const dateInput = document.getElementById("setting-start-date");
    const val = dateInput.value;

    if (!val) {
        alert("Please enter a valid date.");
        return;
    }

    try {
        const res = await fetch(`${API_URL}/settings`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ key: "target_start_date", value: val })
        });

        if (res.ok) alert("Settings saved.");
        else alert("Failed to save settings.");
    } catch (e) {
        alert("Error saving settings: " + e);
        console.error(e);
    }
}

async function runGapAnalysis() {
    const resultsContainer = document.getElementById("gap-results");
    resultsContainer.classList.remove("hidden");
    resultsContainer.innerHTML = '<div style="padding:10px; text-align:center;">Analyzing... <br> (This may take a few seconds)</div>';

    try {
        const res = await fetch(`${API_URL}/gaps`);
        const json = await res.json();

        // Structure: { gaps: { "ETH": [ {start, end, desc, type}, ... ] } }
        resultsContainer.innerHTML = "";

        const assets = Object.keys(json.gaps);
        if (assets.length === 0) {
            resultsContainer.innerHTML = '<div class="text-success" style="padding:10px;">No gaps found!</div>';
            return;
        }

        assets.forEach(asset => {
            const list = json.gaps[asset];
            if (list.length > 0) {
                const group = document.createElement("div");
                group.style.marginBottom = "1rem";
                group.innerHTML = `<strong class="text-accent">${asset}</strong>`;

                list.forEach(gap => {
                    const div = document.createElement("div");
                    div.className = "gap-item"; // Need to style this
                    div.style.fontSize = "0.85em";
                    div.style.padding = "4px 0";
                    div.style.borderBottom = "1px solid var(--border)";

                    // Format dates
                    const start = new Date(gap.start).toLocaleString();
                    const end = gap.end === "NOW" ? "Now" : new Date(gap.end).toLocaleString();

                    div.innerHTML = `
                        <span class="text-error">[${gap.type}]</span> 
                        ${gap.desc} <br>
                        <span class="text-dim">${start} ➔ ${end}</span>
                    `;
                    group.appendChild(div);
                });
                resultsContainer.appendChild(group);
            }
        });

    } catch (e) {
        resultsContainer.innerHTML = `<div class="text-error" style="padding:10px;">Analysis Failed: ${e}</div>`;
        console.error(e);
    }
}

function toggleSettings() {
    const modal = document.getElementById("settings-modal");
    if (modal) {
        modal.classList.toggle("hidden");
        if (!modal.classList.contains("hidden")) {
            loadSettings();
        }
    }
}

async function cleanupData(target) {
    const tableMap = {
        "ohlcv": "OHLCV (Candles)",
        "funding": "Funding Rates",
        "oi": "Open Interest",
        "logs": "System Logs",
        "all": "ALL DATA (Everything)"
    };

    const name = tableMap[target] || target;
    const confirmMsg = target === 'all'
        ? `⚠️ DANGER ZONE ⚠️\n\nAre you sure you want to DELETE ALL DATA?\nThis will wipe the entire database.\n\nType 'DELETE' to confirm.`
        : `Are you sure you want to DELETE ${name}?\nThis action cannot be undone.`;

    if (target === 'all') {
        const input = prompt(confirmMsg);
        if (input !== 'DELETE') return;
    } else {
        if (!confirm(confirmMsg)) return;
    }

    try {
        const res = await fetch(`${API_URL}/cleanup?target=${target}`, { method: 'DELETE' });
        const data = await res.json();

        if (res.ok) {
            alert("Success: " + data.message);
            // Refresh current view if applicable
            if (currentView === 'dashboard') fetchDataFreshness();
            else refreshDataView();
        } else {
            alert("Error: " + data.detail);
        }
    } catch (e) {
        alert("Request Failed: " + e);
        console.error(e);
    }
}

// --- Init ---

function init() {
    // Start on Dashboard
    fetchStatus();
    fetchDataFreshness();
    fetchSchedule();

    // Auto refresh loop
    setInterval(() => {
        if (currentView === 'dashboard') {
            fetchStatus();
            fetchDataFreshness();
            fetchSchedule();
        }
    }, 5000);
}

init();
