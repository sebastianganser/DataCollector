const API_URL = "/api";

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

const SCHEDULE_API_URL = '/api/schedule';

let cmEditor = null; // Global CodeMirror instance

async function fetchOutputStats() {
    try {
        const response = await fetch(`${API_URL}/output/stats`);
        const data = await response.json();
        const el = document.getElementById('output-file-count');
        if (el) {
            el.textContent = data.count !== undefined ? data.count : '-';
        }
    } catch (e) {
        console.error("Failed to fetch output stats:", e);
    }
}


// --- Switching Views ---
function switchView(viewName) {
    currentView = viewName;

    // Update Sidebar
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    // Map view to index: dashboard=0, ohlcv=1, funding=2, oi=3, logs=4, upload=5
    // Note: Assuming Upload is the 6th item (index 5) based on HTML structure
    // Let's rely on finding by function call or text rather than hardcoded index if possible, 
    // but the current code uses indices. Let's fix the indices map.
    const indices = { 'dashboard': 0, 'ohlcv': 1, 'funding': 2, 'oi': 3, 'logs': 4, 'upload': 5 };
    const navItems = document.querySelectorAll('.nav-items .nav-item');
    if (indices[viewName] !== undefined && navItems[indices[viewName]]) {
        navItems[indices[viewName]].classList.add('active');
    }

    // Hide All Views first
    const views = ['view-dashboard', 'view-data', 'view-upload'];
    views.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.classList.add('hidden');
            el.classList.remove('active');
        }
    });

    // Show Selected View
    if (viewName === 'dashboard') {
        const el = document.getElementById('view-dashboard');
        el.classList.remove('hidden');
        el.classList.add('active');
        // Trigger immediate update
        fetchStatus();
        fetchDataFreshness();
        fetchSchedule();
    } else if (viewName === 'upload') {
        const el = document.getElementById('view-upload');
        el.classList.remove('hidden');
        el.classList.add('active');
        fetchOutputStats();
    } else {
        // Generic Data Views (ohlcv, funding, etc.)
        const el = document.getElementById('view-data');
        el.classList.remove('hidden');
        el.classList.add('active');
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
        "episodes": "Market Episodes (Sync Data)",
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

// --- File Upload Logic ---

let currentCorrectionFile = null;

function initUploadLogic() {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const statusDiv = document.getElementById('upload-status');
    const browseBtn = dropZone.querySelector('.browse-btn');

    // Click to Browse
    if (browseBtn) {
        browseBtn.addEventListener('click', () => fileInput.click());
    }

    // Input Change
    if (fileInput) {
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFileUpload(e.target.files[0]);
            }
        });
    }

    if (dropZone) {
        // Drag Over
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });

        // Drag Leave
        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
        });

        // Drop
        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            if (e.dataTransfer.files.length > 0) {
                handleFileUpload(e.dataTransfer.files[0]);
            }
        });
    }

    window.handleFileUpload = async function (file) {
        if (file.type !== "application/json" && !file.name.endsWith(".json")) {
            statusDiv.innerHTML = '<span class="text-error">Invalid file format. Please upload a JSON file.</span>';
            return;
        }

        statusDiv.innerHTML = '<span class="text-dim">Uploading and Processing...</span>';

        const formData = new FormData();
        formData.append('file', file);

        try {
            const res = await fetch(`${API_URL}/upload`, {
                method: 'POST',
                body: formData
            });

            const result = await res.json();

            if (res.ok && result.status === 'success') {
                statusDiv.innerHTML = `<span class="text-success">✅ ${result.message}</span> <button class="btn btn-primary" onclick="processFile('${file.name}')" style="margin-left:10px;">Process</button>`;
            } else if (result.status === 'validation_error') {
                // Calculate line numbers for better usability
                let formattedContent = "";
                try {
                    // We need to read the file content to map lines
                    // Since specific file object is not easily available here without reading input again
                    // We will fetch the content from server just like openCorrectionModal does
                    // OR we can read the file from the input if we have access to 'file' variable which is available in parent scope

                    const text = await file.text();
                    const jsonObj = JSON.parse(text);
                    formattedContent = JSON.stringify(jsonObj, null, 4);
                } catch (e) {
                    // If parse fails, we can't map lines, just use raw
                    formattedContent = "";
                }

                let errorHtml = '<span class="text-error">Validation Failed:</span><ul>';

                result.errors.forEach(err => {
                    let displayErr = err;
                    if (formattedContent) {
                        const line = findLineNumberForError(err, formattedContent);
                        if (line !== -1) {
                            // Replace "Episode X" with "Line Y" or prepend Line Y
                            // "Episode 0: Field 'created_at'..." -> "Line 36: Field 'created_at'..."
                            displayErr = err.replace(/Episode \d+:/, `Line ${line + 1}:`);
                            if (displayErr === err) {
                                displayErr = `Line ${line + 1}: ${err}`;
                            }
                        }
                    }
                    errorHtml += `<li>${displayErr}</li>`;
                });
                errorHtml += '</ul>';

                // Add "Fix" button
                if (result.filename) {
                    errorHtml += `<button class="btn btn-primary" onclick="openCorrectionModal('${result.filename}')" style="margin-top:1rem;">Fix Issues</button>`;
                }

                statusDiv.innerHTML = errorHtml;

                // Store errors globally for the editor to use later
                window.currentValidationErrors = result.errors;

            } else {
                statusDiv.innerHTML = `<span class="text-error">Error: ${result.message}</span>`;
            }

        } catch (e) {
            statusDiv.innerHTML = `<span class="text-error">Upload Error: ${e.message}</span>`;
            console.error(e);
        }
    }
}

// Helper to find line number (shared logic)
function findLineNumberForError(err, content) {
    const lines = content.split('\n');
    let lineIdx = -1;

    const epMatch = err.match(/Episode (\d+):/);
    if (epMatch) {
        const epIndex = parseInt(epMatch[1]);
        let currentEp = -1;
        let insideEpisodes = false;
        let braceDepth = 0;

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            if (line.includes('"episodes":') && line.includes('[')) {
                insideEpisodes = true;
                continue;
            }
            if (insideEpisodes) {
                if (braceDepth === 0 && line.trim().startsWith(']')) {
                    insideEpisodes = false;
                    continue;
                }
                const openBraces = (line.match(/{/g) || []).length;
                const closeBraces = (line.match(/}/g) || []).length;

                if (line.includes('{')) {
                    if (braceDepth === 0) currentEp++;
                    braceDepth += openBraces;
                }

                if (currentEp === epIndex && braceDepth >= 1) {
                    const fieldMatch = err.match(/Field '([^']+)'/);
                    if (fieldMatch) {
                        const fieldName = fieldMatch[1];
                        if (line.includes(`"${fieldName}"`)) {
                            lineIdx = i;
                            break;
                        }
                    } else if (line.includes('{') && braceDepth === 1) {
                        if (lineIdx === -1) lineIdx = i;
                    }
                }
                if (line.includes('}')) braceDepth -= closeBraces;
            }
        }
    } else if (err.includes("Top-level field")) {
        const fieldMatch = err.match(/field '([^']+)'/);
        if (fieldMatch) {
            const fieldName = fieldMatch[1];
            for (let i = 0; i < lines.length; i++) {
                if (lines[i].includes(`"${fieldName}"`)) {
                    lineIdx = i;
                    break;
                }
            }
        }
    }
    return lineIdx;
}

// --- Correction Logic ---

window.openCorrectionModal = function (filename) {
    currentCorrectionFile = filename;
    const modal = document.getElementById('correction-modal');
    modal.classList.remove('hidden');

    document.getElementById('correction-filename').textContent = `Fix File: ${filename}`;

    // Clear previous errors in modal
    const errorList = document.getElementById('correction-errors');
    errorList.innerHTML = '';

    // Fetch content
    fetch(`${API_URL}/file/${filename}`)
        .then(res => {
            if (!res.ok) throw new Error("Failed to load file");
            return res.json(); // Get wrapper object { filename, content }
        })
        .then(data => {
            // content is in data.content
            let fileContent = data.content;

            // Try to pretty-print
            try {
                const jsonObj = JSON.parse(fileContent);
                fileContent = JSON.stringify(jsonObj, null, 4);
            } catch (e) {
                console.warn("Could not pretty-print JSON", e);
            }

            // Initialize CodeMirror if not already done
            if (!cmEditor) {
                const textarea = document.getElementById('json-editor');
                cmEditor = CodeMirror.fromTextArea(textarea, {
                    mode: "application/json",
                    theme: "dracula",
                    lineNumbers: true,
                    gutters: ["CodeMirror-linenumbers", "error-gutter"]
                });
            }

            cmEditor.setValue(fileContent);

            // If we have stored errors from the upload attempt, show them
            if (window.currentValidationErrors) {
                highlightErrors(window.currentValidationErrors);

                window.currentValidationErrors.forEach(err => {
                    const el = document.createElement('div');
                    el.className = 'error-item';
                    el.textContent = `• ${err}`;
                    errorList.appendChild(el);
                });
            }

            // Refresh editor to display correctly
            setTimeout(() => cmEditor.refresh(), 50);
        })
        .catch(err => {
            console.error(err);
            errorList.innerHTML = `<div class="error-item">Error loading file: ${err.message}</div>`;
        });
}

function highlightErrors(errors) {
    if (!cmEditor) return;
    cmEditor.clearGutter("error-gutter"); // Clear old markers

    // Map errors to lines
    // Error formats:
    // "Episode X: Field 'Y' cannot be empty"
    // "Top-level field 'Y' cannot be empty"

    const content = cmEditor.getValue();
    const lines = content.split('\n');

    errors.forEach(err => {
        let lineIdx = -1;

        // 1. Episode Errors
        const epMatch = err.match(/Episode (\d+):/);
        if (epMatch) {
            const epIndex = parseInt(epMatch[1]);
            // Find the Nth occurrence of "{" inside "episodes" array
            // This is a naive regex approach but might work for standard formatting.
            // Better: Parse JSON AST? Too heavy for pure JS frontend without libraries?
            // Let's try a heuristic: find "episodes": [ then count {

            // Heuristic A: Look for "event_id" if mentioned?
            // Heuristic B: Just highlight the opening brace of that episode?

            // Let's try to find the start of the episode object.
            let currentEp = -1;
            let insideEpisodes = false;

            for (let i = 0; i < lines.length; i++) {
                if (lines[i].includes('"episodes":')) {
                    insideEpisodes = true;
                    continue;
                }
                if (insideEpisodes) {
                    if (lines[i].includes('{')) {
                        currentEp++;
                        if (currentEp === epIndex) {
                            lineIdx = i;
                            // Try to refine: if error mentions a field, find that field inside this object block
                            const fieldMatch = err.match(/Field '([^']+)'/);
                            if (fieldMatch) {
                                const fieldName = fieldMatch[1];
                                // Search forward for this field until next '}' or '{'
                                for (let j = i; j < lines.length; j++) {
                                    if (lines[j].includes('}')) break; // End of object
                                    if (lines[j].includes(`"${fieldName}"`)) {
                                        lineIdx = j;
                                        break;
                                    }
                                }
                            }
                            break;
                        }
                    }
                    if (lines[i].includes(']')) {
                        insideEpisodes = false;
                    }
                }
            }
        }

        // 2. Top-level errors
        else if (err.includes("Top-level field")) {
            const fieldMatch = err.match(/field '([^']+)'/);
            if (fieldMatch) {
                const fieldName = fieldMatch[1];
                for (let i = 0; i < lines.length; i++) {
                    if (lines[i].includes(`"${fieldName}"`)) {
                        lineIdx = i;
                        break;
                    }
                }
            }
        }

        // Apply Marker
        if (lineIdx !== -1) {
            const marker = document.createElement("div");
            marker.style.color = "#da3633";
            marker.innerHTML = "●"; // Red dot
            marker.title = err;
            cmEditor.setGutterMarker(lineIdx, "error-gutter", marker);

            // Also line highlighting (background) if desired, but gutter is requested "roter senkrechter strich"
            // Let's stick to gutter marker as requested. 
            // Actually user asked for "roten senkrechten strich". 
            // "border-left" on line class.
            cmEditor.addLineClass(lineIdx, "gutter", "error-gutter-line"); // We can style this class in CSS? 
            // Actually setGutterMarker replaces the content. 
            // Let's just use the red dot for now, or a styled div.
            const bar = document.createElement("div");
            bar.style.height = "100%";
            bar.style.width = "4px";
            bar.style.backgroundColor = "#da3633";
            cmEditor.setGutterMarker(lineIdx, "error-gutter", bar);
        }
    });
}

window.closeCorrectionModal = function () {
    document.getElementById('correction-modal').classList.add('hidden');
    currentCorrectionFile = null;
    if (cmEditor) {
        cmEditor.clearGutter("error-gutter"); // Clear markers when closing
    }
    window.currentValidationErrors = null; // Clear stored errors
}

window.saveCurrentFile = function () {
    if (!currentCorrectionFile) return;

    const content = cmEditor.getValue(); // Use CodeMirror content
    const errorDiv = document.getElementById('correction-errors');
    errorDiv.innerHTML = "Validating...";

    fetch(`${API_URL}/file/${currentCorrectionFile}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: content })
    })
        .then(async res => {
            const result = await res.json();

            if (res.ok && result.status === 'success') {
                closeCorrectionModal();
                // Update main status
                const statusDiv = document.getElementById('upload-status');
                if (statusDiv) statusDiv.innerHTML = `<span class="text-success">✅ ${result.message}</span>`;
                alert("File fixed and saved!");
            } else if (result.status === 'validation_error') {
                let errorHtml = '<span>Validation Failed:</span><ul>';
                result.errors.forEach(err => errorHtml += `<li>${err}</li>`);
                errorHtml += '</ul>';
                errorDiv.innerHTML = errorHtml;

                // Store and highlight new errors
                window.currentValidationErrors = result.errors;
                highlightErrors(result.errors);

            } else {
                errorDiv.innerHTML = `Error: ${result.message}`;
            }
        })
        .catch(e => {
            errorDiv.innerHTML = `System Error: ${e.message}`;
        });
}

window.deleteCurrentFile = function () {
    if (!currentCorrectionFile) return;
    if (!confirm("Are you sure you want to delete this file?")) return;

    fetch(`${API_URL}/file/${currentCorrectionFile}`, {
        method: 'DELETE'
    })
        .then(async res => {
            const result = await res.json();
            if (res.ok) {
                closeCorrectionModal();
                const statusDiv = document.getElementById('upload-status');
                if (statusDiv) statusDiv.innerHTML = `<span class="text-dim">File deleted. Ready for new upload.</span>`;
            } else {
                alert("Error deleting file: " + result.message);
            }
        })
        .catch(e => {
            alert("Error: " + e.message);
        });
}

// --- Process Logic ---
// --- Process Logic (Custom Modal) ---
let fileToProcess = null;

window.processFile = function (filename) {
    fileToProcess = filename;
    document.getElementById('confirm-msg').innerText = `Start processing for ${filename}?`;
    document.getElementById('confirm-modal').style.display = 'flex';
}

window.closeConfirmModal = function () {
    document.getElementById('confirm-modal').style.display = 'none';
    fileToProcess = null;
}

document.getElementById('confirm-btn-yes').onclick = async function () {
    if (!fileToProcess) return;

    const filename = fileToProcess;
    closeConfirmModal(); // Close first

    const statusDiv = document.getElementById('upload-status');
    statusDiv.innerHTML = `<span class="text-info">Processing...</span>`;

    try {
        const res = await fetch(`${API_URL}/process/${filename}`, { method: 'POST' });
        const result = await res.json();

        if (res.ok) {
            statusDiv.innerHTML = `<span class="text-success">✅ ${result.message}</span>`;
        } else {
            statusDiv.innerHTML = `<span class="text-error">Process Failed: ${result.detail || result.message}</span>`;
        }
    } catch (e) {
        statusDiv.innerHTML = `<span class="text-error">Network Error: ${e.message}</span>`;
    }
}

// --- n8n Sync Workflow ---

async function toggleSync() {
    const btn = document.getElementById('btn-sync-toggle');
    // Check current state from button class
    const isRunning = btn.classList.contains('btn-danger'); // Red means running/stop

    try {
        let endpoint = isRunning ? '/api/sync/stop' : '/api/sync/start';
        const response = await fetch(endpoint, { method: 'POST' });
        const result = await response.json();

        if (result.status === 'started' || result.status === 'already_running') {
            updateSyncUI(true);
        } else if (result.status === 'stopped' || result.status === 'not_running') {
            updateSyncUI(false);
        }

    } catch (error) {
        console.error("Sync Toggle Error:", error);
    }
}

function updateSyncUI(isRunning) {
    const btn = document.getElementById('btn-sync-toggle');
    const statusDiv = document.getElementById('sync-status-indicator');

    if (!btn || !statusDiv) return;

    if (isRunning) {
        btn.textContent = "Stop Sync";
        btn.className = "btn btn-danger"; // Red for stop
        statusDiv.textContent = "Running";
        statusDiv.style.borderColor = "var(--success)";
        statusDiv.style.color = "var(--success)";
    } else {
        btn.textContent = "Start Sync";
        btn.className = "btn btn-secondary"; // Default
        statusDiv.textContent = "Stopped";
        statusDiv.style.borderColor = "var(--border)";
        statusDiv.style.color = "var(--text-dim)";
    }
}

async function checkSyncStatus() {
    try {
        const response = await fetch(`${API_URL}/sync/status?_t=${Date.now()}`);
        const data = await response.json();
        updateSyncUI(data.running);
    } catch (e) {
        console.warn("Sync status check failed", e);
    }
}

// --- Init ---


function init() {
    // Start on Dashboard
    fetchStatus();
    fetchDataFreshness();
    fetchSchedule();
    checkSyncStatus();

    // Init Modules
    initUploadLogic();

    // Auto refresh loop
    setInterval(() => {
        if (currentView === 'dashboard') {
            fetchStatus();
            fetchDataFreshness();
            fetchSchedule();
        }
        checkSyncStatus();
    }, 5000);
}

init();
