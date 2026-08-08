let currentTaskId = null;
let selectedTaskId = null;
const streamSequenceByTask = {};
let eventSource = null;
let timerInterval = null;
let seconds = 0;
let estimatedTokens = 0;
let aiBuffer = ""; 
let currentAiDiv = null; 
let currentThinkingBox = null;
let currentTextBox = null;
let thinkingBuffer = "";
window.isUserScrolledUp = false;
let currentActiveAgent = 'supervisor';
let supBuffer = "";
let workerBuffer = "";
let reviewerBuffer = "";
const agentTraceState = {
    supervisor: { output: "", outputNode: null, thinkingNode: null, thinkingChars: 0 },
    worker: { output: "", outputNode: null, thinkingNode: null, thinkingChars: 0 },
    reviewer: { output: "", outputNode: null, thinkingNode: null, thinkingChars: 0 },
};
let usedAccounts = new Set();
let awaitingChatReply = false;
let dashboardRefreshTimer = null;

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function renderSafeMarkdown(text) {
    const source = String(text ?? "");
    const html = typeof marked !== "undefined" ? marked.parse(source) : escapeHtml(source);
    return typeof DOMPurify !== "undefined"
        ? DOMPurify.sanitize(html, { USE_PROFILES: { html: true } })
        : escapeHtml(source);
}

function stripProtocolEnvelope(text) {
    return String(text ?? "")
        .replace(
            /```json\s*\{[\s\S]*?(?:"_action"|"_tool_name")[\s\S]*?\}\s*```/gi,
            ""
        )
        .replace(
            /\{\s*"_(?:action|tool_name)"\s*:[\s\S]*?\}\s*$/i,
            ""
        )
        .trim();
}

function cleanAgentDisplayText(value) {
    const text = String(value ?? "");
    const normalized = text.toLowerCase();
    const refusalMarkers = [
        'không có quyền truy cập',
        'không thể truy cập',
        'filesystem/venv',
        'sandbox claude',
        'chat interface',
        'no access to',
        'cannot access',
        "can't access",
    ];
    return refusalMarkers.some((marker) => normalized.includes(marker))
        ? 'Đã ẩn phản hồi capability/protocol cũ; controller chịu trách nhiệm I/O và machine gate.'
        : text;
}

function normalizeAgentRole(role) {
    return ['supervisor', 'worker', 'reviewer'].includes(role)
        ? role
        : currentActiveAgent;
}

function getAgentTraceElement(role) {
    const ids = {
        supervisor: 'sup-thinking-box',
        worker: 'worker-thinking-box',
        reviewer: 'reviewer-stream',
    };
    return document.getElementById(ids[normalizeAgentRole(role)]);
}

function getAgentStageBadge(role) {
    const ids = {
        supervisor: 'supervisor-stage-badge',
        worker: 'worker-stage-badge',
        reviewer: 'reviewer-verdict-badge',
    };
    return document.getElementById(ids[normalizeAgentRole(role)]);
}

function setAgentStage(role, stage, active = true) {
    const badge = getAgentStageBadge(role);
    if (!badge) return;
    badge.textContent = String(stage || 'IDLE').toUpperCase();
    badge.classList.toggle('is-active', active);
}

function appendAgentTrace(role, message, kind = 'progress', meta = '') {
    const normalizedRole = normalizeAgentRole(role);
    const lane = getAgentTraceElement(normalizedRole);
    if (!lane || !message) return null;
    lane.querySelector('.agent-idle')?.remove();
    const entry = document.createElement('div');
    entry.className = `agent-trace-entry agent-trace-${kind}`;
    if (meta) {
        const metaNode = document.createElement('span');
        metaNode.className = 'agent-trace-meta';
        metaNode.textContent = meta;
        entry.appendChild(metaNode);
    }
    const content = document.createElement('span');
    content.className = 'agent-trace-content';
    content.textContent = String(message);
    entry.appendChild(content);
    lane.appendChild(entry);
    while (lane.children.length > 80) lane.firstElementChild.remove();
    lane.scrollTop = lane.scrollHeight;
    return entry;
}

function resetAgentAttempt(role) {
    const state = agentTraceState[normalizeAgentRole(role)];
    if (!state) return;
    state.output = "";
    state.outputNode = null;
    state.thinkingNode = null;
    state.thinkingChars = 0;
}

function appendAgentOutput(role, chunk) {
    const normalizedRole = normalizeAgentRole(role);
    const state = agentTraceState[normalizedRole];
    if (!state || !chunk) return;
    state.output = (state.output + chunk).slice(-12000);
    const displayText = normalizedRole === 'worker'
        ? `Đang soạn patch · ${state.output.length.toLocaleString()} ký tự`
        : state.output;
    if (!state.outputNode || !state.outputNode.isConnected) {
        state.outputNode = appendAgentTrace(
            normalizedRole,
            displayText,
            'output',
            'live output'
        );
    }
    const content = state.outputNode?.querySelector('.agent-trace-content');
    if (content) {
        content.textContent = displayText;
        const lane = getAgentTraceElement(normalizedRole);
        if (lane) lane.scrollTop = lane.scrollHeight;
    }
}

function updateAgentThinking(role, chunk) {
    const normalizedRole = normalizeAgentRole(role);
    const state = agentTraceState[normalizedRole];
    if (!state || !chunk) return;
    state.thinkingChars += Array.from(chunk).length;
    if (!state.thinkingNode || !state.thinkingNode.isConnected) {
        state.thinkingNode = appendAgentTrace(
            normalizedRole,
            'Đang phân tích dữ liệu được giao...',
            'progress',
            'reasoning activity'
        );
    }
    const content = state.thinkingNode?.querySelector('.agent-trace-content');
    if (content) {
        content.textContent = `Đang phân tích dữ liệu được giao · ${state.thinkingChars.toLocaleString()} ký tự xử lý`;
    }
}

function finalizeAgentOutput(role) {
    const normalizedRole = normalizeAgentRole(role);
    const state = agentTraceState[normalizedRole];
    if (!state?.outputNode) return;
    const content = state.outputNode.querySelector('.agent-trace-content');
    const clean = stripProtocolEnvelope(state.output);
    if (content) {
        content.textContent = normalizedRole === 'worker'
            ? `Đã hoàn tất patch · ${state.output.length.toLocaleString()} ký tự`
            : (clean || 'Đã tạo action có cấu trúc.');
    }
}

function rejectAgentOutput(role, reason = '') {
    const state = agentTraceState[normalizeAgentRole(role)];
    if (!state?.outputNode) return;
    state.outputNode.classList.remove('agent-trace-output');
    state.outputNode.classList.add('agent-trace-retry');
    const meta = state.outputNode.querySelector('.agent-trace-meta');
    const content = state.outputNode.querySelector('.agent-trace-content');
    if (meta) meta.textContent = 'rejected output';
    if (content) {
        content.textContent = reason
            ? `Đã ẩn phản hồi sai schema: ${reason}`
            : 'Đã ẩn phản hồi sai schema.';
    }
}

function resetAllAgentTraces() {
    const idleText = {
        supervisor: 'Đang chờ checkpoint...',
        worker: 'Đang chờ ticket từ Supervisor...',
        reviewer: 'Đang chờ patch vượt machine gate...',
    };
    Object.keys(agentTraceState).forEach((role) => {
        resetAgentAttempt(role);
        const lane = getAgentTraceElement(role);
        if (lane) {
            lane.replaceChildren();
            const idle = document.createElement('div');
            idle.className = 'agent-idle';
            idle.textContent = idleText[role];
            lane.appendChild(idle);
        }
        setAgentStage(role, 'idle', false);
    });
}

function setSystemState(label) {
    const ticker = document.getElementById("system-ticker");
    if (ticker) ticker.innerText = `SYSTEM: ${label}`;
}

function isChatMode() {
    return document.querySelector('input[name="mode"]:checked')?.value === 'chat';
}

function setModeRadio(mode) {
    document.querySelectorAll('input[name="mode"]').forEach((input) => {
        input.checked = input.value === mode;
    });
    if (typeof updateModeUI === 'function') updateModeUI();
}

window.handleRunClick = function() {
    if (awaitingChatReply && currentTaskId) {
        return window.sendReply();
    }
    return window.createTask();
};

window.openTaskSettings = function() {
    document.body.classList.add('dashboard-mode');
    document.body.classList.remove('task-detail-mode');
    document.getElementById('dashboard-home')?.classList.add('hidden');
    const drawer = document.getElementById('task-settings-drawer');
    drawer?.classList.remove('hidden');
    drawer?.classList.add('flex');
    document.getElementById('task')?.focus();
};

window.showCleanDashboard = function() {
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    selectedTaskId = null;
    currentTaskId = null;
    clearInterval(timerInterval);
    document.body.classList.add('dashboard-mode');
    document.body.classList.remove('chat-mode', 'task-detail-mode');
    document.getElementById('btn-back-dashboard')?.classList.add('hidden');
    document.getElementById('selected-task-summary')?.classList.add('hidden');
    document.getElementById('execution-result-panel')?.classList.add('hidden');

    const streamBox = document.getElementById('live-stream');
    if (streamBox) {
        streamBox.replaceChildren();
        const welcome = document.createElement('div');
        welcome.className = 'flex flex-col items-center justify-center h-full text-center text-gray-600';
        const title = document.createElement('h2');
        title.className = 'text-sm font-bold text-gray-400';
        title.textContent = 'Task Dashboard';
        const text = document.createElement('p');
        text.className = 'text-[11px] mt-2 max-w-md';
        text.textContent = 'Chọn một task để mở workspace, hoặc tạo task mới.';
        welcome.append(title, text);
        streamBox.appendChild(welcome);
    }
    refreshTaskDashboard();
};

window.closeTaskSettings = function() {
    const drawer = document.getElementById('task-settings-drawer');
    drawer?.classList.add('hidden');
    drawer?.classList.remove('flex');
    document.getElementById('dashboard-home')?.classList.remove('hidden');
};

window.openSelectedTaskSettings = async function() {
    if (!selectedTaskId) {
        openTaskSettings();
        return;
    }
    try {
        const response = await fetch(`/api/tasks/${selectedTaskId}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const task = await response.json();
        const settings = task.settings || {};
        const setValue = (id, value) => {
            const element = document.getElementById(id);
            if (element && value != null) element.value = value;
        };
        setValue('task-name', `${task.name} (copy)`);
        setValue('task', task.prompt);
        setValue('root', task.root);
        setValue('files', (task.files || []).join(', '));
        setValue('model', settings.worker_model);
        setValue('effort', settings.worker_effort);
        setValue('supervisor-model', settings.supervisor_model);
        setValue('supervisor-effort', settings.supervisor_effort);
        setValue('reviewer-model', settings.reviewer_model);
        setValue('reviewer-effort', settings.reviewer_effort);
        setValue('account-mode', settings.account_mode);
        setValue('test-cmd', settings.test_cmd || '');
        setValue('max-turns', settings.max_turns || 25);
        setValue('project-mode', settings.project_mode || 'edit');
        const autoApply = document.getElementById('auto-apply');
        const createZip = document.getElementById('create-zip');
        const autoContinue = document.getElementById('auto-continue');
        if (autoApply) autoApply.checked = settings.auto_apply !== false;
        if (createZip) createZip.checked = settings.create_zip !== false;
        if (autoContinue) autoContinue.checked = settings.auto_continue === true;
        setModeRadio(task.mode);
        if (typeof updateProjectModeUI === 'function') updateProjectModeUI();
        openTaskSettings();
    } catch (error) {
        addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'ERR', String(error), 'error');
    }
};

function statusClass(status) {
    return `task-status-${String(status || 'queued').toLowerCase()}`;
}

function formatTaskTime(value) {
    if (!value) return '';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '' : date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
}

function isTaskActiveStatus(status) {
    return new Set([
        'QUEUED', 'RUNNING', 'PLANNING', 'CODING',
        'REVIEWING', 'REVISION', 'WAITING_INPUT', 'STOPPING', 'RESUMING',
    ]).has(status);
}

function isTaskResumable(task) {
    return task.mode === 'orchestrator' && new Set([
        'STOPPED', 'FAILED', 'MAX_TURNS', 'INTERRUPTED',
    ]).has(task.status);
}

window.refreshTaskDashboard = async function() {
    const list = document.getElementById('task-dashboard-list');
    if (!list) return;
    try {
        const response = await fetch('/api/tasks');
        const data = await response.json();
        list.replaceChildren();
        if (!data.tasks?.length) {
            const empty = document.createElement('div');
            empty.className = 'text-[10px] text-gray-600 italic text-center mt-8';
            empty.textContent = 'Chưa có task. Bấm + TASK để bắt đầu.';
            list.appendChild(empty);
            return;
        }
        data.tasks.forEach((task) => {
            const card = document.createElement('div');
            card.className = `dashboard-task ${task.id === selectedTaskId ? 'active' : ''}`;

            const name = document.createElement('div');
            name.className = 'dashboard-task-name';
            name.textContent = task.name;

            const meta = document.createElement('div');
            meta.className = 'dashboard-task-meta';
            const status = document.createElement('span');
            status.className = `task-status-badge ${statusClass(task.status)}`;
            status.textContent = task.status;
            const info = document.createElement('span');
            const fileCount = Object.keys(task.changed_files || {}).length;
            info.textContent = `${task.project_mode || task.mode} · ${fileCount} files · ${formatTaskTime(task.updated_at)}`;
            meta.append(status, info);

            const actions = document.createElement('div');
            actions.className = 'mt-2 flex items-center gap-2';
            const openButton = document.createElement('button');
            openButton.type = 'button';
            openButton.className = 'text-[9px] font-bold text-cyan-400 hover:text-cyan-200';
            openButton.textContent = 'MỞ';
            openButton.onclick = () => selectDashboardTask(task.id);
            actions.appendChild(openButton);

            if (isTaskActiveStatus(task.status)) {
                const stopButton = document.createElement('button');
                stopButton.type = 'button';
                stopButton.className = 'text-[9px] font-bold text-amber-400 hover:text-amber-200';
                stopButton.textContent = 'DỪNG';
                stopButton.onclick = () => stopDashboardTask(task.id);
                actions.appendChild(stopButton);
            } else {
                if (isTaskResumable(task)) {
                    const resumeButton = document.createElement('button');
                    resumeButton.type = 'button';
                    resumeButton.className = 'text-[9px] font-bold text-emerald-400 hover:text-emerald-200';
                    resumeButton.textContent = 'TIẾP TỤC';
                    resumeButton.onclick = () => resumeDashboardTask(task.id);
                    actions.appendChild(resumeButton);
                }
                const deleteButton = document.createElement('button');
                deleteButton.type = 'button';
                deleteButton.className = 'text-[9px] font-bold text-rose-500 hover:text-rose-300';
                deleteButton.textContent = 'XÓA';
                deleteButton.onclick = () => deleteDashboardTask(task.id, task.name);
                actions.appendChild(deleteButton);
            }

            card.append(name, meta, actions);
            list.appendChild(card);
        });
    } catch (error) {
        list.textContent = `Không tải được dashboard: ${error}`;
    }
};

window.stopDashboardTask = async function(taskId) {
    try {
        const response = await fetch(`/api/stop/${taskId}`, { method: 'POST' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        await refreshTaskDashboard();
    } catch (error) {
        alert(`Không thể dừng task: ${error}`);
    }
};

window.resumeDashboardTask = async function(taskId) {
    try {
        const response = await fetch(
            `/api/tasks/${taskId}/resume`,
            { method: 'POST' }
        );
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || `HTTP ${response.status}`);
        }
        streamSequenceByTask[taskId] = 0;
        await refreshTaskDashboard();
        await selectDashboardTask(taskId);
    } catch (error) {
        alert(`Không thể tiếp tục task: ${error}`);
    }
};

window.deleteDashboardTask = async function(taskId, taskName) {
    if (!confirm(`Xóa task "${taskName}" khỏi Dashboard?`)) return;
    try {
        const response = await fetch(`/api/tasks/${taskId}`, { method: 'DELETE' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
        if (selectedTaskId === taskId) showCleanDashboard();
        delete streamSequenceByTask[taskId];
        await refreshTaskDashboard();
    } catch (error) {
        alert(`Không thể xóa task: ${error}`);
    }
};

function prepareTaskWorkspace(task) {
    aiBuffer = "";
    thinkingBuffer = "";
    supBuffer = "";
    workerBuffer = "";
    reviewerBuffer = "";
    currentActiveAgent = task.mode === "chat" ? "worker" : "supervisor";
    usedAccounts.clear();

    const streamBox = document.getElementById('live-stream');
    if (streamBox) {
        streamBox.replaceChildren();
        if (task.prompt) {
            const request = document.createElement('div');
            request.className = 'mb-6 p-4 glass-panel border-l-4 border-l-emerald-400 rounded-r-xl shadow-lg';
            request.innerHTML = `
                <span class="text-emerald-400 font-bold text-xs uppercase tracking-wider">Yêu cầu</span>
                <div class="markdown-body text-gray-200 mt-2">${renderSafeMarkdown(task.prompt)}</div>`;
            streamBox.appendChild(request);
        }
        currentAiDiv = document.createElement('div');
        currentAiDiv.className = 'mb-6 p-4 glass-panel border-l-4 border-l-cyan-500 rounded-r-xl shadow-lg relative';
        currentThinkingBox = document.createElement('details');
        currentThinkingBox.className = 'ai-thinking-box hidden mb-3';
        currentThinkingBox.innerHTML = `
            <summary class="text-[10px] text-cyan-600 font-bold uppercase tracking-widest cursor-pointer">
                <i class="fa-solid fa-microchip mr-1"></i> Quá trình suy nghĩ
            </summary>
            <div class="thinking-content mt-2 p-3 bg-black/40 rounded-lg border border-cyan-900/30 text-gray-500 font-mono text-[11px] whitespace-pre-wrap max-h-60 overflow-y-auto"></div>`;
        currentTextBox = document.createElement('div');
        currentTextBox.className = 'markdown-body text-gray-200';
        currentAiDiv.append(currentThinkingBox, currentTextBox);
        streamBox.appendChild(currentAiDiv);
    }

    resetAllAgentTraces();
    if (isTaskActiveStatus(task.status)) {
        appendAgentTrace(
            task.current_agent || 'supervisor',
            `Đang khôi phục task ở phase ${task.phase || 'unknown'}.`,
            'progress',
            'resume'
        );
        setAgentStage(task.current_agent || 'supervisor', task.phase || 'running');
    }
    if (task.last_worker_feedback) {
        appendAgentTrace(
            'worker',
            cleanAgentDisplayText(task.last_worker_feedback),
            'action',
            'last feedback'
        );
    }
    if (task.last_reviewer_feedback) {
        appendAgentTrace(
            'reviewer',
            cleanAgentDisplayText(task.last_reviewer_feedback),
            'action',
            'last verdict'
        );
    }
    document.getElementById('sup-todo-box')?.classList.add('hidden');
    document.getElementById('reviewer-result-box')?.classList.add('hidden');
    document.getElementById('execution-result-panel')?.classList.add('hidden');
    document.getElementById('sys-audit-logs')?.replaceChildren();
}

window.selectDashboardTask = async function(taskId) {
    try {
        const response = await fetch(`/api/tasks/${taskId}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const task = await response.json();
        selectedTaskId = task.id;
        prepareTaskWorkspace(task);
        renderSelectedTask(task);
        refreshTaskDashboard();
        if (eventSource) {
            eventSource.close();
            eventSource = null;
        }
        currentTaskId = null;

        if (isTaskActiveStatus(task.status)) {
            currentTaskId = task.id;
            streamSequenceByTask[task.id] = 0;
            setupEventSource();
        }
    } catch (error) {
        addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'ERR', String(error), 'error');
    }
};

function renderSelectedTask(task) {
    const summary = document.getElementById('selected-task-summary');
    const name = document.getElementById('selected-task-name');
    const meta = document.getElementById('selected-task-meta');
    const status = document.getElementById('selected-task-status');
    const files = document.getElementById('selected-task-files');
    if (!summary || !name || !meta || !status || !files) return;

    document.body.classList.remove('dashboard-mode');
    document.body.classList.add('task-detail-mode');
    document.getElementById('btn-back-dashboard')?.classList.remove('hidden');
    setModeRadio(task.mode);

    summary.classList.remove('hidden');
    name.textContent = task.name;
    const settings = task.settings || {};
    const turnLimit = settings.max_turns ? `/${settings.max_turns}` : '';
    meta.textContent = `${settings.project_mode || task.mode} · Turn ${task.turn_count || 0}${turnLimit} · ${settings.worker_model || settings.supervisor_model || ''} · ${task.phase || ''}`;
    status.textContent = task.status;
    status.className = `task-status-badge ${statusClass(task.status)}`;
    files.replaceChildren();
    const downloadLink = document.getElementById('artifact-download-link');
    if (downloadLink) {
        const hasZip = Boolean(task.artifact?.zip_path);
        downloadLink.classList.toggle('hidden', !hasZip);
        downloadLink.href = hasZip
            ? `/api/tasks/${task.id}/artifacts/download`
            : '#';
    }

    Object.entries(task.changed_files || {}).forEach(([path, stats]) => {
        const chip = document.createElement('span');
        chip.className = 'changed-file-chip';
        const pathText = document.createElement('span');
        pathText.textContent = `${path} `;
        const additions = document.createElement('span');
        additions.className = 'additions';
        additions.textContent = `+${stats.additions || 0}`;
        const deletions = document.createElement('span');
        deletions.className = 'deletions';
        deletions.textContent = ` −${stats.deletions || 0}`;
        chip.append(pathText, additions, deletions);
        files.appendChild(chip);
    });

    if (!files.children.length) {
        const empty = document.createElement('span');
        empty.className = 'text-[9px] text-gray-600';
        empty.textContent = 'Chưa có file được Reviewer duyệt.';
        files.appendChild(empty);
    }

    const streamBox = document.getElementById('live-stream');
    const activeStates = ['QUEUED', 'RUNNING', 'PLANNING', 'CODING', 'REVIEWING', 'REVISION', 'WAITING_INPUT', 'STOPPING', 'RESUMING'];
    if (streamBox && !activeStates.includes(task.status)) {
        streamBox.replaceChildren();
        const heading = document.createElement('div');
        heading.className = 'session-summary';
        const title = document.createElement('h3');
        title.textContent = `${task.name} · ${task.status}`;
        const detail = document.createElement('pre');
        detail.textContent = [
            task.last_execution_result ? `Execution: ${task.last_execution_result}` : '',
            task.last_worker_feedback ? `Worker: ${cleanAgentDisplayText(task.last_worker_feedback)}` : '',
            task.last_review_verdict ? `Review: ${task.last_review_verdict}` : '',
            task.last_reviewer_feedback ? `Tester: ${cleanAgentDisplayText(task.last_reviewer_feedback)}` : '',
            task.last_error ? `Error: ${task.last_error}` : '',
        ].filter(Boolean).join('\n\n');
        heading.append(title, detail);
        streamBox.appendChild(heading);
    }
}

async function refreshSelectedTaskSummary() {
    if (!selectedTaskId) return;
    const requestedTaskId = selectedTaskId;
    try {
        const response = await fetch(`/api/tasks/${requestedTaskId}`);
        if (!response.ok) return;
        const task = await response.json();
        if (selectedTaskId !== requestedTaskId) return;
        renderSelectedTask(task);
    } catch (_) {
        // Dashboard refresh is best-effort; SSE remains the primary channel.
    }
}

document.addEventListener('DOMContentLoaded', () => {
    showCleanDashboard();
    dashboardRefreshTimer = setInterval(refreshTaskDashboard, 5000);
});

// 1. Cấu hình Marked.js tích hợp Highlight.js
if (typeof marked !== 'undefined') {
    marked.setOptions({
        highlight: function(code, lang) {
            if (lang && typeof hljs !== 'undefined' && hljs.getLanguage(lang)) {
                return hljs.highlight(code, { language: lang }).value;
            }
            return typeof hljs !== 'undefined' ? hljs.highlightAuto(code).value : code;
        },
        breaks: true
    });
}

// 2. Bắt đầu Task Mới
window.createTask = async function() {
    const taskName = document.getElementById('task-name')?.value.trim() || null;
    const root = document.getElementById('root')?.value || "";
    const files = document.getElementById('files')?.value || "";
    const task = document.getElementById('task')?.value || "";
    const mode = document.querySelector('input[name="mode"]:checked')?.value || "chat";
    const model = document.getElementById('model')?.value || "claude-sonnet-5";
    const effort = document.getElementById('effort')?.value || "max";
    const supervisorModel = document.getElementById('supervisor-model')?.value || model;
    const supervisorEffort = document.getElementById('supervisor-effort')?.value || "max";
    const reviewerModel = document.getElementById('reviewer-model')?.value || model;
    const reviewerEffort = document.getElementById('reviewer-effort')?.value || "high";
    const projectMode = mode === 'orchestrator'
        ? (document.getElementById('project-mode')?.value || 'edit')
        : 'edit';
    const autoApply = document.getElementById('auto-apply')?.checked !== false;
    const createZip = document.getElementById('create-zip')?.checked !== false;
    const autoContinue = document.getElementById('auto-continue')?.checked === true;
    const accountMode = document.getElementById('account-mode')?.value || "sticky";
    const testCmd = document.getElementById('test-cmd')?.value.trim() || null;
    const maxTurns = Number.parseInt(
        document.getElementById('max-turns')?.value || '25',
        10
    );

    if(!task) { alert("Vui lòng nhập yêu cầu!"); return; }
    if (!Number.isInteger(maxTurns) || maxTurns < 1 || maxTurns > 500) {
        alert("Max Turns phải là số nguyên từ 1 đến 500.");
        return;
    }
    if(mode !== "chat" && !root) {
        alert("Vui lòng chọn project root trước khi chạy Orchestrator!");
        return;
    }

    resetUI();
    aiBuffer = "";
    thinkingBuffer = ""; // Reset buffer suy nghĩ
    estimatedTokens = calculateTokens(task);
    usedAccounts.clear(); 

    const streamBox = document.getElementById('live-stream');
    
    streamBox.innerHTML = `
        <div class="mb-6 p-4 glass-panel border-l-4 border-l-emerald-400 rounded-r-xl shadow-lg">
            <span class="text-emerald-400 font-bold text-xs uppercase tracking-wider">👤 Bạn:</span>
            <div class="markdown-body text-gray-200 mt-2">${renderSafeMarkdown(task)}</div>
        </div>`;

    // Khởi tạo bong bóng AI với 2 thành phần: Hộp suy nghĩ và Hộp Text
    currentAiDiv = document.createElement('div');
    currentAiDiv.className = "mb-6 p-4 glass-panel border-l-4 border-l-cyan-500 rounded-r-xl shadow-lg relative";

    currentThinkingBox = document.createElement('details');
    currentThinkingBox.className = "ai-thinking-box hidden mb-3";
    currentThinkingBox.innerHTML = `
        <summary class="text-[10px] text-cyan-600 font-bold uppercase tracking-widest cursor-pointer select-none outline-none hover:text-cyan-400 transition">
            <i class="fa-solid fa-microchip animate-pulse mr-1"></i> Quá trình suy nghĩ
        </summary>
        <div class="thinking-content mt-2 p-3 bg-black/40 rounded-lg border border-cyan-900/30 text-gray-500 font-mono text-[11px] whitespace-pre-wrap max-h-60 overflow-y-auto"></div>
    `;

    currentTextBox = document.createElement('div');
    currentTextBox.className = "markdown-body text-gray-200";

    currentAiDiv.appendChild(currentThinkingBox);
    currentAiDiv.appendChild(currentTextBox);
    streamBox.appendChild(currentAiDiv);

    let data;
    try {
        const res = await fetch('/api/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: taskName, root, task, files, mode, model, effort,
                supervisor_model: supervisorModel,
                supervisor_effort: supervisorEffort,
                reviewer_model: reviewerModel,
                reviewer_effort: reviewerEffort,
                project_mode: projectMode,
                auto_apply: autoApply,
                create_zip: createZip,
                account_mode: accountMode,
                test_cmd: testCmd,
                max_turns: maxTurns,
                auto_continue: autoContinue
            })
        });
        data = await res.json();
        if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
    } catch (error) {
        addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'ERR', String(error), 'error');
        finishUI();
        setSystemState("ERROR");
        return;
    }
    currentTaskId = data.task_id;
    selectedTaskId = data.task_id;
    streamSequenceByTask[data.task_id] = 0;
    document.body.classList.remove('dashboard-mode');
    document.body.classList.add('task-detail-mode');
    document.getElementById('btn-back-dashboard')?.classList.remove('hidden');
    awaitingChatReply = false;
    currentActiveAgent = mode === "chat" ? "worker" : "supervisor";
    
    if(document.getElementById('current-task-id')) {
        document.getElementById('current-task-id').innerText = `ID: ${currentTaskId}`;
    }

    startTimer();
    setSystemState("RUNNING");
    setupEventSource();
    closeTaskSettings();
    refreshTaskDashboard();
    refreshSelectedTaskSummary();
}

// 3. Gửi tin nhắn tiếp theo trong luồng Chat
window.sendReply = async function() {
    const msg = document.getElementById('task').value;
    if (!msg) return;
    removeCursor(currentTextBox);
    removeCursor(currentAiDiv);
    
    document.getElementById('task').value = "";
    document.getElementById('btn-run').classList.add('hidden');
    document.getElementById('btn-stop').classList.remove('hidden');
    
    aiBuffer = "";
    thinkingBuffer = ""; // Reset buffer suy nghĩ
    const streamBox = document.getElementById('live-stream');
    
    streamBox.insertAdjacentHTML('beforeend', `
        <div class="mb-6 mt-4 p-4 glass-panel border-l-4 border-l-emerald-400 rounded-r-xl shadow-lg">
            <span class="text-emerald-400 font-bold text-xs uppercase tracking-wider">👤 Bạn:</span>
            <div class="markdown-body text-gray-200 mt-2">${renderSafeMarkdown(msg)}</div>
        </div>`);
        
    // Khởi tạo bong bóng AI cho Chat
    currentAiDiv = document.createElement('div');
    currentAiDiv.className = "mb-6 p-4 glass-panel border-l-4 border-l-cyan-500 rounded-r-xl shadow-lg relative";

    currentThinkingBox = document.createElement('details');
    currentThinkingBox.className = "ai-thinking-box hidden mb-3";
    currentThinkingBox.innerHTML = `
        <summary class="text-[10px] text-cyan-600 font-bold uppercase tracking-widest cursor-pointer select-none outline-none hover:text-cyan-400 transition">
            <i class="fa-solid fa-microchip animate-pulse mr-1"></i> Quá trình suy nghĩ
        </summary>
        <div class="thinking-content mt-2 p-3 bg-black/40 rounded-lg border border-cyan-900/30 text-gray-500 font-mono text-[11px] whitespace-pre-wrap max-h-60 overflow-y-auto"></div>
    `;

    currentTextBox = document.createElement('div');
    currentTextBox.className = "markdown-body text-gray-200";

    currentAiDiv.appendChild(currentThinkingBox);
    currentAiDiv.appendChild(currentTextBox);
    streamBox.appendChild(currentAiDiv);
    
    scrollToBottom();
    
    try {
        const response = await fetch(`/api/chat/${currentTaskId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: msg })
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        awaitingChatReply = false;
        setSystemState("RUNNING");
    } catch (error) {
        addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'ERR', String(error), 'error');
        finishUI();
    }
} 

// 4. Xử lý Stream (SSE) từ Server
function setupEventSource() {
    if (eventSource) eventSource.close();
    const streamTaskId = currentTaskId;
    const after = streamSequenceByTask[streamTaskId] || 0;
    const source = new EventSource(`/api/stream/${streamTaskId}?after=${after}`);
    eventSource = source;
    const streamBox = document.getElementById('live-stream'); // Vùng giữa (Thợ)

    source.onopen = function() {
        setSystemState("CONNECTED | RUNNING");
    };

    source.onmessage = function(event) {
        let streamData;
        try {
            streamData = JSON.parse(event.data);
        } catch (error) {
            addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'WARN', 'SSE frame không hợp lệ', 'warn');
            return;
        }
        if (Number.isFinite(Number(streamData._seq))) {
            streamSequenceByTask[streamTaskId] = Math.max(
                streamSequenceByTask[streamTaskId] || 0,
                Number(streamData._seq)
            );
        }

        if (streamData.type === "protocol_retry") {
            const roleLabels = {
                supervisor: 'Supervisor',
                worker: 'Worker',
                reviewer: 'Reviewer',
            };
            const role = roleLabels[streamData.role] || streamData.role || 'Agent';
            const retryText = streamData.switching
                ? `${role} sai protocol ${streamData.attempt}/${streamData.max_attempts} — đang đổi account`
                : `${role} sai protocol — đang thử lại ${streamData.attempt}/${streamData.max_attempts}`;
            rejectAgentOutput(streamData.role, streamData.error || '');
            resetAgentAttempt(streamData.role);
            appendAgentTrace(
                streamData.role,
                retryText,
                'retry',
                streamData.reason || 'protocol repair'
            );
            setAgentStage(streamData.role, 'retry');
            if (streamData.role === 'supervisor') {
                supBuffer = "";
            } else if (streamData.role === 'worker') {
                workerBuffer = "";
                if (currentTextBox) currentTextBox.textContent = retryText;
            } else if (streamData.role === 'reviewer') {
                reviewerBuffer = "";
            }
            setSystemState(retryText);
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'RETRY',
                retryText,
                'warn'
            );
            return;
        }

        if (streamData.type === "auto_continue") {
            const message = `Auto Continue chu kỳ ${streamData.cycle} · tiếp tục từ turn ${streamData.turn_count}`;
            setSystemState(message);
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'CONTINUE',
                message,
                'info'
            );
            return;
        }

        if (streamData.type === "account_switch") {
            const roleLabels = {
                supervisor: 'Supervisor',
                worker: 'Worker',
                reviewer: 'Reviewer',
            };
            const role = roleLabels[streamData.role] || streamData.role || 'Agent';
            const cooldownHours = Number(streamData.cooldown_seconds || 0) / 3600;
            const cooldownText = cooldownHours
                ? `, nghỉ ${cooldownHours.toLocaleString()} giờ`
                : '';
            const message = streamData.reason === 'account_invalid'
                ? `${role}: ${streamData.from_account || 'account'} không còn hợp lệ, đã xóa cookie và đang chuyển account`
                : `${role}: ${streamData.from_account || 'account'} bị rate limit${cooldownText}, đang chuyển account`;
            setSystemState(message);
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                '↻',
                message,
                'warn'
            );
            return;
        }

        if (streamData.type === "file_fetched") {
            const path = streamData.file_path || '';
            const message = `Local file agent nạp: ${path}`;
            setSystemState(message);
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'FILE',
                message,
                'info'
            );
            return;
        }

        if (streamData.type === "agent_action") {
            const role = streamData.role || 'agent';
            let description = `${role} → ${streamData.action}`;
            if (streamData.action === 'delegate_task' && streamData.file_path) {
                description = `Supervisor giao ${streamData.file_path} → Worker`;
            } else if (streamData.action === 'request_context') {
                const files = streamData.files_needed || [];
                description = `Supervisor yêu cầu context: ${files.join(', ')}`;
            } else if (streamData.action === 'submit_patch') {
                description = `Worker nộp patch: ${streamData.file_path || ''}`;
            } else if (streamData.action === 'review_patch') {
                description = `Reviewer ${streamData.verdict || 'đã kiểm tra'}: ${streamData.file_path || ''}`;
            }
            setSystemState(description);
            finalizeAgentOutput(role);
            appendAgentTrace(role, description, 'action', `turn ${streamData.turn || '?'}`);
            setAgentStage(role, streamData.action || 'done', false);
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'ACTION',
                description,
                'info'
            );
            return;
        }
        
        // 1. NHẬN DIỆN AGENT QUA STATUS
        if (streamData.type === "status") {
            const statusText = String(streamData.data || "");
            const logTime = new Date().toLocaleTimeString('en-US', {hour12: false});
            const explicitRole = ['supervisor', 'worker', 'reviewer'].includes(streamData.role)
                ? streamData.role
                : null;
            
            if (isChatMode()) {
                currentActiveAgent = 'worker';
                addSysLog(logTime, 'CHAT', statusText);
            } else if (explicitRole) {
                currentActiveAgent = explicitRole;
                toggleAgentLEDs(explicitRole);
                setAgentStage(explicitRole, streamData.stage || 'calling model');
                appendAgentTrace(
                    explicitRole,
                    'Đã kết nối model; đang xử lý input của vai trò này.',
                    'progress',
                    streamData.stage || 'model call'
                );
                addSysLog(logTime, 'AGENT', `${explicitRole}: ${streamData.stage || 'running'}`);
            } else if (statusText.includes("Quản lý")) {
                currentActiveAgent = 'supervisor';
                toggleAgentLEDs('supervisor');
                addSysLog(logTime, '🟢', 'Supervisor đang lên kế hoạch...');
            } else if (statusText.includes("Thợ Code")) {
                currentActiveAgent = 'worker';
                toggleAgentLEDs('worker');
                addSysLog(logTime, '🚀', 'Đã khởi tạo Worker. Đang gõ code...');
            } else if (statusText.includes("Reviewer/Tester")) {
                currentActiveAgent = 'reviewer';
                toggleAgentLEDs('reviewer');
                addSysLog(logTime, 'TEST', 'Reviewer/Tester đang nghiệm thu patch...');
            } else {
                addSysLog(logTime, '⚙️', statusText);
            }
            const accountMatch = statusText.match(/:\s*([^\s]+)$/);
            if (accountMatch) {
                usedAccounts.add(accountMatch[1]);
                const counter = document.getElementById('acc-counter');
                if (counter) counter.innerText = String(usedAccounts.size);
            }
            setSystemState(statusText);
            return;
        }

        if (streamData.type === "agent_progress") {
            const role = normalizeAgentRole(streamData.role);
            currentActiveAgent = role;
            resetAgentAttempt(role);
            toggleAgentLEDs(role);
            setAgentStage(role, streamData.stage || 'running');
            appendAgentTrace(
                role,
                streamData.message || 'Đang xử lý...',
                'progress',
                `turn ${streamData.turn || '?'} · ${streamData.stage || 'running'}`
            );
            setSystemState(
                `${role.toUpperCase()} | ${(streamData.stage || 'running').toUpperCase()}`
            );
            return;
        }

        // 2. PHÂN LUỒNG RENDER TEXT
        if (streamData.type === "token" || streamData.type === "thinking") {
            const chunk = String(streamData.text || "");
            const role = normalizeAgentRole(streamData.role || currentActiveAgent);
            currentActiveAgent = role;
            estimatedTokens += calculateTokens(chunk);
            const tokenCounter = document.getElementById('token-counter');
            if (tokenCounter) tokenCounter.innerText = Math.round(estimatedTokens).toLocaleString();

            if (streamData.type === "thinking") {
                if (isChatMode() && currentThinkingBox) {
                    currentThinkingBox.classList.remove("hidden");
                    const thinkingContent = currentThinkingBox.querySelector(".thinking-content");
                    thinkingBuffer += chunk;
                    if (thinkingContent) thinkingContent.textContent = thinkingBuffer;
                } else {
                    updateAgentThinking(role, chunk);
                }
                return;
            }

            if (isChatMode()) {
                aiBuffer += chunk;
                if (currentTextBox) {
                    currentTextBox.innerHTML = renderSafeMarkdown(aiBuffer)
                        + '<span class="blinking-cursor"></span>';
                }
                if (!window.isUserScrolledUp) scrollToBottom();
                return;
            }

            appendAgentOutput(role, chunk);
            if (role === 'reviewer') {
                reviewerBuffer += chunk;
            } else if (role === 'supervisor') {
                supBuffer += chunk;
                extractAndRenderTodo(supBuffer);
            } else {
                workerBuffer += chunk;
                const fancyHTML = renderDiffFromPatch(stripProtocolEnvelope(workerBuffer));
                if (currentTextBox) {
                    currentTextBox.innerHTML = fancyHTML + '<span class="blinking-cursor"></span>';
                }
                if (!window.isUserScrolledUp) scrollToBottom();
            }
            return;
        }

        if (streamData.type === "turn_start") {
            currentActiveAgent = 'supervisor';
            toggleAgentLEDs('supervisor');
            setAgentStage('supervisor', streamData.phase || 'planning');
            addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'TURN', `Lượt ${streamData.turn}: ${streamData.phase}`);
            return;
        }

        if (streamData.type === "turn_phase") {
            const role = normalizeAgentRole(streamData.role || streamData.phase);
            currentActiveAgent = role;
            if (role === 'worker') workerBuffer = "";
            if (role === 'reviewer') reviewerBuffer = "";
            toggleAgentLEDs(role);
            setAgentStage(role, streamData.phase || 'running');
            setSystemState(`TURN ${streamData.turn} | ${role.toUpperCase()}`);
            return;
        }

        if (streamData.type === "execution_result") {
            renderExecutionResult(streamData);
            refreshSelectedTaskSummary();
            refreshTaskDashboard();
            return;
        }

        if (streamData.type === "review_result") {
            renderReviewResult(streamData);
            refreshSelectedTaskSummary();
            refreshTaskDashboard();
            return;
        }

        if (streamData.type === "artifact_progress") {
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'SAVED',
                `Đã lưu file được duyệt: ${streamData.file_path}`,
                'info'
            );
            setSystemState(`SAVED | ${streamData.file_path}`);
            refreshSelectedTaskSummary();
            return;
        }

        if (streamData.type === "artifact_ready") {
            const link = document.getElementById('artifact-download-link');
            if (link && streamData.download_url) {
                link.href = streamData.download_url;
                link.classList.remove('hidden');
            }
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'ARTIFACT',
                `${streamData.files?.length || 0} file sẵn sàng · ${streamData.status}`
            );
            refreshSelectedTaskSummary();
            return;
        }

        if (streamData.type === "turn_end") {
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                streamData.accepted ? 'PASS' : 'FAIL',
                `Lượt ${streamData.turn}: ${streamData.detail}`,
                streamData.accepted ? 'info' : 'warn'
            );
            return;
        }
        
        // 3. KHI HOÀN THÀNH TURN / LỖI
        if (streamData.type === "finish_chat_turn" || streamData.type === "finish" || streamData.type === "error") {
            removeCursor(currentTextBox);
            removeCursor(currentAiDiv);
            toggleAgentLEDs('none'); // Tắt hết đèn
            if (streamData.type === "error") {
                const logTime = new Date().toLocaleTimeString('en-US', {hour12: false});
                addSysLog(logTime, '❌', `ERROR: ${streamData.data}`);
                appendAgentTrace(currentActiveAgent, streamData.data, 'error', 'task error');
                setAgentStage(currentActiveAgent, 'error', false);
                appendError(streamData.data);
                setSystemState("ERROR");
                finishUI();
            } else if (streamData.type === "finish") {
                renderSessionResult(streamData);
                Object.keys(agentTraceState).forEach((role) => {
                    setAgentStage(role, 'done', false);
                });
                setSystemState(`COMPLETE | ${streamData.reason}`);
                refreshSelectedTaskSummary();
                refreshTaskDashboard();
            } else {
                awaitingChatReply = true;
                const runBtn = document.getElementById('btn-run');
                const stopBtn = document.getElementById('btn-stop');
                if (runBtn) {
                    runBtn.classList.remove('hidden');
                    runBtn.innerHTML = '<i class="fa-solid fa-paper-plane mr-2"></i> GỬI TIẾP';
                }
                if (stopBtn) stopBtn.classList.add('hidden');
                setSystemState("WAITING INPUT");
            }
            // Reset buffer cho vòng lặp sau
            workerBuffer = ""; 
        }

        if (streamData.type === "done") {
            finishUI();
            refreshSelectedTaskSummary();
            refreshTaskDashboard();
        }
    };

    source.onerror = function() {
        if (eventSource !== source) return;
        source.close();
        eventSource = null;
        setSystemState("CONNECTION LOST");
        addSysLog(new Date().toLocaleTimeString('en-US', {hour12: false}), 'WARN', 'Mất kết nối SSE', 'warn');
        setTimeout(() => {
            if (currentTaskId === streamTaskId && !eventSource) {
                setupEventSource();
            }
        }, 1500);
    };
}

function renderExecutionResult(data) {
    const panel = document.getElementById('execution-result-panel');
    const badge = document.getElementById('execution-result-badge');
    const file = document.getElementById('execution-result-file');
    const feedback = document.getElementById('worker-feedback-text');
    const detail = document.getElementById('execution-result-detail');
    if (!panel || !badge || !file || !feedback || !detail) return;

    const executionText = String(data.execution_result || '');
    const failed = executionText.startsWith('Fail');
    const unverified = executionText.startsWith('Unverified');
    panel.classList.remove('hidden');
    badge.className = `text-[10px] font-bold uppercase tracking-widest ${
        failed ? 'text-rose-400' : (unverified ? 'text-amber-400' : 'text-emerald-400')
    }`;
    badge.textContent = failed
        ? 'MACHINE GATE FAILED'
        : (unverified ? 'MACHINE UNVERIFIED' : 'MACHINE GATE OK');
    file.textContent = data.file_path || '';
    feedback.textContent = cleanAgentDisplayText(
        data.worker_feedback || 'Worker không cung cấp feedback.'
    );
    detail.textContent = failed
        ? executionText || data.detail || ''
        : `${executionText}${data.detail ? `\n${data.detail}` : ''}`;
    detail.classList.toggle('hidden', !detail.textContent);
    appendAgentTrace(
        'worker',
        [cleanAgentDisplayText(data.worker_feedback), executionText]
            .filter(Boolean)
            .join('\n'),
        failed ? 'error' : 'action',
        failed ? 'machine gate failed' : 'machine gate complete'
    );
    setAgentStage('worker', failed ? 'gate failed' : 'gate passed', false);
}

function renderReviewResult(data) {
    const panel = document.getElementById('execution-result-panel');
    const badge = document.getElementById('execution-result-badge');
    const feedback = document.getElementById('worker-feedback-text');
    const detail = document.getElementById('execution-result-detail');
    if (!panel || !badge || !feedback || !detail) return;

    const approved = data.verdict === 'approved';
    panel.classList.remove('hidden');
    badge.className = `text-[10px] font-bold uppercase tracking-widest ${approved ? 'text-emerald-400' : 'text-amber-400'}`;
    badge.textContent = approved ? 'REVIEW APPROVED' : 'REVIEW REVISE';
    const workerText = feedback.textContent ? `${feedback.textContent}\n\n` : '';
    feedback.textContent = `${workerText}Reviewer: ${cleanAgentDisplayText(
        data.reviewer_feedback || 'Không có nhận xét.'
    )}`;
    detail.textContent = data.next_instructions || '';
    detail.classList.toggle('hidden', !detail.textContent);
    addSysLog(
        new Date().toLocaleTimeString('en-US', {hour12: false}),
        approved ? 'PASS' : 'REVISE',
        data.reviewer_feedback || data.verdict,
        approved ? 'info' : 'warn'
    );

    const reviewerBox = document.getElementById('reviewer-result-box');
    const reviewerVerdict = document.getElementById('reviewer-verdict-badge');
    const reviewerFile = document.getElementById('reviewer-file');
    const reviewerFeedback = document.getElementById('reviewer-feedback');
    const reviewerNext = document.getElementById('reviewer-next-instructions');
    if (reviewerBox) reviewerBox.classList.remove('hidden');
    if (reviewerVerdict) {
        reviewerVerdict.textContent = approved ? 'APPROVED' : 'REVISE';
        reviewerVerdict.className = `agent-stage-badge font-bold ${approved ? 'text-emerald-400' : 'text-rose-400'}`;
    }
    if (reviewerFile) reviewerFile.textContent = data.file_path || '';
    if (reviewerFeedback) {
        reviewerFeedback.textContent = cleanAgentDisplayText(
            data.reviewer_feedback || ''
        );
    }
    if (reviewerNext) {
        reviewerNext.textContent = data.next_instructions
            ? `Yêu cầu tiếp theo: ${data.next_instructions}`
            : '';
    }
    appendAgentTrace(
        'reviewer',
        [
            cleanAgentDisplayText(data.reviewer_feedback || data.verdict),
            data.next_instructions ? `Tiếp theo: ${data.next_instructions}` : '',
        ].filter(Boolean).join('\n'),
        approved ? 'action' : 'retry',
        approved ? 'approved' : 'revise'
    );
    setAgentStage('reviewer', approved ? 'approved' : 'revise', false);
}

function renderSessionResult(data) {
    const streamBox = document.getElementById('live-stream');
    if (!streamBox) return;
    const wrapper = document.createElement('section');
    wrapper.className = 'session-summary';

    const title = document.createElement('h3');
    title.textContent = `Kết thúc: ${data.reason || 'unknown'}`;
    wrapper.appendChild(title);

    (data.turns || []).forEach((turn, index) => {
        const row = document.createElement('div');
        row.className = `session-turn ${turn.accepted ? 'turn-pass' : 'turn-fail'}`;
        const heading = document.createElement('strong');
        heading.textContent = `Turn ${index + 1} · ${turn.tool} · ${turn.accepted ? 'PASS' : 'FAIL'}`;
        const detail = document.createElement('pre');
        detail.textContent = turn.detail || '';
        row.append(heading, detail);
        wrapper.appendChild(row);
    });

    streamBox.appendChild(wrapper);
    scrollToBottom();
}

function appendError(message) {
    const streamBox = document.getElementById('live-stream');
    if (!streamBox) return;
    const node = document.createElement('div');
    node.className = 'stream-error';
    node.textContent = `[LỖI] ${message}`;
    streamBox.appendChild(node);
}

// ==========================================
// CÁC HÀM XỬ LÝ GIAO DIỆN MỚI
// ==========================================

function toggleAgentLEDs(active) {
    const supLed = document.getElementById('led-sup');
    const supContainer = document.getElementById('led-sup-container');
    const workLed = document.getElementById('led-work');
    const workContainer = document.getElementById('led-work-container');
    const reviewLed = document.getElementById('led-review');
    const reviewContainer = document.getElementById('led-review-container');
    
    if (active === 'supervisor') {
        supLed.className = "w-2.5 h-2.5 rounded-full mr-1.5 led-indicator led-active-sup animate-pulse";
        supContainer.classList.add('text-purple-400');
        workLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        workContainer.classList.remove('text-emerald-400');
        reviewLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        reviewContainer.classList.remove('text-amber-400');
    } else if (active === 'worker') {
        workLed.className = "w-2.5 h-2.5 rounded-full mr-1.5 led-indicator led-active-work animate-pulse";
        workContainer.classList.add('text-emerald-400');
        supLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        supContainer.classList.remove('text-purple-400');
        reviewLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        reviewContainer.classList.remove('text-amber-400');
    } else if (active === 'reviewer') {
        reviewLed.className = "w-2.5 h-2.5 rounded-full mr-1.5 led-indicator led-active-review animate-pulse";
        reviewContainer.classList.add('text-amber-400');
        supLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        workLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        supContainer.classList.remove('text-purple-400');
        workContainer.classList.remove('text-emerald-400');
    } else {
        supLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        workLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        supContainer.classList.remove('text-purple-400');
        workContainer.classList.remove('text-emerald-400');
        reviewLed.className = "w-2.5 h-2.5 rounded-full bg-gray-700 mr-1.5 led-indicator";
        reviewContainer.classList.remove('text-amber-400');
    }
}

function addSysLog(time, icon, msg, severity = 'info') {
    const logBox = document.getElementById('sys-audit-logs');
    if (!logBox) return;
    const cleanMsg = msg.replace(/[⚡🔑🔄]/g, '').trim();
    const previous = logBox.lastElementChild;
    if (previous?.dataset.message === cleanMsg && previous.dataset.icon === icon) {
        const count = Number(previous.dataset.count || 1) + 1;
        previous.dataset.count = String(count);
        previous.querySelector('.sys-log-time').textContent = `[${time}]`;
        previous.querySelector('.sys-log-content').textContent = `${icon} ${cleanMsg} ×${count}`;
        logBox.scrollTop = logBox.scrollHeight;
        return;
    }
    const line = document.createElement('div');
    line.className = `sys-log-line sys-log-${severity}`;
    line.dataset.message = cleanMsg;
    line.dataset.icon = icon;
    line.dataset.count = '1';
    const timestamp = document.createElement('span');
    timestamp.className = 'sys-log-time';
    timestamp.textContent = `[${time}]`;
    const content = document.createElement('span');
    content.className = 'sys-log-content';
    content.textContent = `${icon} ${cleanMsg}`;
    line.append(timestamp, content);
    logBox.appendChild(line);
    while (logBox.children.length > 500) logBox.firstElementChild.remove();
    logBox.scrollTop = logBox.scrollHeight;
}

function extractAndRenderTodo(text) {
    // Quét các dòng bắt đầu bằng - [ ] hoặc - [x]
    const todoRegex = /-\s*\[([ xX])\]\s*(.+)/g;
    let matches;
    let html = '';
    while ((matches = todoRegex.exec(text)) !== null) {
        const isDone = matches[1].trim().toLowerCase() === 'x';
        const taskName = escapeHtml(matches[2]);
        html += `
            <div class="todo-item ${isDone ? 'done' : ''}">
                <i class="fa-regular ${isDone ? 'fa-square-check' : 'fa-square'} todo-checkbox"></i>
                <span>${taskName}</span>
            </div>
        `;
    }
    
    const todoBox = document.getElementById('sup-todo-box');
    const todoContent = document.getElementById('todo-list-content');
    if (html && todoBox) {
        todoBox.classList.remove('hidden');
        todoContent.innerHTML = html;
    }
}

function renderDiffFromPatch(rawText) {
    // Nếu chưa có thẻ patch, cứ render markdown bình thường
    if (!rawText.includes('<patch>')) return renderSafeMarkdown(rawText);
    
    // Tách phần text giải thích và phần <patch>
    let html = renderSafeMarkdown(rawText.replace(/<patch>[\s\S]*?(<\/patch>|$)/g, ''));
    
    // Quét tìm tất cả các khối <patch>
    const patchRegex = /<patch>([\s\S]*?)(?:<\/patch>|$)/g;
    let match;
    
    while ((match = patchRegex.exec(rawText)) !== null) {
        const patchBody = match[1];
        
        // Quét SEARCH/REPLACE bên trong patch
        const blockRegex = /<<<< SEARCH\n([\s\S]*?)\n====\n([\s\S]*?)(?:\n>>>> REPLACE|$)/g;
        let blockMatch;
        
        while ((blockMatch = blockRegex.exec(patchBody)) !== null) {
            const searchCode = blockMatch[1] ? escapeHtml(blockMatch[1]) : '/* FILE MỚI TINH */';
            const replaceCode = blockMatch[2] ? escapeHtml(blockMatch[2]) : '';
            
            html += `
            <div class="diff-container mb-4">
                <div class="diff-header">
                    <span>DIFF PREVIEW</span>
                    <i class="fa-solid fa-code-compare"></i>
                </div>
                <div class="diff-body">
                    ${searchCode !== '/* FILE MỚI TINH */' ? `<div class="diff-row"><div class="diff-remove">- ${searchCode}</div></div>` : ''}
                    ${replaceCode ? `<div class="diff-row"><div class="diff-add">+ ${replaceCode}</div></div>` : ''}
                </div>
            </div>`;
        }
    }
    return html;
}
// 5. Artifacts: Quét mã HTML/CSS/JS và nhúng vào iFrame
function updateIframePreview(markdownText) {
    const iframe = document.getElementById('ui-preview-frame');
    if(!iframe) return;
    
    const htmlMatch = markdownText.match(/```html\n([\s\S]*?)```/);
    if(htmlMatch && htmlMatch[1]) {
        iframe.setAttribute('sandbox', '');
        iframe.srcdoc = htmlMatch[1];
    }
}

// 6. Các hàm Utility nhỏ
window.stopCurrentTask = async function() {
    if (currentTaskId) {
        const stopBtn = document.getElementById('btn-stop');
        if(stopBtn) stopBtn.innerText = "⏳ Đang ép dừng...";
        await fetch(`/api/stop/${currentTaskId}`, { method: 'POST' });
    }
}

function removeCursor(divElement) {
    if(divElement) divElement.innerHTML = divElement.innerHTML.replace('<span class="blinking-cursor"></span>', '');
}

function scrollToBottom() {
    const streamBox = document.getElementById('live-stream');
    if (streamBox && !window.isUserScrolledUp) {
        streamBox.scrollTop = streamBox.scrollHeight;
    }
}

function startTimer() {
    clearInterval(timerInterval);
    seconds = 0;
    timerInterval = setInterval(() => {
        seconds++;
        const m = String(Math.floor(seconds / 60)).padStart(2, '0');
        const s = String(seconds % 60).padStart(2, '0');
        const timerLbl = document.getElementById('timer');
        if(timerLbl) timerLbl.innerText = `${m}:${s}`;
    }, 1000);
}

function resetUI() {
    clearInterval(timerInterval);
    supBuffer = "";
    workerBuffer = "";
    reviewerBuffer = "";
    thinkingBuffer = "";
    estimatedTokens = 0;
    awaitingChatReply = false;
    resetAllAgentTraces();
    const tokenCounter = document.getElementById('token-counter');
    const accountCounter = document.getElementById('acc-counter');
    if (tokenCounter) tokenCounter.innerText = "0";
    if (accountCounter) accountCounter.innerText = "0";
    const resultPanel = document.getElementById('execution-result-panel');
    if (resultPanel) resultPanel.classList.add('hidden');
    const reviewerResult = document.getElementById('reviewer-result-box');
    const reviewerVerdict = document.getElementById('reviewer-verdict-badge');
    if (reviewerResult) reviewerResult.classList.add('hidden');
    if (reviewerVerdict) {
        reviewerVerdict.textContent = 'IDLE';
        reviewerVerdict.className = 'agent-stage-badge';
    }
    const runBtn = document.getElementById('btn-run');
    const stopBtn = document.getElementById('btn-stop');
    
    if(runBtn) runBtn.classList.add('hidden');
    if(stopBtn) {
        stopBtn.classList.remove('hidden');
        stopBtn.innerText = "⏹️ DỪNG";
    }
    
    const resultBox = document.getElementById('json-result');
    if(resultBox) resultBox.innerHTML = `<p class="text-gray-600 italic mt-4 text-center">Đang chờ AI trả kết quả JSON...</p>`;
    
    const streamBox = document.getElementById('live-stream');
    if(streamBox) streamBox.innerHTML = "";
    
    const ticker = document.getElementById('system-ticker');
    if(ticker) ticker.innerText = "SYSTEM: IDLE | AWAITING COMMAND...";
}

function finishUI() {
    clearInterval(timerInterval);
    const runBtn = document.getElementById('btn-run');
    const stopBtn = document.getElementById('btn-stop');
    
    if(stopBtn) stopBtn.classList.add('hidden');
    if(runBtn) {
        runBtn.classList.remove('hidden');
        runBtn.innerHTML = isChatMode()
            ? '<i class="fa-solid fa-paper-plane mr-2"></i> BẮT ĐẦU CHAT'
            : '<i class="fa-solid fa-bolt mr-2"></i> KÍCH HOẠT AI';
    }
    if(eventSource) eventSource.close();
    eventSource = null;
    toggleAgentLEDs('none');
    document.getElementById('task')?.focus();
}
// Mỗi ký tự Unicode được tính là 4 token, kể cả khoảng trắng, dấu câu và xuống dòng.
function calculateTokens(text) {
    if (!text) return 0;
    return Array.from(String(text)).length * 4;
}