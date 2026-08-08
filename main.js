/* =========================================================================
   MAIN.JS — AI Orchestrator Command Center
   Nâng cấp UX: Drag&Drop mount, Code block header (copy/save),
   Global Hotkeys, Smart Scroll + Token Speedometer, Prompt History
   ========================================================================= */

let splitInstance = null;
let isPanelsHidden = false;

document.addEventListener('DOMContentLoaded', () => {
    initSplitView();
    initTextareaBehavior();
    initDragDropZone();
    initGlobalHotkeys();
    initSmartScroll();
    initTokenSpeedometer();
    initPromptHistory();
    initCodeBlockEnhancer();
    initModeUI();
});

function initModeUI() {
    const radios = document.querySelectorAll('input[name="mode"]');
    radios.forEach((radio) => radio.addEventListener('change', updateModeUI));
    updateModeUI();
}

function updateModeUI() {
    const mode = document.querySelector('input[name="mode"]:checked')?.value || 'orchestrator';
    const isChat = mode === 'chat';
    document.body.classList.toggle('chat-mode', isChat);

    const modelLabel = document.getElementById('primary-model-label');
    const effortLabel = document.getElementById('primary-effort-label');
    const arenaTitle = document.getElementById('arena-title');
    const runButton = document.getElementById('btn-run');

    if (modelLabel) modelLabel.textContent = isChat ? '// chat_model' : '// worker_model';
    if (effortLabel) effortLabel.textContent = isChat ? '// chat_effort' : '// worker_effort';
    if (arenaTitle) {
        arenaTitle.innerHTML = isChat
            ? '<i class="fa-solid fa-comments mr-1.5 text-cyan-500"></i>CHAT'
            : '<i class="fa-solid fa-terminal mr-1.5 text-emerald-500"></i>WORKER ARENA';
    }
    if (runButton && !runButton.classList.contains('hidden')) {
        runButton.innerHTML = isChat
            ? '<i class="fa-solid fa-paper-plane mr-2"></i> BẮT ĐẦU CHAT'
            : '<i class="fa-solid fa-bolt mr-2"></i> KÍCH HOẠT AI';
    }
    if (typeof updateProjectModeUI === 'function') updateProjectModeUI();
}

/* ---------- 0. Split.js 3 cột (giữ nguyên logic cũ) ---------- */
function initSplitView() {
    if (typeof Split === 'undefined' || !document.getElementById('panel-left')) return;
    if (window.matchMedia('(max-width: 1024px)').matches) return;
    const savedSizes = JSON.parse(localStorage.getItem('splitSizes')) || [25, 40, 35];
    splitInstance = Split(['#panel-left', '#panel-center', '#panel-right'], {
        sizes: savedSizes,
        minSize: [250, 350, 250],
        gutterSize: 6,
        onDragEnd: function (sizes) {
            localStorage.setItem('splitSizes', JSON.stringify(sizes));
        }
    });
}

/* ---------- 0b. Textarea auto-expand + Shift+Enter (giữ nguyên logic cũ) ---------- */
function autoResizeTextarea(el) {
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
    el.style.overflowY = el.scrollHeight > 300 ? 'auto' : 'hidden';
}

function initTextareaBehavior() {
    const taskInput = document.getElementById('task');
    const runBtn = document.getElementById('btn-run');
    if (!taskInput) return;

    taskInput.addEventListener('input', function () {
        autoResizeTextarea(this);
    });

    taskInput.addEventListener('keydown', function (e) {
        // BUGFIX: loại trừ Ctrl/Cmd để không trùng lặp với hotkey Ctrl+Enter
        // toàn cục trong initGlobalHotkeys() (tránh double-click #btn-run).
        if (e.key === 'Enter' && !e.shiftKey && !e.ctrlKey && !e.metaKey) {
            e.preventDefault();
            if (runBtn && !runBtn.classList.contains('hidden')) {
                runBtn.click();
            }
        }
    });
}

/* =========================================================================
   1. DRAG & DROP ZONE — #panel-left làm Drop Zone mount folder/file
   ========================================================================= */
function initDragDropZone() {
    const dropZone = document.getElementById('panel-left');
    if (!dropZone) return;

    let dragCounter = 0;

    const overlay = document.createElement('div');
    overlay.id = 'drop-zone-overlay';
    overlay.className = 'drop-zone-overlay hidden';
    overlay.innerHTML = `
        <div class="drop-zone-overlay-inner">
            <i class="fa-solid fa-folder-open"></i>
            <p>DROP FOLDER HERE TO MOUNT</p>
        </div>`;
    dropZone.appendChild(overlay);

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach((evt) => {
        dropZone.addEventListener(evt, (e) => {
            e.preventDefault();
            e.stopPropagation();
        });
    });

    dropZone.addEventListener('dragenter', () => {
        dragCounter++;
        dropZone.classList.add('drop-zone-active');
        overlay.classList.remove('hidden');
    });

    dropZone.addEventListener('dragleave', () => {
        dragCounter = Math.max(0, dragCounter - 1);
        if (dragCounter === 0) {
            dropZone.classList.remove('drop-zone-active');
            overlay.classList.add('hidden');
        }
    });

    dropZone.addEventListener('drop', async (e) => {
        dragCounter = 0;
        dropZone.classList.remove('drop-zone-active');
        overlay.classList.add('hidden');

        // Browser không expose absolute path — không enumerate file giả.
        // Folder-only: bắt buộc xác nhận root qua native picker.
        if (typeof addSysLog === 'function') {
            addSysLog(
                new Date().toLocaleTimeString('en-US', {hour12: false}),
                'WARN',
                'Drag & Drop không gắn được absolute path. Hãy bấm CHỌN THƯ MỤC CODE.',
                'warn'
            );
        }
        if (typeof openProjectPicker === 'function') {
            openProjectPicker();
        }
    });
}

/* =========================================================================
   2. CODE BLOCK ENHANCER — Header (ngôn ngữ + Copy + Save) cho mỗi <pre><code>
   ========================================================================= */
function initCodeBlockEnhancer() {
    const streamBox = document.getElementById('live-stream');
    if (!streamBox) return;

    const observer = new MutationObserver(() => enhanceCodeBlocks(streamBox));
    observer.observe(streamBox, { childList: true, subtree: true });

    enhanceCodeBlocks(streamBox);
}

function enhanceCodeBlocks(container) {
    const codeBlocks = container.querySelectorAll('pre > code');

    codeBlocks.forEach((codeEl) => {
        const preEl = codeEl.parentElement;
        if (!preEl || preEl.dataset.enhanced === '1' || !preEl.parentElement) return;

        // Gỡ nút Copy đơn giản cũ (do api.js/task.html tự chèn bằng regex) để tránh trùng lặp
        const legacyBtn = preEl.previousElementSibling;
        if (legacyBtn && legacyBtn.tagName === 'BUTTON' &&
            legacyBtn.getAttribute('onclick') === 'window.copyCode(this)') {
            legacyBtn.remove();
        }

        let lang = 'plaintext';
        const langClass = [...codeEl.classList].find((c) => c.startsWith('language-'));
        if (langClass) {
            lang = langClass.replace('language-', '');
        } else if (codeEl.className) {
            const guess = codeEl.className.replace('hljs', '').trim().split(/\s+/)[0];
            if (guess) lang = guess;
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'code-block-wrapper';

        const header = document.createElement('div');
        header.className = 'code-block-header';
        header.innerHTML = `
            <span class="code-block-lang">${lang}</span>
            <span class="code-block-actions">
                <button type="button" class="code-block-btn code-block-copy-btn">📋 Copy</button>
                <button type="button" class="code-block-btn code-block-save-btn">💾 Save</button>
            </span>`;

        preEl.parentElement.insertBefore(wrapper, preEl);
        wrapper.appendChild(header);
        wrapper.appendChild(preEl);
        preEl.dataset.enhanced = '1';

        header.querySelector('.code-block-copy-btn').addEventListener('click', (e) => {
            const btn = e.currentTarget;
            navigator.clipboard.writeText(codeEl.innerText).then(() => {
                const original = btn.innerHTML;
                btn.innerHTML = '✅ Copied';
                setTimeout(() => { btn.innerHTML = original; }, 1500);
            });
        });

        header.querySelector('.code-block-save-btn').addEventListener('click', () => {
            downloadCodeAsFile(codeEl.innerText, lang);
        });
    });
}

function downloadCodeAsFile(code, lang) {
    const extMap = {
        javascript: 'js', js: 'js', typescript: 'ts', jsx: 'jsx', tsx: 'tsx',
        python: 'py', py: 'py', html: 'html', xml: 'xml', css: 'css', scss: 'scss',
        json: 'json', bash: 'sh', shell: 'sh', sh: 'sh', sql: 'sql', java: 'java',
        c: 'c', cpp: 'cpp', 'c++': 'cpp', csharp: 'cs', cs: 'cs', php: 'php',
        ruby: 'rb', go: 'go', rust: 'rs', rs: 'rs', yaml: 'yml', yml: 'yml', markdown: 'md'
    };
    const ext = extMap[lang.toLowerCase()] || 'txt';
    const blob = new Blob([code], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `snippet_${Date.now()}.${ext}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

/* =========================================================================
   3. GLOBAL HOTKEYS
   ========================================================================= */
function initGlobalHotkeys() {
    document.addEventListener('keydown', (e) => {
        const isMac = navigator.platform.toUpperCase().includes('MAC');
        const ctrlOrCmd = isMac ? e.metaKey : e.ctrlKey;

        // Ctrl/Cmd + Enter -> Kích hoạt gửi yêu cầu
        if (ctrlOrCmd && e.key === 'Enter') {
            e.preventDefault();
            const runBtn = document.getElementById('btn-run');
            if (runBtn && !runBtn.classList.contains('hidden')) runBtn.click();
            return;
        }

        // Esc -> Ép dừng khẩn cấp + chớp đỏ màn hình
        if (e.key === 'Escape') {
            const stopBtn = document.getElementById('btn-stop');
            if (stopBtn && !stopBtn.classList.contains('hidden')) {
                stopBtn.click();
                flashRedScreen();
            }
            return;
        }

        // Ctrl/Cmd + ` hoặc Ctrl/Cmd + B -> Bật/tắt Focus Mode
        if (ctrlOrCmd && (e.key === '`' || e.key.toLowerCase() === 'b')) {
            e.preventDefault();
            if (typeof window.togglePanels === 'function') window.togglePanels();
            return;
        }

        // Ctrl/Cmd + L -> Xóa sạch Terminal
        if (ctrlOrCmd && e.key.toLowerCase() === 'l') {
            e.preventDefault();
            const streamBox = document.getElementById('live-stream');
            if (streamBox) streamBox.innerHTML = '';
            return;
        }
    });
}

function flashRedScreen() {
    const flash = document.createElement('div');
    flash.className = 'emergency-flash';
    document.body.appendChild(flash);
    setTimeout(() => flash.remove(), 200);
}

/* =========================================================================
   4. SMART SCROLL + TOKEN SPEEDOMETER
   ========================================================================= */
function initSmartScroll() {
    const streamBox = document.getElementById('live-stream');
    if (!streamBox || !streamBox.parentElement) return;

    const THRESHOLD = 60; // Ngưỡng pixel để xác định "sát đáy"
    window.isUserScrolledUp = false; // Biến toàn cục để api.js có thể đọc

    // Tạo popup nếu chưa có
    let popup = document.getElementById('smart-scroll-popup');
    if (!popup) {
        popup = document.createElement('button');
        popup.type = 'button';
        popup.id = 'smart-scroll-popup';
        popup.className = 'smart-scroll-popup hidden';
        popup.innerHTML = '<span class="new-msg-dot"></span><span>↓ Có tin nhắn mới</span>';
        streamBox.parentElement.appendChild(popup);
    }

    // Xử lý khi nhấn nút popup: Cuộn mượt xuống đáy
    popup.addEventListener('click', () => {
        window.isUserScrolledUp = false;
        streamBox.scrollTo({ top: streamBox.scrollHeight, behavior: 'smooth' });
        popup.classList.add('hidden');
    });

    // Lắng nghe hành vi cuộn chuột của user
    streamBox.addEventListener('scroll', () => {
        const isNearBottom = streamBox.scrollHeight - streamBox.scrollTop - streamBox.clientHeight < THRESHOLD;
        window.isUserScrolledUp = !isNearBottom;

        // Nếu user cuộn xuống chạm đáy, tự động ẩn popup
        if (!window.isUserScrolledUp) {
            popup.classList.add('hidden');
        }
    });

    // Reset cờ cuộn khi bắt đầu chạy task mới
    const runBtn = document.getElementById('btn-run');
    if (runBtn) {
        runBtn.addEventListener('click', () => {
            window.isUserScrolledUp = false;
            popup.classList.add('hidden');
        });
    }
}

function initTokenSpeedometer() {
    const counter = document.getElementById('token-counter');
    const speedEl = document.getElementById('token-speed');
    const runBtn = document.getElementById('btn-run');
    if (!counter) return;

    const parseTokenValue = (text) => parseInt(String(text).replace(/[.,]/g, ''), 10) || 0;

    let lastValue = parseTokenValue(counter.innerText);
    let lastTime = performance.now();
    let smoothedSpeed = 0;

    const render = () => { if (speedEl) speedEl.innerText = `${Math.round(smoothedSpeed)} T/s`; };

    const observer = new MutationObserver(() => {
        const now = performance.now();
        const currentValue = parseTokenValue(counter.innerText);
        const dt = (now - lastTime) / 1000;
        const dv = currentValue - lastValue;

        if (dt > 0 && dv >= 0) {
            const instantSpeed = dv / dt;
            smoothedSpeed = smoothedSpeed === 0 ? instantSpeed : smoothedSpeed * 0.6 + instantSpeed * 0.4;
            render();
        }
        lastValue = currentValue;
        lastTime = now;
    });
    observer.observe(counter, { childList: true, characterData: true, subtree: true });

    if (runBtn) {
        runBtn.addEventListener('click', () => {
            lastValue = 0;
            lastTime = performance.now();
            smoothedSpeed = 0;
            render();
        });
    }

    // Giảm dần về 0 khi không còn token mới (idle > 1.5s) để HUD không "đứng hình" ở giá trị cũ
    setInterval(() => {
        if ((performance.now() - lastTime) / 1000 > 1.5 && smoothedSpeed !== 0) {
            smoothedSpeed = 0;
            render();
        }
    }, 500);
}

/* =========================================================================
   5. LỊCH SỬ PROMPT (Local History)
   ========================================================================= */
const PROMPT_HISTORY_KEY = 'ai_orchestrator_prompt_history';
const MAX_PROMPT_HISTORY = 50;

function loadPromptHistory() {
    try {
        const raw = JSON.parse(localStorage.getItem(PROMPT_HISTORY_KEY));
        return Array.isArray(raw) ? raw : [];
    } catch (e) {
        return [];
    }
}

function initPromptHistory() {
    const taskInput = document.getElementById('task');
    const streamBox = document.getElementById('live-stream');
    if (!taskInput) return;

    let promptHistory = loadPromptHistory();
    let historyIndex = -1;
    let draftBeforeHistory = '';

    const saveToHistory = (text) => {
        const trimmed = text.trim();
        if (!trimmed || promptHistory[0] === trimmed) return;
        promptHistory.unshift(trimmed);
        if (promptHistory.length > MAX_PROMPT_HISTORY) promptHistory.length = MAX_PROMPT_HISTORY;
        localStorage.setItem(PROMPT_HISTORY_KEY, JSON.stringify(promptHistory));
        historyIndex = -1;
    };

    // Theo dõi các bong bóng "👤 Bạn:" mới xuất hiện trong terminal để tự lưu lịch sử,
    // độc lập với thời điểm api.js xóa nội dung #task (tránh việc đọc hụt giá trị).
    if (streamBox) {
        const historyObserver = new MutationObserver((mutations) => {
            mutations.forEach((m) => {
                m.addedNodes.forEach((node) => {
                    if (node.nodeType !== 1 || !node.querySelector) return;
                    const label = node.querySelector('.text-emerald-400');
                    if (label && label.textContent.includes('Bạn:')) {
                        const bodyEl = node.querySelector('.markdown-body');
                        if (bodyEl) saveToHistory(bodyEl.innerText);
                    }
                });
            });
        });
        historyObserver.observe(streamBox, { childList: true });
    }

    taskInput.addEventListener('input', () => {
        // Người dùng đã tự gõ sửa nội dung -> thoát chế độ duyệt lịch sử
        historyIndex = -1;
    });

    taskInput.addEventListener('keydown', (e) => {
        if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;

        const isEmpty = taskInput.value.trim() === '';
        const isBrowsingHistory = historyIndex !== -1;
        if (!isEmpty && !isBrowsingHistory) return; // để trình duyệt tự xử lý di chuyển con trỏ

        if (e.key === 'ArrowUp') {
            if (promptHistory.length === 0) return;
            e.preventDefault();
            if (historyIndex === -1) {
                draftBeforeHistory = taskInput.value;
                historyIndex = 0;
            } else if (historyIndex < promptHistory.length - 1) {
                historyIndex++;
            }
            taskInput.value = promptHistory[historyIndex];
            autoResizeTextarea(taskInput);
        } else if (e.key === 'ArrowDown') {
            if (historyIndex === -1) return;
            e.preventDefault();
            if (historyIndex > 0) {
                historyIndex--;
                taskInput.value = promptHistory[historyIndex];
            } else {
                historyIndex = -1;
                taskInput.value = draftBeforeHistory;
            }
            autoResizeTextarea(taskInput);
        }
    });
}

/* =========================================================================
   CÁC HÀM ĐÃ CÓ TỪ TRƯỚC (giữ nguyên hành vi)
   ========================================================================= */
window.switchTab = function (tabId) {
    document.querySelectorAll('.tab-content').forEach((c) => c.classList.add('hidden'));
    document.querySelectorAll('.tab-btn').forEach((b) => {
        b.classList.remove('border-cyan-400', 'text-cyan-400', 'bg-cyan-500/5');
        b.classList.add('border-transparent', 'text-gray-500');
    });

    const targetContent = document.getElementById(tabId);
    if (targetContent) targetContent.classList.remove('hidden');

    const activeBtn = document.querySelector(`[onclick="switchTab('${tabId}')"]`);
    if (activeBtn) {
        activeBtn.classList.remove('border-transparent', 'text-gray-500');
        activeBtn.classList.add('border-cyan-400', 'text-cyan-400', 'bg-cyan-500/5');
    }
};

window.copyCode = function (button) {
    const codeBlock = button.nextElementSibling;
    if (!codeBlock) return;

    navigator.clipboard.writeText(codeBlock.innerText).then(() => {
        const originalText = button.innerHTML;
        button.innerHTML = '✅ Copied!';
        button.classList.add('text-emerald-400', 'border-emerald-500');
        setTimeout(() => {
            button.innerHTML = originalText;
            button.classList.remove('text-emerald-400', 'border-emerald-500');
        }, 2000);
    });
};

window.togglePanels = function () {
    const leftPanel = document.getElementById('panel-left');
    const rightPanel = document.getElementById('panel-right');
    const centerPanel = document.getElementById('panel-center');
    const gutters = document.querySelectorAll('.gutter');
    const toggleIcon = document.querySelector('#btn-toggle-panels i');

    isPanelsHidden = !isPanelsHidden;

    if (isPanelsHidden) {
        leftPanel.style.display = 'none';
        rightPanel.style.display = 'none';
        gutters.forEach((g) => (g.style.display = 'none'));
        centerPanel.style.width = '100%';
        if (toggleIcon) {
            toggleIcon.classList.remove('fa-expand');
            toggleIcon.classList.add('fa-compress');
        }
    } else {
        leftPanel.style.display = 'flex';
        rightPanel.style.display = 'flex';
        gutters.forEach((g) => (g.style.display = 'block'));
        if (splitInstance) {
            const savedSizes = JSON.parse(localStorage.getItem('splitSizes')) || [25, 40, 35];
            splitInstance.setSizes(savedSizes);
        }
        if (toggleIcon) {
            toggleIcon.classList.remove('fa-compress');
            toggleIcon.classList.add('fa-expand');
        }
    }
};