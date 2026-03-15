const TEXT = {
  requestFailed: "请求失败",
  seconds: "秒",
  noMonitors: "还没有监控的标，可以先在左侧创建。",
  running: "运行中",
  paused: "已暂停",
  configured: "已配置",
  missing: "缺失",
  rules: "规则",
  edit: "编辑",
  delete: "删除",
  selectMonitorFirst: "请先选择一个监控的标",
  addRulesAfterSelect: "选中监控的标后再创建规则。",
  currentPrice: "当前价",
  noRules: "还没有规则，可以先创建一条。",
  minutes: "分钟",
  enabled: "启用",
  disabled: "停用",
  noAlerts: "还没有告警记录。",
  newMonitor: "新增监控的标",
  updatedMonitor: "监控已更新",
  createdMonitor: "监控已创建",
  selectMonitorBeforeRule: "请先选择监控标的再创建规则",
  updatedRule: "规则已更新",
  createdRule: "规则已创建",
  runningNow: "正在执行",
  matchedThisRun: "本轮命中",
  queuedThisRun: "已入队",
  failedThisRun: "入队失败",
  runFinished: "手动执行完成",
  runFailed: "执行失败",
  confirmDeleteMonitor: "删除这个监控会同时删除它的规则和相关任务，确定继续吗？",
  deletedMonitor: "监控已删除",
  selectMonitorBeforeAction: "请先选择一个监控的标",
  confirmDeleteRule: "确定删除这条规则吗？",
  deletedRule: "规则已删除",
  refreshed: "数据已刷新",
  refreshingQuotes: "正在刷新行情",
  quotesUpdated: "行情已更新",
  savingInterval: "正在保存轮询周期",
  intervalSaved: "轮询周期已保存",
  loggingIn: "正在登录",
  loginSuccess: "登录成功",
  loginFailed: "登录失败",
  registerSuccess: "注册成功",
  resetSuccess: "密码已重置",
  logoutSuccess: "已退出登录",
  authRequired: "请先登录后再执行此操作",
  authLoggedOut: "未登录",
  authLoggedIn: "已登录",
  clearAlertsConfirm: "清空已完成的告警记录吗？未发送完的任务会保留。",
  clearedAlerts: "告警记录已清空",
  pageLabel: "第",
  andModeOn: "所有满足",
  andModeOff: "任一满足",
  andModeHintOn: "",
  andModeHintOff: "",
  andModeSavedOn: "已启用与触发逻辑",
  andModeSavedOff: "已启用或触发逻辑",
};

const state = {
  system: null,
  metadata: null,
  monitors: [],
  alerts: {
    items: [],
    page: 1,
    pageSize: 10,
    total: 0,
    totalPages: 1,
  },
  selectedMonitorId: null,
  auth: {
    token: localStorage.getItem("monitor_auth_token") || "",
    authenticated: false,
    username: "",
    accountInitialized: false,
    registeredUserCount: 0,
  },
  eventSource: null,
  justUpdatedId: null,
  justUpdatedType: null,
};

function $(selector) {
  return document.querySelector(selector);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatPrice(value) {
  if (typeof value !== "number") return "-";
  return parseFloat(value.toFixed(3)).toString();
}

function formatPct(value) {
  return typeof value === "number" ? `${value.toFixed(2)}%` : "-";
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const text = String(value);
  const matched = text.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})/);
  if (matched) {
    return `${matched[1]} ${matched[2]}`;
  }
  return text;
}

function typeLabel(value) {
  const instrumentTypes = state.metadata?.instrument_types || [];
  return instrumentTypes.find((item) => item.value === value)?.label || value || "-";
}

function latestQuoteTimestamp(monitors) {
  return (
    monitors
      .map((item) => item.latest_quote?.timestamp || "")
      .filter(Boolean)
      .sort()
      .at(-1) || "-"
  );
}

function canEdit() {
  return state.auth.authenticated;
}

function parseMentionInput(value) {
  return String(value || "")
    .split(/[\uFF0C,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function clearAuthState() {
  state.auth.token = "";
  state.auth.authenticated = false;
  state.auth.username = "";
  localStorage.removeItem("monitor_auth_token");
  closeEventStream();
}

function closeEventStream() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function resetProtectedState() {
  const pageSize = Math.max(1, Number(state.alerts?.pageSize) || 10);
  state.monitors = [];
  state.selectedMonitorId = null;
  state.alerts = {
    items: [],
    page: 1,
    pageSize,
    total: 0,
    totalPages: 1,
  };
}

async function api(path, options = {}) {
  const config = {
    headers: {
      "Content-Type": "application/json",
      ...(state.auth.token ? { "X-Auth-Token": state.auth.token } : {}),
      ...(options.headers || {}),
    },
    ...options,
  };

  const response = await fetch(path, config);
  if (response.status === 204) {
    return null;
  }

  const data = await response.json();
  if (!response.ok) {
    if (response.status === 401) {
      clearAuthState();
      resetProtectedState();
      renderAll();
    }
    const detail = data.detail;
    throw new Error(
      typeof detail === "string" ? detail :
      Array.isArray(detail) ? detail.map((e) => e.msg || JSON.stringify(e)).join("; ") :
      detail ? JSON.stringify(detail) : TEXT.requestFailed
    );
  }
  return data;
}

function showToast(message, isError = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.remove("hidden");
  toast.style.background = isError ? "var(--danger)" : "var(--accent-dark)";
  
  toast.style.animation = 'none';
  toast.offsetHeight;
  toast.style.animation = null;

  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => toast.classList.add("hidden"), 3000);
}

function flashElement(selector) {
  const el = $(selector);
  if (el) {
    el.classList.add("updated-flash");
    setTimeout(() => el.classList.remove("updated-flash"), 1500);
  }
}

function currentMonitor() {
  return state.monitors.find((item) => item.id === state.selectedMonitorId) || null;
}

function renderAuthPanel() {
  $("#auth-status").textContent = canEdit()
    ? `${TEXT.authLoggedIn}${state.auth.username ? ` (${state.auth.username})` : ""}`
    : TEXT.authLoggedOut;

  const count = state.auth.registeredUserCount;
  $("#auth-account-hint").textContent =
    count > 0
      ? `当前已注册 ${count} 个账号，可继续注册新账号，或使用注册码重置任意账号密码。`
      : "当前还没有账号，请先使用注册码注册。";

  $("#logout-btn").disabled = !canEdit();

  const authPanel = document.querySelector(".auth-panel");
  if (authPanel) {
    if (canEdit()) {
      authPanel.classList.add("hidden");
    } else {
      authPanel.classList.remove("hidden");
    }
  }

  const contentGrid = document.querySelector(".content-grid");
  if (contentGrid) {
    if (canEdit()) {
      contentGrid.classList.remove("hidden");
    } else {
      contentGrid.classList.add("hidden");
    }
  }
}

function renderSystem() {
  $("#system-provider").textContent = state.system?.quote_provider || "-";
  $("#system-interval").textContent = state.system ? `${state.system.poll_interval_seconds} ${TEXT.seconds}` : "-";
  $("#system-monitor-count").textContent = String(state.monitors.length);
  $("#latest-quote-time").textContent = latestQuoteTimestamp(state.monitors);
  $("#poll-interval-input").value = state.system?.poll_interval_seconds || 30;
}

function renderMonitorTable() {
  const body = $("#monitor-table-body");
  const lockedAttr = canEdit() ? "" : "disabled";

  if (!state.monitors.length) {
    body.innerHTML = `<tr><td colspan="10" class="empty-cell">${TEXT.noMonitors}</td></tr>`;
    return;
  }

  body.innerHTML = state.monitors
    .map((monitor) => {
      const activeClass = monitor.id === state.selectedMonitorId ? "active-row" : "";
      const updatedClass = (state.justUpdatedType === 'monitor' && monitor.id === state.justUpdatedId) ? "updated-flash" : "";
      const statusBadge = monitor.enabled
        ? `<span class="badge success">${TEXT.running}</span>`
        : `<span class="badge warn">${TEXT.paused}</span>`;
      const latestQuote = monitor.latest_quote;
      const changeClass = typeof latestQuote?.change_pct === "number" && latestQuote.change_pct < 0 ? "warn" : "success";

      return `
        <tr class="${activeClass} ${updatedClass}">
          <td>${escapeHtml(typeLabel(monitor.instrument_type))}</td>
          <td>${escapeHtml(monitor.code)}</td>
          <td>${escapeHtml(monitor.name || latestQuote?.name || "-")}</td>
          <td>${formatPrice(latestQuote?.last_price)}</td>
          <td>${latestQuote ? `<span class="badge ${changeClass}">${formatPct(latestQuote.change_pct)}</span>` : "-"}</td>
          <td>${escapeHtml(latestQuote?.timestamp || "-")}</td>
          <td>${monitor.rules.length}</td>
          <td>${statusBadge}</td>
          <td>${monitor.webhook_url ? TEXT.configured : TEXT.missing}</td>
          <td>
            <div class="inline-actions">
              <button class="mini-btn" data-action="select-monitor" data-id="${monitor.id}">${TEXT.rules}</button>
              <button class="mini-btn" data-action="edit-monitor" data-id="${monitor.id}" ${lockedAttr}>${TEXT.edit}</button>
              <button class="mini-btn danger" data-action="delete-monitor" data-id="${monitor.id}" ${lockedAttr}>${TEXT.delete}</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");
  
  if (state.justUpdatedType === 'monitor') {
    setTimeout(() => { state.justUpdatedId = null; state.justUpdatedType = null; }, 2000);
  }
}

function renderRulePanel() {
  const monitor = currentMonitor();
  const title = $("#rule-panel-title");
  const body = $("#rule-table-body");
  const lockedAttr = canEdit() ? "" : "disabled";

  if (!monitor) {
    title.textContent = TEXT.selectMonitorFirst;
    body.innerHTML = `<tr><td colspan="8" class="empty-cell">${TEXT.addRulesAfterSelect}</td></tr>`;
    return;
  }

  const latestPrice = monitor.latest_quote ? ` | ${TEXT.currentPrice} ${formatPrice(monitor.latest_quote.last_price)}` : "";
  title.textContent = `${monitor.name || monitor.code}${latestPrice}`;
  if (!monitor.rules.length) {
    body.innerHTML = `<tr><td colspan="8" class="empty-cell">${TEXT.noRules}</td></tr>`;
    return;
  }

  const fieldLabelMap = Object.fromEntries(state.metadata.fields.map((item) => [item.value, item.label]));
  const operatorLabelMap = Object.fromEntries(state.metadata.operators.map((item) => [item.value, item.label]));

  body.innerHTML = monitor.rules
    .map(
      (rule) => {
        const updatedClass = (state.justUpdatedType === 'rule' && rule.id === state.justUpdatedId) ? "updated-flash" : "";
        return `
          <tr class="${updatedClass}">
            <td>${escapeHtml(fieldLabelMap[rule.field] || rule.field)}</td>
            <td>${escapeHtml(operatorLabelMap[rule.operator] || rule.operator)}</td>
            <td>${rule.threshold}</td>
            <td>${rule.cooldown_minutes} ${TEXT.minutes}</td>
            <td>${rule.current_consecutive_hits}/${rule.consecutive_hits_required}</td>
            <td>${rule.enabled ? `<span class="badge success">${TEXT.enabled}</span>` : `<span class="badge warn">${TEXT.disabled}</span>`}</td>
            <td>${escapeHtml(formatDateTime(rule.last_triggered_at))}</td>
            <td>
              <div class="inline-actions">
                <button class="mini-btn" data-action="edit-rule" data-id="${rule.id}" ${lockedAttr}>${TEXT.edit}</button>
                <button class="mini-btn danger" data-action="delete-rule" data-id="${rule.id}" ${lockedAttr}>${TEXT.delete}</button>
              </div>
            </td>
          </tr>
        `;
      },
    )
    .join("");

  if (state.justUpdatedType === 'rule') {
    setTimeout(() => { state.justUpdatedId = null; state.justUpdatedType = null; }, 2000);
  }
}

function renderRuleLogicControl() {
  const button = $("#rule-logic-toggle-btn");
  const hint = $("#rule-logic-hint");
  if (!button) {
    return;
  }

  const monitor = currentMonitor();
  if (!monitor) {
    button.dataset.enabled = "false";
    button.textContent = TEXT.andModeOff;
    button.disabled = true;
    if (hint) hint.textContent = TEXT.selectMonitorFirst;
    return;
  }

  const enabled = Boolean(monitor.require_all_rules);
  button.dataset.enabled = enabled ? "true" : "false";
  button.textContent = enabled ? TEXT.andModeOn : TEXT.andModeOff;
  button.disabled = !canEdit();
  if (hint) hint.textContent = enabled ? TEXT.andModeHintOn : TEXT.andModeHintOff;
}

function alertBadgeClass(status) {
  if (status === "sent") return "success";
  if (status === "queued" || status === "retrying") return "pending";
  return "warn";
}

function renderAlertTable() {
  const body = $("#alert-table-body");

  if (!state.alerts.items.length) {
    body.innerHTML = `<tr><td colspan="5" class="empty-cell">${TEXT.noAlerts}</td></tr>`;
  } else {
    body.innerHTML = state.alerts.items
      .map((alert) => {
        const summary = escapeHtml(alert.message.split("\n").slice(0, 2).join(" | "));
        return `
          <tr>
            <td>${escapeHtml(formatDateTime(alert.created_at))}</td>
            <td>${escapeHtml(alert.code)}</td>
            <td><span class="badge ${alertBadgeClass(alert.status)}">${escapeHtml(alert.status)}</span></td>
            <td>${alert.triggered_value ?? "-"}</td>
            <td>${summary}</td>
          </tr>
        `;
      })
      .join("");
  }

  $("#alert-page-info").textContent = `${TEXT.pageLabel} ${state.alerts.page} / ${state.alerts.totalPages} 页`;
  $("#alert-page-input").value = state.alerts.page;
  $("#alert-prev-btn").disabled = state.alerts.page <= 1;
  $("#alert-next-btn").disabled = state.alerts.page >= state.alerts.totalPages;
}

function applyEditLockState() {
  const locked = !canEdit();
  document
    .querySelectorAll(
      "#monitor-form input, #monitor-form select, #monitor-form button, #rule-form input, #rule-form select, #rule-form button, #poll-interval-input, #save-poll-interval-btn, #refresh-btn, #run-once-btn, #clear-alerts-btn",
    )
    .forEach((element) => {
      element.disabled = locked;
    });
}

function renderAll() {
  renderAuthPanel();
  renderSystem();
  renderMonitorTable();
  renderRulePanel();
  renderAlertTable();
  applyEditLockState();
  renderRuleLogicControl();
}

function populateMetadata() {
  const selectedMonitorType = $("#monitor-type").value;
  const selectedRuleField = $("#rule-field").value;
  const selectedRuleOperator = $("#rule-operator").value;

  $("#monitor-type").innerHTML = state.metadata.instrument_types
    .map((item) => `<option value="${item.value}">${escapeHtml(item.label)}</option>`)
    .join("");
  $("#rule-field").innerHTML = state.metadata.fields
    .map((item) => `<option value="${item.value}">${escapeHtml(item.label)}</option>`)
    .join("");
  $("#rule-operator").innerHTML = state.metadata.operators
    .map((item) => `<option value="${item.value}">${escapeHtml(item.label)}</option>`)
    .join("");

  if (selectedMonitorType && state.metadata.instrument_types.some((item) => item.value === selectedMonitorType)) {
    $("#monitor-type").value = selectedMonitorType;
  }
  if (selectedRuleField && state.metadata.fields.some((item) => item.value === selectedRuleField)) {
    $("#rule-field").value = selectedRuleField;
  }
  if (selectedRuleOperator && state.metadata.operators.some((item) => item.value === selectedRuleOperator)) {
    $("#rule-operator").value = selectedRuleOperator;
  }
}

function resetMonitorForm() {
  $("#monitor-form-title").textContent = TEXT.newMonitor;
  $("#monitor-id").value = "";
  $("#monitor-type").value = state.metadata?.instrument_types?.[0]?.value || "stock";
  $("#monitor-code").value = "";
  $("#monitor-name").value = "";
  $("#monitor-webhook").value = "";
  $("#monitor-mentioned-mobiles").value = "";
  $("#monitor-mentioned-user-ids").value = "";
  $("#monitor-note").value = "";
  $("#monitor-enabled").checked = true;
}

function resetRuleForm() {
  $("#rule-id").value = "";
  $("#rule-threshold").value = "";
  $("#rule-cooldown").value = "5";
  $("#rule-consecutive-hits").value = "1";
  $("#rule-description").value = "";
  $("#rule-enabled").checked = true;
  if (state.metadata) {
    $("#rule-field").value = state.metadata.fields[0]?.value || "";
    $("#rule-operator").value = state.metadata.operators[0]?.value || "";
  }
}

function resetAuthForms() {
  $("#login-form").reset();
  $("#register-form").reset();
  $("#reset-password-form").reset();
}

function fillMonitorForm(monitor) {
  $("#monitor-form-title").textContent = `${TEXT.edit} ${monitor.name || monitor.code}`;
  $("#monitor-id").value = monitor.id;
  $("#monitor-type").value = monitor.instrument_type;
  $("#monitor-code").value = monitor.code;
  $("#monitor-name").value = monitor.name || "";
  $("#monitor-webhook").value = monitor.webhook_url;
  $("#monitor-mentioned-mobiles").value = (monitor.mentioned_mobiles || []).join(", ");
  $("#monitor-mentioned-user-ids").value = (monitor.mentioned_user_ids || []).join(", ");
  $("#monitor-note").value = monitor.note || "";
  $("#monitor-enabled").checked = monitor.enabled;
}

function fillRuleForm(rule) {
  $("#rule-id").value = rule.id;
  $("#rule-field").value = rule.field;
  $("#rule-operator").value = rule.operator;
  $("#rule-threshold").value = rule.threshold;
  $("#rule-cooldown").value = rule.cooldown_minutes;
  $("#rule-consecutive-hits").value = rule.consecutive_hits_required;
  $("#rule-description").value = rule.description || "";
  $("#rule-enabled").checked = rule.enabled;
}

function normalizeAlerts(payload) {
  const currentPageSize = Number(state.alerts?.pageSize) || 10;
  const page = Math.max(1, Number(payload?.page ?? 1) || 1);
  const pageSize = Math.max(
    1,
    Number(payload?.page_size) || currentPageSize,
  );
  const total = Math.max(0, Number(payload?.total ?? 0) || 0);
  const totalPages = Math.max(
    1,
    Number(payload?.total_pages ?? (total ? Math.ceil(total / pageSize) : 1)) || 1,
  );

  return {
    items: Array.isArray(payload?.items) ? payload.items : [],
    page,
    pageSize,
    total,
    totalPages,
  };
}

async function refreshData() {
  if (!canEdit()) {
    resetProtectedState();
    renderAll();
    return;
  }

  const alertPage = Math.max(1, Number(state.alerts?.page) || 1);
  const alertPageSize = Math.max(1, Number(state.alerts?.pageSize) || 10);
  const [system, metadata, monitors, alerts] = await Promise.all([
    api("/api/system"),
    api("/api/metadata"),
    api("/api/monitors"),
    api(`/api/alerts?page=${alertPage}&page_size=${alertPageSize}`),
  ]);

  state.system = system;
  state.metadata = metadata;
  state.monitors = monitors;
  state.alerts = normalizeAlerts(alerts);

  if (state.alerts.total === 0 && state.alerts.page !== 1) {
    state.alerts.page = 1;
    return refreshData();
  }
  if (state.alerts.page > state.alerts.totalPages) {
    state.alerts.page = state.alerts.totalPages;
    return refreshData();
  }

  if (!state.selectedMonitorId || !state.monitors.some((item) => item.id === state.selectedMonitorId)) {
    state.selectedMonitorId = state.monitors[0]?.id || null;
  }

  populateMetadata();
  renderAll();
}

async function refreshAuthStatus() {
  const status = await api("/api/auth/status");
  state.auth.authenticated = status.authenticated;
  state.auth.username = status.username || "";
  state.auth.accountInitialized = Boolean(status.account_initialized);
  state.auth.registeredUserCount = Number(status.registered_user_count || 0);
  if (!status.authenticated && state.auth.token) {
    clearAuthState();
  }
  if (!status.authenticated) {
    resetProtectedState();
    closeEventStream();
  }
  renderAll();
}

async function handleLogin(event) {
  event.preventDefault();
  const username = $("#login-username-input").value.trim();
  const password = $("#login-password-input").value;
  if (!username || !password) {
    showToast(TEXT.loginFailed, true);
    return;
  }

  $("#run-status").textContent = TEXT.loggingIn;
  try {
    const result = await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    state.auth.token = result.token;
    state.auth.authenticated = result.authenticated;
    state.auth.username = result.username || username;
    localStorage.setItem("monitor_auth_token", result.token);
    await refreshAuthStatus();
    await refreshData();
    connectEventStream();
    $("#run-status").textContent = TEXT.loginSuccess;
    $("#login-form").reset();
    showToast(TEXT.loginSuccess);
  } catch (error) {
    $("#run-status").textContent = TEXT.loginFailed;
    showToast(error.message || TEXT.loginFailed, true);
  }
}

async function handleRegister(event) {
  event.preventDefault();
  const username = $("#register-username-input").value.trim();
  const password = $("#register-password-input").value;
  const registrationCode = $("#register-code-input").value.trim();
  if (!username || !password || !registrationCode) {
    showToast(TEXT.requestFailed, true);
    return;
  }

  try {
    const result = await api("/api/auth/register", {
      method: "POST",
      body: JSON.stringify({ username, password, registration_code: registrationCode }),
    });
    state.auth.token = result.token;
    state.auth.authenticated = result.authenticated;
    state.auth.username = result.username || username;
    localStorage.setItem("monitor_auth_token", result.token);
    await refreshAuthStatus();
    await refreshData();
    connectEventStream();
    $("#run-status").textContent = TEXT.registerSuccess;
    resetAuthForms();
    showToast(TEXT.registerSuccess);
  } catch (error) {
    showToast(error.message, true);
  }
}

async function handlePasswordReset(event) {
  event.preventDefault();
  const username = $("#reset-username-input").value.trim();
  const newPassword = $("#reset-password-input").value;
  const registrationCode = $("#reset-code-input").value.trim();
  if (!username || !newPassword || !registrationCode) {
    showToast(TEXT.requestFailed, true);
    return;
  }

  try {
    const result = await api("/api/auth/reset-password", {
      method: "POST",
      body: JSON.stringify({ username, new_password: newPassword, registration_code: registrationCode }),
    });
    state.auth.token = result.token;
    state.auth.authenticated = result.authenticated;
    state.auth.username = result.username || username;
    localStorage.setItem("monitor_auth_token", result.token);
    await refreshAuthStatus();
    await refreshData();
    connectEventStream();
    $("#run-status").textContent = TEXT.resetSuccess;
    resetAuthForms();
    showToast(TEXT.resetSuccess);
  } catch (error) {
    showToast(error.message, true);
  }
}

async function logout() {
  try {
    await api("/api/auth/logout", { method: "POST" });
  } catch (error) {
    showToast(error.message, true);
  } finally {
    clearAuthState();
    resetProtectedState();
    renderAll();
    showToast(TEXT.logoutSuccess);
  }
}

async function refreshQuotesThenData() {
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }
  $("#run-status").textContent = TEXT.refreshingQuotes;
  const result = await api("/api/quotes/refresh", { method: "POST" });
  await refreshData();
  $("#run-status").textContent = `${TEXT.quotesUpdated} ${result.updated}`;
}

async function savePollInterval() {
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }
  const value = Number($("#poll-interval-input").value);
  $("#run-status").textContent = TEXT.savingInterval;
  await api("/api/system/poll-interval", {
    method: "PUT",
    body: JSON.stringify({ poll_interval_seconds: value }),
  });
  await refreshData();
  $("#run-status").textContent = `${TEXT.intervalSaved} ${value}`;
  showToast(TEXT.intervalSaved);
}

async function clearAlerts() {
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }
  if (!window.confirm(TEXT.clearAlertsConfirm)) {
    return;
  }
  await api("/api/alerts", { method: "DELETE" });
  state.alerts.page = 1;
  await refreshData();
  showToast(TEXT.clearedAlerts);
}

async function handleMonitorSubmit(event) {
  event.preventDefault();
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }

  const monitorId = $("#monitor-id").value;
  const editingMonitor = monitorId
    ? state.monitors.find((item) => item.id === Number(monitorId))
    : null;

  const payload = {
    instrument_type: $("#monitor-type").value,
    code: $("#monitor-code").value.trim(),
    name: $("#monitor-name").value.trim(),
    webhook_url: $("#monitor-webhook").value.trim(),
    mentioned_mobiles: parseMentionInput($("#monitor-mentioned-mobiles").value),
    mentioned_user_ids: parseMentionInput($("#monitor-mentioned-user-ids").value),
    require_all_rules: Boolean(editingMonitor?.require_all_rules),
    note: $("#monitor-note").value.trim(),
    enabled: $("#monitor-enabled").checked,
  };

  try {
    if (monitorId) {
      await api(`/api/monitors/${monitorId}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast(TEXT.updatedMonitor);
      state.justUpdatedId = Number(monitorId);
      state.justUpdatedType = 'monitor';
    } else {
      const created = await api("/api/monitors", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      state.selectedMonitorId = created.id;
      state.justUpdatedId = created.id;
      state.justUpdatedType = 'monitor';
      showToast(TEXT.createdMonitor);
    }

    resetMonitorForm();
    await refreshData();
    flashElement(".list-panel");
  } catch (error) {
    showToast(error.message, true);
  }
}

async function handleRuleLogicToggle() {
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }

  const monitor = currentMonitor();
  if (!monitor) {
    showToast(TEXT.selectMonitorFirst, true);
    return;
  }

  const nextValue = !Boolean(monitor.require_all_rules);
  const payload = {
    instrument_type: monitor.instrument_type,
    code: monitor.code,
    name: monitor.name || "",
    webhook_url: monitor.webhook_url,
    mentioned_mobiles: monitor.mentioned_mobiles || [],
    mentioned_user_ids: monitor.mentioned_user_ids || [],
    require_all_rules: nextValue,
    note: monitor.note || "",
    enabled: monitor.enabled,
  };

  try {
    await api(`/api/monitors/${monitor.id}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    await refreshData();
    showToast(nextValue ? TEXT.andModeSavedOn : TEXT.andModeSavedOff);
  } catch (error) {
    showToast(error.message, true);
  }
}

async function handleRuleSubmit(event) {
  event.preventDefault();
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }
  const monitor = currentMonitor();
  if (!monitor) {
    showToast(TEXT.selectMonitorBeforeRule, true);
    return;
  }

  const payload = {
    field: $("#rule-field").value,
    operator: $("#rule-operator").value,
    threshold: Number($("#rule-threshold").value),
    cooldown_minutes: Number($("#rule-cooldown").value),
    consecutive_hits_required: Number($("#rule-consecutive-hits").value),
    description: $("#rule-description").value.trim(),
    enabled: $("#rule-enabled").checked,
  };

  const ruleId = $("#rule-id").value;

  try {
    if (ruleId) {
      await api(`/api/rules/${ruleId}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast(TEXT.updatedRule);
      state.justUpdatedId = Number(ruleId);
      state.justUpdatedType = 'rule';
    } else {
      await api(`/api/monitors/${monitor.id}/rules`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      showToast(TEXT.createdRule);
      state.justUpdatedType = 'rule';
    }

    resetRuleForm();
    await refreshData();
    flashElement(".rule-panel");
  } catch (error) {
    showToast(error.message, true);
  }
}

async function handleRunOnce() {
  if (!canEdit()) {
    showToast(TEXT.authRequired, true);
    return;
  }
  try {
    $("#run-status").textContent = TEXT.runningNow;
    const result = await api("/api/run-once", { method: "POST" });
    $("#run-status").textContent = `${TEXT.matchedThisRun} ${result.matched} / ${TEXT.queuedThisRun} ${result.queued}`;
    await refreshData();
    showToast(
      `${TEXT.runFinished}：${TEXT.matchedThisRun} ${result.matched}，${TEXT.queuedThisRun} ${result.queued}，${TEXT.failedThisRun} ${result.failed}`,
    );
  } catch (error) {
    $("#run-status").textContent = TEXT.runFailed;
    showToast(error.message, true);
  }
}

async function handleTableClick(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  const id = Number(button.dataset.id);

  try {
    if (action === "select-monitor") {
      state.selectedMonitorId = id;
      resetRuleForm();
      renderAll();
      return;
    }

    if (!canEdit()) {
      showToast(TEXT.authRequired, true);
      return;
    }

    if (action === "edit-monitor") {
      const monitor = state.monitors.find((item) => item.id === id);
      if (monitor) {
        state.selectedMonitorId = id;
        fillMonitorForm(monitor);
        renderAll();
      }
      return;
    }

    if (action === "delete-monitor") {
      if (!window.confirm(TEXT.confirmDeleteMonitor)) {
        return;
      }
      await api(`/api/monitors/${id}`, { method: "DELETE" });
      if (state.selectedMonitorId === id) {
        state.selectedMonitorId = null;
      }
      resetMonitorForm();
      resetRuleForm();
      await refreshData();
      showToast(TEXT.deletedMonitor);
      return;
    }

    const monitor = currentMonitor();
    if (!monitor) {
      showToast(TEXT.selectMonitorBeforeAction, true);
      return;
    }

    const rule = monitor.rules.find((item) => item.id === id);
    if (action === "edit-rule" && rule) {
      fillRuleForm(rule);
      return;
    }

    if (action === "delete-rule") {
      if (!window.confirm(TEXT.confirmDeleteRule)) {
        return;
      }
      await api(`/api/rules/${id}`, { method: "DELETE" });
      resetRuleForm();
      await refreshData();
      showToast(TEXT.deletedRule);
    }
  } catch (error) {
    showToast(error.message, true);
  }
}

function changeAlertPage(nextPage) {
  const parsedNextPage = Number(nextPage);
  const targetPage = Number.isFinite(parsedNextPage) ? Math.trunc(parsedNextPage) : state.alerts.page;
  const maxPage = Math.max(1, Number(state.alerts.totalPages) || 1);
  const safePage = Math.max(1, Math.min(targetPage, maxPage));
  state.alerts.page = safePage;
  refreshData().catch((error) => showToast(error.message, true));
}

function scheduleRefreshData(delay = 350) {
  clearTimeout(scheduleRefreshData.timer);
  scheduleRefreshData.timer = setTimeout(() => {
    if (!canEdit()) {
      return;
    }
    refreshData().catch((error) => showToast(error.message, true));
  }, delay);
}

function connectEventStream() {
  closeEventStream();
  if (!canEdit()) {
    return;
  }

  const eventSource = new EventSource("/api/events");
  const handleServerUpdate = () => {
    scheduleRefreshData();
  };

  eventSource.addEventListener("quotes_updated", handleServerUpdate);
  eventSource.addEventListener("config_changed", handleServerUpdate);
  eventSource.addEventListener("system_updated", handleServerUpdate);
  eventSource.addEventListener("alerts_updated", handleServerUpdate);
  state.eventSource = eventSource;
}

function bindEvents() {
  $("#login-form").addEventListener("submit", handleLogin);
  $("#register-form").addEventListener("submit", handleRegister);
  $("#reset-password-form").addEventListener("submit", handlePasswordReset);
  $("#logout-btn").addEventListener("click", async () => {
    await logout();
  });
  $("#monitor-form").addEventListener("submit", handleMonitorSubmit);
  $("#rule-form").addEventListener("submit", handleRuleSubmit);
  $("#refresh-btn").addEventListener("click", async () => {
    try {
      await refreshQuotesThenData();
      showToast(TEXT.refreshed);
    } catch (error) {
      showToast(error.message, true);
    }
  });
  $("#save-poll-interval-btn").addEventListener("click", async () => {
    try {
      await savePollInterval();
    } catch (error) {
      showToast(error.message, true);
    }
  });
  $("#clear-alerts-btn").addEventListener("click", async () => {
    try {
      await clearAlerts();
    } catch (error) {
      showToast(error.message, true);
    }
  });
  $("#alert-prev-btn").addEventListener("click", () => changeAlertPage(state.alerts.page - 1));
  $("#alert-next-btn").addEventListener("click", () => changeAlertPage(state.alerts.page + 1));
  $("#alert-page-go-btn").addEventListener("click", () => {
    const page = Number($("#alert-page-input").value || state.alerts.page);
    changeAlertPage(page);
  });
  $("#rule-logic-toggle-btn").addEventListener("click", handleRuleLogicToggle);
  $("#run-once-btn").addEventListener("click", handleRunOnce);
  $("#monitor-reset-btn").addEventListener("click", resetMonitorForm);
  $("#rule-reset-btn").addEventListener("click", resetRuleForm);
  $("#monitor-table-body").addEventListener("click", handleTableClick);
  $("#rule-table-body").addEventListener("click", handleTableClick);
}

async function bootstrap() {
  bindEvents();
  try {
    await refreshAuthStatus();
    if (canEdit()) {
      await refreshData();
      connectEventStream();
    }
    resetMonitorForm();
    resetRuleForm();
  } catch (error) {
    showToast(error.message, true);
  }
}

window.addEventListener("DOMContentLoaded", bootstrap);
