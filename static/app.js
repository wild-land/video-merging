const $ = (selector) => document.querySelector(selector);
const form = $('#config-form');
const apiBase = new URL('./', window.location.href).pathname;
let activeTab = 'runs';
let toastTimer;
let csrfToken = '';

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
}
function formatTime(value) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? escapeHtml(value) : date.toLocaleString('zh-CN', {hour12:false});
}
function showToast(message) {
  const toast = $('#toast'); toast.textContent = message; toast.classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => toast.classList.remove('show'), 3300);
}
async function api(url, options = {}) {
  if (options.method === 'POST' && !csrfToken) await loadConfig();
  const request = {cache:'no-store', ...options};
  if (options.method === 'POST') request.headers = {...options.headers, 'X-CSRF-Token': csrfToken};
  const response = await fetch(apiBase + url.replace(/^\/+/, ''), request);
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `请求失败 (${response.status})`);
  return body;
}
async function loadConfig() {
  const {config, next_run, source_root, output_root, deployment, authorization_required, csrf_token} = await api('/api/config');
  csrfToken = csrf_token;
  for (const [key, value] of Object.entries(config)) {
    const input = form.elements.namedItem(key);
    if (input) input.type === 'checkbox' ? input.checked = value : input.value = value;
  }
  form.elements.namedItem('run_time').value = `${String(config.start_hour).padStart(2, '0')}:${String(config.minute).padStart(2, '0')}`;
  $('#source-hint').textContent = deployment === 'fpk'
    ? (authorization_required ? '请先在飞牛应用设置中授权包含录像和输出目录的父目录，然后重启应用' : `录像目录须位于已授权目录 ${source_root} 内`)
    : `录像目录须位于 ${source_root} 内（Docker 只读挂载）`;
  $('#output-hint').textContent = `输出目录须位于 ${output_root} 内；会自动创建 YYYYMMDD 子文件夹`;
  $('#metric-next').textContent = next_run ? formatTime(next_run) : '未启用';
  $('#metric-next-foot').textContent = next_run ? (deployment === 'fpk' ? '按飞牛设备时间执行' : '按容器时区执行') : '启用计划后显示下次时间';
  const chip = $('#plan-chip'); chip.textContent = config.enabled ? '● 计划已启用' : '○ 计划未启用'; chip.classList.toggle('off', !config.enabled);
}
async function loadStatus() {
  const status = await api('/api/status');
  $('#metric-success').textContent = status.totals.success;
  $('#metric-failed').textContent = status.totals.failed;
  const button = $('#run-now'); button.disabled = status.running;
  button.querySelector('span').textContent = status.running ? '正在执行…' : '立即执行一次';
}
function badge(value) {
  const labels = {success:'成功', failed:'失败', running:'运行中', interrupted:'已中断', info:'信息', error:'错误', pending:'待处理'};
  return `<span class="badge ${escapeHtml(value)}">${labels[value] || escapeHtml(value)}</span>`;
}
function renderTable(headers, rows) {
  if (!rows.length) return '<div class="empty">暂无记录。保存计划后可点击“立即执行一次”。</div>';
  return `<table class="log-table"><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table>`;
}
async function loadHistory() {
  const {items} = await api(`/api/${activeTab}?limit=100`);
  let html;
  if (activeTab === 'runs') {
    html = renderTable(['开始时间','触发方式','状态','成功 / 失败','结果'], items.map(row => `<tr><td class="strong">${formatTime(row.started_at)}</td><td>${row.trigger === 'manual' ? '手动执行' : '定时计划'}</td><td>${badge(row.status)}</td><td>${row.jobs_ok} / ${row.jobs_failed}</td><td>${escapeHtml(row.message || '执行中…')}</td></tr>`));
  } else if (activeTab === 'jobs') {
    html = renderTable(['日期 / 来源','输出文件','片段','状态','错误信息'], items.map(row => `<tr><td><span class="strong">${escapeHtml(row.day)}</span><br><span class="path">${escapeHtml(row.source_dir)}</span></td><td class="path">${escapeHtml(row.output_path)}</td><td>${row.clip_count}</td><td>${badge(row.status)}</td><td>${row.error ? `<div class="error-text">${escapeHtml(row.error)}</div>` : '—'}</td></tr>`));
  } else {
    html = renderTable(['时间','级别','任务','详情'], items.map(row => `<tr><td class="strong">${formatTime(row.created_at)}</td><td>${badge(row.level)}</td><td>${row.job_id ? `#${row.job_id}` : row.run_id ? `执行 #${row.run_id}` : '系统'}</td><td>${escapeHtml(row.message)}</td></tr>`));
  }
  $('#history-content').innerHTML = html;
}
function readForm() {
  const [start_hour, minute] = form.elements.namedItem('run_time').value.split(':').map(Number);
  return {source_dir:form.source_dir.value.trim(), output_dir:form.output_dir.value.trim(),
    extension:form.extension.value, group_size:Number(form.group_size.value), sort_mode:form.sort_mode.value,
    enabled:form.enabled.checked, start_hour, minute};
}
form.addEventListener('submit', async event => {
  event.preventDefault();
  const button = form.querySelector('button[type=submit]');
  const message = $('#save-message'); button.disabled = true; message.textContent = '正在保存…'; message.classList.remove('error');
  try {
    await api('/api/config', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(readForm())});
    message.textContent = '配置已保存'; showToast('执行计划已保存'); await Promise.all([loadConfig(), loadHistory()]);
  } catch (error) {message.textContent = error.message; message.classList.add('error'); showToast(error.message);}
  finally {button.disabled = false;}
});
$('#run-now').addEventListener('click', async () => {
  try {await api('/api/run', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); showToast('已开始扫描历史录像'); await loadStatus(); setTimeout(refresh, 1500);}
  catch (error) {showToast(error.message);}
});
async function refresh() {
  try {await Promise.all([loadStatus(), loadHistory()]);} catch (error) {showToast(error.message);}
}
$('#refresh').addEventListener('click', refresh);
$('#clear-logs').addEventListener('click', async () => {
  if (!window.confirm('将清空执行记录、详细日志和服务日志。合并视频及去重记录会保留。确定清空吗？')) return;
  const button = $('#clear-logs');
  button.disabled = true;
  try {
    const result = await api('/api/logs/clear', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
    await Promise.all([loadHistory(), loadStatus()]);
    showToast(result.warning ? `日志已清空；${result.warning}` : '日志已清空，合并去重记录已保留');
  } catch (error) {showToast(error.message);}
  finally {button.disabled = false;}
});
document.querySelectorAll('[data-tab]').forEach(tab => tab.addEventListener('click', () => {
  activeTab = tab.dataset.tab;
  document.querySelectorAll('[data-tab]').forEach(item => item.setAttribute('aria-selected', item === tab));
  loadHistory().catch(error => showToast(error.message));
}));
document.querySelectorAll('.nav-link').forEach(link => link.addEventListener('click', () => {
  document.querySelectorAll('.nav-link').forEach(item => item.classList.toggle('active', item === link));
}));
function updateClock() {$('#clock').textContent = new Date().toLocaleString('zh-CN', {year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});}
updateClock(); setInterval(updateClock, 30000);
Promise.all([loadConfig(), loadStatus(), loadHistory()]).catch(error => showToast(error.message));
setInterval(refresh, 15000);
