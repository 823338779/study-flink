'use strict';

const get = (id) => document.getElementById(id);
const form = get('payment-form');
let eventId = crypto.randomUUID();
let occurredAt = new Date().toISOString();
let lastAttempt = null;
let busy = false;

function buildEvent() {
  const raw = get('amount').value.trim();
  if (!/^[0-9]+(\.[0-9]{1,2})?$/.test(raw)) {
    throw new Error('请输入大于 0 的金额，最多两位小数。');
  }
  const [yuan, decimals = ''] = raw.split('.');
  const cents = BigInt(yuan) * 100n + BigInt(decimals.padEnd(2, '0'));
  if (cents <= 0n || cents > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error('金额必须大于 0，且不能超过 90071992547409.91 元。');
  }
  return { eventId, shopId: get('shop').value, amountCents: Number(cents), occurredAt };
}

function preview() {
  get('message-key').textContent = get('shop').value || '请填写商店 ID';
  try {
    get('payload').textContent = JSON.stringify(buildEvent(), null, 2);
  } catch (error) {
    get('payload').textContent = error.message;
  }
}

function feedback(state, title, message, receipt) {
  document.querySelector('.result').dataset.state = state;
  get('result-state').textContent = title;
  get('feedback').textContent = message;
  get('receipt').hidden = receipt === undefined;
  get('receipt').textContent = receipt === undefined ? '' : JSON.stringify(receipt, null, 2);
}

function controls() {
  get('fields').disabled = busy || lastAttempt !== null;
  get('send').disabled = busy || lastAttempt !== null;
  get('retry').disabled = busy || lastAttempt === null;
  get('next').disabled = busy || lastAttempt === null;
  form.setAttribute('aria-busy', String(busy));
}

async function send(event) {
  if (busy) return;
  lastAttempt = event;
  busy = true;
  controls();
  feedback('pending', '发送中', '正在等待 Kafka 接收确认…');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch('/api/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event),
      signal: controller.signal
    });
    const text = await response.text();
    let result;
    try { result = JSON.parse(text); } catch { result = null; }
    if (!response.ok) {
      const uncertain = response.status >= 500;
      feedback('error', uncertain ? '请确认发送结果' : '请求未通过',
        (result?.message || `服务返回 HTTP ${response.status}。`) +
        (uncertain ? ' 如需重试，请点“重发同一事件”，保留事件身份。' : ' 请点“准备下一笔”修改输入。'), result ?? undefined);
      return;
    }
    if (result?.status !== 'ACCEPTED' || result.eventId !== event.eventId) {
      throw new Error('服务返回了无法识别的回执');
    }
    feedback('success', 'Kafka 已接收',
      `商店 ${event.shopId} 的支付事件已接收。可在 Flink 的 processElement 断点观察处理过程。`, result);
  } catch (error) {
    feedback('error', '发送结果不确定',
      `${error.name === 'AbortError' ? '等待服务响应超时' : '未收到有效的服务确认'}。请检查 payment-service 是否启动；事件可能已经写入 Kafka，重试请使用“重发同一事件”。`);
  } finally {
    clearTimeout(timer);
    busy = false;
    controls();
  }
}

form.addEventListener('input', preview);
form.addEventListener('submit', (e) => {
  e.preventDefault();
  if (busy || lastAttempt) return;
  try { void send(buildEvent()); }
  catch (error) { feedback('error', '检查输入', error.message); get('amount').focus(); }
});
get('retry').addEventListener('click', () => { if (lastAttempt) void send(lastAttempt); });
get('next').addEventListener('click', () => {
  eventId = crypto.randomUUID();
  occurredAt = new Date().toISOString();
  lastAttempt = null;
  controls();
  preview();
  feedback('idle', '尚未发送', '已生成新的事件 ID。填写金额后发送下一笔支付。');
  get('amount').focus();
});
preview();
