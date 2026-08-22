/**
 * Batch-window tests: rapid consecutive messages (texts, or text + image)
 * merge into ONE agent turn; commands flush the pending batch first;
 * batchWindowMs=0 restores immediate per-message delivery.
 */
import assert from 'node:assert/strict'
import { Bridge } from '../lib/bridge.js'
import { SessionMap } from '../lib/session-map.js'
import { StreamingCardManager } from '../lib/cards.js'

let passed = 0
const pending = []
const ok = (name, fn) => {
  const result = fn()
  if (result instanceof Promise) {
    pending.push(
      result.then(() => {
        passed += 1
        console.log(`${name}: true`)
      }),
    )
    return
  }
  passed += 1
  console.log(`${name}: true`)
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
// Delivery crosses the per-chat chain plus session-map disk I/O - poll for
// the actual outcome instead of sleeping a fixed span.
const waitFor = async (cond, timeoutMs = 2000) => {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (cond()) return
    await sleep(10)
  }
}

const WINDOW_MS = 60

function batchBridge(overrides = {}) {
  const events = []
  const fakeAgent = {
    id: 's',
    sent: [],
    followup(m) {
      this.sent.push(m)
      events.push('followup')
    },
    cancel() {},
  }
  const transport = {
    sent: [],
    connectionState: () => 'ready',
    onMessage(h) {
      this._h = h
    },
    onCardAction() {},
    async sendText(chatId, text) {
      this.sent.push({ chatId, text })
      events.push(`reply:${text.slice(0, 12)}`)
    },
    async sendCard() {
      return 'm-card'
    },
    async updateCard() {},
  }
  const agentStore = {
    get: id => (id === fakeAgent.id ? fakeAgent : undefined),
    resume: async () => {
      throw new Error('no log')
    },
    create: async sessionId => {
      fakeAgent.id = sessionId
      return fakeAgent
    },
  }
  const logger = { info() {}, warn() {}, error() {} }
  const cards = new StreamingCardManager(transport, { throttleMs: 1, logger })
  const bridge = new Bridge({
    transport,
    sessionMap: new SessionMap('/tmp/nonexistent/batch-session-map.json'),
    agentStore,
    cards,
    logger,
    defaultCwd: '/work',
    batchWindowMs: WINDOW_MS,
    ...overrides,
  })
  bridge.start()
  return { transport, fakeAgent, bridge, cards, events }
}

const textEvent = (id, text) => ({
  messageId: id,
  chatId: 'oc_1',
  chatType: 'p2p',
  senderOpenId: 'ou_x',
  text,
})
const imageEvent = (id, imageKey) => ({
  messageId: id,
  chatId: 'oc_1',
  chatType: 'p2p',
  senderOpenId: 'ou_x',
  text: '',
  imageKey,
})

ok('two rapid texts merge into one turn', async () => {
  const { transport, fakeAgent, bridge, cards } = batchBridge()
  await transport._h(textEvent('b1', '看下这张图'))
  await transport._h(textEvent('b2', '重点看左边的报错'))
  await waitFor(() => fakeAgent.sent.length > 0)
  assert.equal(fakeAgent.sent.length, 1, 'exactly one turn for both messages')
  const texts = fakeAgent.sent[0].content.filter(b => b.type === 'text').map(b => b.text)
  assert.ok(texts.some(t => t.includes('看下这张图')), 'first text in the merged turn')
  assert.ok(texts.some(t => t.includes('重点看左边的报错')), 'second text in the merged turn')
  bridge.dispose()
  cards.dispose()
})

ok('text + image within the window merge into one turn', async () => {
  const { transport, fakeAgent, bridge, cards } = batchBridge({
    resolveInboundImage: async () => ({
      kind: 'attachment',
      ref: { attachmentId: 'att-9', mediaType: 'image/png', bytes: 10, width: 4, height: 4 },
    }),
  })
  await transport._h(imageEvent('b3', 'img_v3_x'))
  await transport._h(textEvent('b4', '这张图里写了什么'))
  await waitFor(() => fakeAgent.sent.length > 0)
  assert.equal(fakeAgent.sent.length, 1, 'image and caption in one turn')
  const content = fakeAgent.sent[0].content
  const image = content.find(b => b.type === 'image')
  assert.ok(image !== undefined, 'image block present')
  assert.equal(image.attachment.attachmentId, 'att-9')
  assert.ok(content.some(b => b.type === 'text' && b.text.includes('这张图里写了什么')), 'caption text present')
  assert.ok(!content.some(b => b.type === 'text' && b.text.includes('用户发来一张图片')), 'no redundant auto caption in a batch')
  bridge.dispose()
  cards.dispose()
})

ok('messages beyond the window stay separate turns', async () => {
  const { transport, fakeAgent, bridge, cards } = batchBridge()
  await transport._h(textEvent('b5', '第一条'))
  await waitFor(() => fakeAgent.sent.length > 0)
  await transport._h(textEvent('b6', '第二条'))
  await waitFor(() => fakeAgent.sent.length > 1)
  assert.equal(fakeAgent.sent.length, 2, 'two turns for spaced messages')
  bridge.dispose()
  cards.dispose()
})

ok('batchWindowMs=0 delivers every message immediately', async () => {
  const { transport, fakeAgent, bridge, cards } = batchBridge({ batchWindowMs: 0 })
  await transport._h(textEvent('b7', '一'))
  await transport._h(textEvent('b8', '二'))
  await waitFor(() => fakeAgent.sent.length > 1)
  assert.equal(fakeAgent.sent.length, 2, 'no merging without a window')
  bridge.dispose()
  cards.dispose()
})

ok('a command flushes the pending batch first', async () => {
  const { transport, fakeAgent, bridge, cards, events } = batchBridge()
  await transport._h(textEvent('b9', '先记住这个'))
  await transport._h(textEvent('b10', '/status'))
  await waitFor(() => events.includes('followup') && events.some(e => e.startsWith('reply:')))
  assert.equal(fakeAgent.sent.length, 1, 'buffered text delivered')
  assert.ok(
    events.indexOf('followup') < events.findIndex(e => e.startsWith('reply:')),
    'the message turn started before the command reply',
  )
  bridge.dispose()
  cards.dispose()
})

await Promise.all(pending)
console.log(`BATCH OK (${passed} checks)`)
