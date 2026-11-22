/**
 * dingtalk-stream-compat.js
 *
 * 钉钉 Stream 模式客户端（Workers/workerd 兼容形）。
 *
 * 这是 DomonAI fork 的核心改造件：官方 `dingtalk-stream` npm 包在模块顶层
 * require npm `ws` 包（且经 axios 依赖 node:http 适配器），而 workerd 无 npm
 * 原生/协议库 socket 面，插件无法原样装载（SPEC 0206 §2「npm ws 族永不可
 * 装」）。本文件按官方 SDK 的线上协议 1:1 重写传输层：
 *
 *   1. POST https://api.dingtalk.com/v1.0/gateway/connections/open
 *      （body: clientId/clientSecret/ua/subscriptions，须带 Accept: application/json）
 *      → { endpoint, ticket } → dw_url = `${endpoint}?ticket=${ticket}`
 *   2. WebSocket 连 dw_url（订阅在 gateway/open 请求里随连接注册，无需注册帧）
 *   3. 下行帧 { type: 'SYSTEM'|'EVENT'|'CALLBACK', headers, data }：
 *        - SYSTEM，headers.topic:
 *            CONNECTED / REGISTERED（registered=true）/ disconnect（清标志）/
 *            KEEPALIVE（系统级心跳，记录 isAlive）/ ping（必须原样回 200 帧）
 *        - EVENT  → 按 registerAllEventListener 的结果回 ACK（默认 SUCCESS）
 *        - CALLBACK → this.emit(topic, msg)（机器人消息即此通道）
 *   4. 上行 ACK/响应帧固定形：
 *      { code: 200, headers: { contentType: 'application/json', messageId },
 *        message: 'OK', data: JSON.stringify(value) }
 *
 * 与官方 SDK（dingtalk-stream-sdk-nodejs，MIT，
 * https://github.com/open-dingtalk/dingtalk-stream-sdk-nodejs）的差异：
 *   - `ws` 包 → 全局 WebSocket（workerd / Node ≥22 原生，WHATWG EventTarget 形）
 *   - `axios` → 全局 fetch；`debug` → 去除（日志交上层）
 *   - `keepAlive`（ws 层 ping/pong 探活）不再提供：钉钉服务端系统级
 *     KEEPALIVE/ping 帧仍按协议处理；连接守护由上层哨兵+强制重建负责
 *   - 官方 SDK 不透传 socket 层 error/close；本实现把这两个事件 re-emit 到
 *     client 本体（上游 dingtalk-client.js 本就监听它们记日志——官方实现下
 *     是盲区，这里如实补上，语义只增不减）
 *
 * DWClient 对外表面（构造 opts / registerCallbackListener /
 * registerAllEventListener / connect / disconnect / send / connected /
 * registered / socket）与官方 SDK 保持一致，供 dingtalk-client.js 无感切换。
 */

import { EventEmitter } from 'node:events';

export const GATEWAY_URL = 'https://api.dingtalk.com/v1.0/gateway/connections/open';
export const GET_TOKEN_URL = 'https://oapi.dingtalk.com/gettoken';
export const TOPIC_ROBOT = '/v1.0/im/bot/messages/get';
export const TOPIC_CARD = '/v1.0/card/instances/callback';
export const TOPIC_AI_GRAPH_API = '/v1.0/graph/api/invoke';

export const EventAck = { SUCCESS: 'SUCCESS', LATER: 'LATER' };

const defaultConfig = {
  autoReconnect: true,
  keepAlive: false,
  ua: '',
  subscriptions: [{ type: 'EVENT', topic: '*' }],
};

export class DWClient extends EventEmitter {
  debug = false;
  connected = false;
  registered = false;
  reconnecting = false;
  userDisconnect = false;
  reconnectInterval = 1_000;
  /** socket 层探活（系统 KEEPALIVE 帧刷新；本实现不做 ws 层 ping/pong）。 */
  isAlive = false;
  config;
  socket = null;
  dw_url = '';
  onEventReceived = () => ({ status: EventAck.SUCCESS });

  constructor(opts) {
    super();
    this.config = { ...defaultConfig, ...opts };
    if (!this.config.clientId || !this.config.clientSecret) {
      throw new Error('clientId or clientSecret is null');
    }
    if (this.config.debug !== undefined) this.debug = this.config.debug;
  }

  getConfig() {
    return { ...this.config };
  }

  printDebug(msg) {
    if (this.debug) console.info(`[${new Date().toISOString()}]`, msg);
  }

  registerAllEventListener(onEventReceived) {
    this.onEventReceived = onEventReceived;
    return this;
  }

  registerCallbackListener(eventId, callback) {
    if (!eventId || !callback) {
      throw new Error('registerCallbackListener: eventId and callback must be defined');
    }
    if (!this.config.subscriptions.find((x) => x.topic === eventId && x.type === 'CALLBACK')) {
      this.config.subscriptions.push({ type: 'CALLBACK', topic: eventId });
    }
    this.on(eventId, callback);
    return this;
  }

  /** gateway/open：换取 endpoint+ticket（fetch 形，替代官方 axios）。 */
  async getEndpoint() {
    const res = await fetch(GATEWAY_URL, {
      method: 'POST',
      headers: { Accept: 'application/json', 'content-type': 'application/json' },
      body: JSON.stringify({
        clientId: this.config.clientId,
        clientSecret: this.config.clientSecret,
        ua: this.config.ua,
        subscriptions: this.config.subscriptions,
      }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data) throw new Error(`getEndpoint: gateway/open HTTP ${res.status}`);
    this.printDebug(`res.data ${JSON.stringify(data)}`);
    this.config.endpoint = data;
    const { endpoint, ticket } = data;
    if (!endpoint || !ticket) throw new Error('endpoint or ticket is null');
    this.dw_url = `${endpoint}?ticket=${ticket}`;
    return this;
  }

  /** 打开 Stream WebSocket（全局 WebSocket 形，替代官方 `ws` 包）。 */
  _connect() {
    return new Promise((resolve, reject) => {
      this.userDisconnect = false;
      this.printDebug(`Connecting to dingtalk websocket @ ${this.dw_url}`);
      try {
        this.socket = new WebSocket(this.dw_url);
      } catch (err) {
        this.printDebug('WebSocket constructor error');
        reject(err);
        return;
      }
      const socket = this.socket;
      socket.addEventListener('open', () => {
        this.connected = true;
        console.info(`[${new Date().toISOString()}] connect success`);
        resolve();
      });
      socket.addEventListener('message', (ev) => {
        try {
          if (typeof ev.data !== 'string') return;
          this.onDownStream(JSON.parse(ev.data));
        } catch (err) {
          this.printDebug(`downstream parse error: ${err?.message || err}`);
        }
      });
      socket.addEventListener('close', () => {
        this.printDebug('Socket closed');
        this.connected = false;
        this.registered = false;
        // 官方 SDK 不透传 socket close；这里如实 re-emit（上层日志/守护依赖）。
        this.emit('close');
        if (this.config.autoReconnect && !this.userDisconnect) {
          this.reconnecting = true;
          this.printDebug(`Reconnecting in ${this.reconnectInterval / 1000} seconds...`);
          setTimeout(() => this.connect().catch(() => {}), this.reconnectInterval);
        }
      });
      socket.addEventListener('error', () => {
        this.printDebug('SOCKET ERROR');
        // WHATWG 形的 error 事件无 err 载荷；包一层 Error 保持上层
        // `err?.message` 日志形。error 后必有 close，终止/清理由 close 分支负责。
        this.emit('error', new Error('dingtalk stream websocket error'));
        reject(new Error('dingtalk stream websocket error'));
      });
    });
  }

  async connect() {
    try {
      await this.getEndpoint();
      await this._connect();
    } catch (err) {
      if (this.config.autoReconnect && !this.userDisconnect) {
        this.printDebug(`Connect failed, retrying in ${this.reconnectInterval / 1000} seconds...`);
        setTimeout(() => this.connect().catch(() => {}), this.reconnectInterval);
      }
      // 与官方 SDK 同语义：connect() 不 reject（成功与否经 connected 判断），
      // 上层桥接器自带 connect 超时保护 + connected!==true 校验。
    }
  }

  disconnect() {
    console.info('Disconnecting.');
    this.userDisconnect = true;
    try {
      this.socket?.close();
    } catch {
      // 已关闭/从未打开时忽略
    }
  }

  heartbeat() {
    this.isAlive = true;
    this.printDebug('CLIENT-SIDE HEARTBEAT');
  }

  onDownStream(msg) {
    this.printDebug('Received message from dingtalk websocket server');
    switch (msg?.type) {
      case 'SYSTEM':
        this.onSystem(msg);
        break;
      case 'EVENT':
        this.onEvent(msg);
        break;
      case 'CALLBACK':
        this.onCallback(msg);
        break;
    }
  }

  onSystem(downstream) {
    switch (downstream?.headers?.topic) {
      case 'CONNECTED':
        this.printDebug('CONNECTED');
        break;
      case 'REGISTERED':
        this.registered = true;
        this.reconnecting = false;
        break;
      case 'disconnect':
        this.connected = false;
        this.registered = false;
        break;
      case 'KEEPALIVE':
        this.heartbeat();
        break;
      case 'ping':
        this.printDebug('PING');
        this.socket?.send(JSON.stringify({
          code: 200,
          headers: downstream.headers,
          message: 'OK',
          data: downstream.data,
        }));
        break;
    }
  }

  onEvent(message) {
    this.printDebug(`received event, message=${JSON.stringify(message)}`);
    const ackData = this.onEventReceived(message);
    this.socket?.send(JSON.stringify({
      code: 200,
      headers: {
        contentType: 'application/json',
        messageId: message.headers.messageId,
      },
      message: 'OK',
      data: JSON.stringify(ackData),
    }));
  }

  onCallback(message) {
    this.emit(message.headers.topic, message);
  }

  /** 消息响应（ACK），避免服务端 60s 内重试投递。 */
  send(messageId, value) {
    if (!messageId) throw new Error('send: messageId must be defined');
    this.socket?.send(JSON.stringify({
      code: 200,
      headers: {
        contentType: 'application/json',
        messageId,
      },
      message: 'OK',
      data: JSON.stringify(value),
    }));
  }

  /** Graph API 结果回传（同 ACK 帧形，data 包一层 response）。 */
  socketCallBackResponse(messageId, result) {
    this.send(messageId, { response: result });
  }
}
