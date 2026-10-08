"""WS Keepalive 保活调优插件。

为什么需要它
------------
KiraAI 的 OneBot 适配器在连接时把保活参数写死了：

    await ws_compatible_connect(..., ping_timeout=10.0)

`websockets` 库的保活跑在一个独立的 task 里：每 `ping_interval` 秒发一个
ping，然后最多 `ping_timeout` 秒等 pong。**关键**：这个等待和应用代码共享
同一个 asyncio 事件循环 —— 只要任何一段代码把事件循环卡住超过
`ping_timeout`，pong 就处理不了，客户端判定 "keepalive ping timeout"，
发出 1011 并断开连接。

10 秒对 KiraAI 这种既要调 LLM、又要下图片的进程本来就偏紧：一次
LLM 描述图片、一次 httpx 图片下载、一次 GC 停顿，都可能超过它。

本插件做什么
------------
不改 KiraAI 任何源码，只把保活预算放宽（`ping_timeout`、`ping_interval`、
`open_timeout` 都是 websockets 连接对象上的普通实例属性，可运行时调整）。
两个动作：

1. **挂钩连接函数** —— 让**每一次**新建连接（含所有重连）都带上调优后的
   参数。重连同样走 `NapCatWebSocketClient.connect`，所以挂在这一处即可全覆盖。
2. **巡检已建立的连接** —— 处理「插件加载时连接已经建立」的情况，以及任何
   绕过挂钩的重连。

谁来判定「连接死了」
--------------------
两端都有心跳，所以容忍度必须**有次序**：

* **服务端（SnowLuma / NapCat）** 才是判定生死的权威——它清楚自己的状态，
  回收时会发一个带原因的优雅关闭（1001），客户端能看到确切理由。
* **客户端（本插件）** 因此绝不能抢在服务端之前断开：`ping_timeout` 默认
  放宽到 300 秒，与服务端心跳容忍对齐。一次卡顿，若服务端本打算「再等等」，
  就不该被客户端单方面的 1011 变成断线。

真断线照样被发现，且**不依赖心跳**：进程一死，TCP 立刻关闭（FIN/RST），
客户端的接收循环立刻报错并重连；只读不消费的客户端由服务端写背压兜底。

设计原则：只放宽容忍度，**绝不伪造心跳**。真断线照样会被发现并重连。
插件停用/卸载时会把连接函数还原回原样。
"""

import asyncio
import importlib
from typing import Any, Optional

from core.plugin import BasePlugin, logger

_SELF_PLUGIN_ID = "ws-keepalive"

# 巡检间隔（秒）：足够快以兜住重连，又不至于频繁打扰。
_WATCH_INTERVAL_S = 15.0


def _safe_float(value: Any, default: float, minimum: Optional[float] = None) -> float:


    """把配置值安全转成 float。

    - 非法值（None / 非数字 / NaN）→ 回退 default
    - 数值但低于 ```minimum``` → 收敛到 minimum（比"悄悄换回默认值"更可预期）
    """
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if out != out:  # NaN
        return default
    if minimum is not None and out < minimum:
        return minimum
    return out


def _section(cfg: dict, name: str) -> dict:


    """读取一个 section 的字段，兼容两种下发形态。

    框架通常直接把字段平铺在 section 下（``{"section_x": {"field": v}}``），
    但个别路径会保留 schema 的 ``fields`` 包装。两种都接受，避免配置静默失效。
    """
    sec = cfg.get(name, {}) or {}
    if not isinstance(sec, dict):
        return {}
    fields = sec.get("fields")
    return fields if isinstance(fields, dict) else sec


def _is_connection(obj: Any) -> bool:


    """鸭子类型判断 websockets 连接对象（各版本/异步实现都有这三个属性）。"""
    return all(hasattr(obj, attr) for attr in ("ping_timeout", "ping_interval", "close_timeout"))


class WsKeepalivePlugin(BasePlugin):
    """放宽 OneBot WebSocket 的保活预算，避免卡顿时误判断线。"""

    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)

        # ========== 从 section_keepalive 读取保活参数 ==========
        keep = _section(cfg, "section_keepalive")
        # 300s：与服务端（SnowLuma）的心跳容忍对齐（30s × (9+1) ≈ 300s）。客户端
        # 绝不能抢在服务端之前断开，否则一次卡顿就被单方面 1011 成断线；真断线
        # 由 TCP 兜底（进程死 → 立刻 FIN/RST），不依赖客户端心跳。
        self.ping_timeout = _safe_float(keep.get("ping_timeout"), 300.0, minimum=5.0)
        self.ping_interval = _safe_float(keep.get("ping_interval"), 20.0, minimum=5.0)
        self.open_timeout = _safe_float(keep.get("open_timeout"), 10.0, minimum=3.0)

        # ========== 从 section_advanced 读取高级选项 ==========
        adv = _section(cfg, "section_advanced")
        self.debug_log = bool(adv.get("debug_log", False))

        # 运行时状态
        self._patched = False
        self._orig_connect: Optional[Any] = None
        self._wrapper: Optional[Any] = None
        self._watch_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    # ── 生命周期 ─────────────────────────────────────────────────────────

    async def initialize(self):
        ok = self._patch_connect()
        if ok:
            logger.info(
                f"[{_SELF_PLUGIN_ID}] 已应用保活调优: "
                f"ping_timeout={self.ping_timeout}s ping_interval={self.ping_interval}s "
                f"open_timeout={self.open_timeout}s（覆盖新建连接与所有重连）"
            )
        else:
            logger.warning(
                f"[{_SELF_PLUGIN_ID}] 无法挂钩连接函数（KiraAI 版本可能不同）；"
                f"将仅对已建立的连接生效"
            )

        # 适配器可能在本插件加载前就已连接，先实时调优一次
        n = self._retune_live()
        if n and self.debug_log:
            logger.info(f"[{_SELF_PLUGIN_ID}] 初始化时已调优 {n} 个已建立连接")

        # 巡检**始终**运行：它不只为「加载前已连接」兜底，也是挂钩失败时
        # （KiraAI 版本不同）唯一能保证重连后参数不回退的机制。开销可忽略
        # （每 15 秒几次属性比较，微秒级），因此不提供关闭开关。
        self._stop_event = asyncio.Event()
        self._watch_task = asyncio.create_task(self._watch_loop())

    async def terminate(self):
        if self._stop_event is not None:
            self._stop_event.set()
        if self._watch_task is not None:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"[{_SELF_PLUGIN_ID}] 巡检任务收尾异常: {e}")
            self._watch_task = None
        self._restore_connect()
        logger.info(f"[{_SELF_PLUGIN_ID}] 已停止")

    # ── 动作 1：挂钩连接函数 ─────────────────────────────────────────────

    def _patch_connect(self) -> bool:
        """包装 `napcat_client` 模块级的 `ws_compatible_connect`。

        挂**函数**而不是**方法**：KiraAI 的初次连接和每次重连都走
        `NapCatWebSocketClient.connect`，而它内部调这个函数 —— 挂一处全覆盖。
        """
        if self._patched:
            return True
        try:
            from core.adapter.src.qq import napcat_client as pkg

            module = importlib.import_module(f"{pkg.__name__}.client")
        except Exception as e:
            logger.debug(f"[{_SELF_PLUGIN_ID}] 导入 napcat_client 失败: {e}")
            return False

        original = getattr(module, "ws_compatible_connect", None)
        if original is None or not callable(original):
            return False

        # 若当前函数已经是【我们自己的包装】，先拆到真正的原始函数再重新包装 ——
        # 直接链式包装会让内层（更早的实例）在自己那一层把参数又改回去，
        # 结果是"新实例配的值被旧实例覆盖"。拆开重包则永远是最外层说了算。
        if getattr(original, "_ws_keepalive_wrapper", False):
            real = getattr(original, "_ws_keepalive_original", None)
            if callable(real):
                original = real

        plugin = self

        def tuned_connect(uri, *, extra_headers, **kwargs):
            # 适配器对这三个参数都传了显式值（ping_timeout=10.0, open_timeout=5.0），
            # 所以必须【赋值覆盖】，不能用 setdefault —— setdefault 只在键缺失时生效，
            # 对显式传入的值毫无作用（open_timeout 曾经因此没被改到）。
            kwargs["ping_timeout"] = plugin.ping_timeout
            kwargs["ping_interval"] = plugin.ping_interval
            kwargs["open_timeout"] = plugin.open_timeout
            if plugin.debug_log:
                logger.info(
                    f"[{_SELF_PLUGIN_ID}] 建立连接: ping_timeout={kwargs.get('ping_timeout')} "
                    f"ping_interval={kwargs.get('ping_interval')} "
                    f"open_timeout={kwargs.get('open_timeout')}"
                )
            return original(uri, extra_headers=extra_headers, **kwargs)

        # 打上标记，供上面"拆开重包"与 _restore_connect 的守卫识别
        tuned_connect._ws_keepalive_wrapper = True  # type: ignore[attr-defined]
        tuned_connect._ws_keepalive_original = original  # type: ignore[attr-defined]

        self._orig_connect = original
        self._wrapper = tuned_connect
        module.ws_compatible_connect = tuned_connect
        self._patched = True
        return True

    def _restore_connect(self) -> None:
        """还原连接函数。

        只在本模块仍然持有**我们自己的**包装函数时才还原 —— 热重载时可能出现
        「旧实例 terminate() 晚于新实例 initialize()」的顺序，若无条件还原，
        旧实例会把新实例刚打上的补丁一起抹掉。
        """
        if not self._patched or self._orig_connect is None:
            return
        try:
            from core.adapter.src.qq import napcat_client as pkg

            module = importlib.import_module(f"{pkg.__name__}.client")
            if getattr(module, "ws_compatible_connect", None) is not self._wrapper:
                # 已被别人替换（新实例/其它插件）——不要动它
                self._patched = False
                return
            module.ws_compatible_connect = self._orig_connect
        except Exception:
            pass
        self._patched = False

    # ── 动作 2：实时调优已建立的连接 ─────────────────────────────────────

    def _adapter_clients(self):
        """枚举适配器管理器下所有存活的 NapCatWebSocketClient。"""
        try:
            adapter_mgr = getattr(self.ctx, "adapter_mgr", None)
            if adapter_mgr is None:
                return
            # AdapterManager 暴露 get_adapters()；原始 dict 是私有的 _adapters
            getter = getattr(adapter_mgr, "get_adapters", None)
            adapters = getter() if callable(getter) else getattr(adapter_mgr, "_adapters", {})
            if not adapters:
                return
            values = adapters.values() if isinstance(adapters, dict) else adapters
            for adapter in values:
                bot = getattr(adapter, "bot", None)
                if bot is not None and hasattr(bot, "websocket"):
                    yield bot
        except Exception as e:
            logger.debug(f"[{_SELF_PLUGIN_ID}] 枚举适配器失败: {e}")

    def _retune_live(self) -> int:
        """把调优参数套用到每个已建立的连接上，返回生效的连接数。"""
        applied = 0
        for bot in self._adapter_clients():
            ws = getattr(bot, "websocket", None)
            if ws is None or not _is_connection(ws):
                continue
            # 已经是我们想要的值就跳过，避免无意义的写入与日志噪音
            if (getattr(ws, "ping_timeout", None) == self.ping_timeout
                    and getattr(ws, "ping_interval", None) == self.ping_interval):
                continue
            try:
                ws.ping_timeout = self.ping_timeout
                ws.ping_interval = self.ping_interval
                applied += 1
                if self.debug_log:
                    logger.info(
                        f"[{_SELF_PLUGIN_ID}] 已对现有连接生效: "
                        f"ping_timeout={ws.ping_timeout} ping_interval={ws.ping_interval}"
                    )
            except Exception as e:
                logger.debug(f"[{_SELF_PLUGIN_ID}] 调整现有连接失败: {e}")
        return applied

    async def _watch_loop(self) -> None:
        """定时巡检：确保重连后的连接不会退回 10 秒。

        开销极小（几次属性写入），且值一致时直接跳过。
        """
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=_WATCH_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            if self._stop_event.is_set():
                break
            try:
                n = self._retune_live()
                if n and self.debug_log:
                    logger.info(f"[{_SELF_PLUGIN_ID}] 巡检: 已刷新 {n} 个连接")
            except Exception as e:
                logger.debug(f"[{_SELF_PLUGIN_ID}] 巡检出错: {e}")
