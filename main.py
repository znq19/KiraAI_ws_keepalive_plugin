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
import copy
import importlib
import json
import time
from typing import Any, Optional

from core.plugin import BasePlugin, logger
from core.utils.path_utils import get_config_path

_SELF_PLUGIN_ID = "ws-keepalive"

# 巡检间隔（秒）：足够快以兜住重连，又不至于频繁打扰。
_WATCH_INTERVAL_S = 15.0

# ── 一次性配置迁移 ──────────────────────────────────────────────────────
# 背景：框架会把 schema 默认值**物化**进配置文件，所以 v1.0.1 的存量用户文件里
# 存的是旧默认 ``ping_timeout=30``；框架只补缺失键、不会覆盖已有值——不迁的话
# 他们永远停在 30。这里把「还停在旧默认 30」的用户一次性迁到新默认 300，
# 用户自己改过的值一律不动；配置里的版本哨兵保证只迁一次。
_CFG_VERSION_KEY = "_cfg_version"
_CFG_VERSION = 2                       # 2 = ping_timeout 默认 300
_LEGACY_PING_TIMEOUT_DEFAULT = 30.0    # v1.0.1 及更早的默认
_NEW_PING_TIMEOUT_DEFAULT = 300.0      # 本版起的新默认


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

        # 一次性迁移：把「还停在旧默认 ping_timeout=30」的存量用户迁到新默认 300。
        # 必须在读取参数之前做，且 self.plugin_cfg 与注册表缓存是同一份，
        # 原地改 + 落盘后 WebUI 与后续运行都看到迁移后的值。
        self._migrate_config_once()

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

        # ========== 从 section_group_cache 读取群信息缓存参数 ==========
        # 默认开：群名在 TTL 内不会变化，重复读取返回同一份数据，
        # 只是把「每条群消息一次 get_group_info」的 WS 往返省掉。
        gcache = _section(cfg, "section_group_cache")
        self.group_cache_enabled = bool(gcache.get("enabled", True))
        self.group_cache_ttl = _safe_float(gcache.get("ttl"), 600.0, minimum=5.0)
        self.group_cache_max = int(_safe_float(gcache.get("max_entries"), 512.0, minimum=16.0))

        # 运行时状态
        self._patched = False
        self._orig_connect: Optional[Any] = None
        self._wrapper: Optional[Any] = None
        self._group_cache_patched = False
        self._orig_get_group_info: Optional[Any] = None
        self._watch_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    # ── 一次性配置迁移 ─────────────────────────────────────────────────

    def _migrate_config_once(self) -> None:
        """把「还停在旧默认 ping_timeout=30」的存量用户迁到新默认 300，只迁一次。

        - **只动旧默认值**：值恰好等于 30 才改；用户自己设的值（45/120/…）不碰。
          （无法区分「用户特意设了 30」与「一直是默认 30」——前者会被迁一次，
          可再改回，且因哨兵不会被再迁。）
        - **只迁一次**：写入版本哨兵 ``_cfg_version``。之后用户再把 ping_timeout
          改回 30，也不会被重复迁移。
        """
        if _safe_float(self.plugin_cfg.get(_CFG_VERSION_KEY), 0.0) >= _CFG_VERSION:
            return  # 已迁过
        keep = _section(self.plugin_cfg, "section_keepalive")
        cur = keep.get("ping_timeout")
        if cur is None or abs(
            _safe_float(cur, _LEGACY_PING_TIMEOUT_DEFAULT) - _LEGACY_PING_TIMEOUT_DEFAULT
        ) < 1e-9:
            keep["ping_timeout"] = _NEW_PING_TIMEOUT_DEFAULT
        self.plugin_cfg[_CFG_VERSION_KEY] = _CFG_VERSION
        self._persist_config()

    def _persist_config(self) -> bool:
        """把内存里的 plugin_cfg 写回插件配置文件。

        ``self.plugin_cfg`` 与注册表在内存里持有的是**同一份** dict，所以原地
        修改 + 落盘后，WebUI 与后续运行无需重载就能看到迁移后的值。
        （与框架内置 agent 插件的迁移写法一致；不调用 update_plugin_config，
        以免在 initialize 期间触发本插件重载。）
        """
        config_path = get_config_path() / "plugins" / f"{_SELF_PLUGIN_ID}.json"
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with config_path.open("w", encoding="utf-8") as f:
                json.dump(self.plugin_cfg, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[{_SELF_PLUGIN_ID}] 迁移配置落盘失败: {e}")
            return False
        return True

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

        # 群信息缓存：把「每条群消息一次 get_group_info」的重复 WS 往返省掉
        # 关闭时也调用：会拆掉旧实例（热重载残留）的包装，确保关闭立即生效
        cache_ok = self._patch_group_cache()
        if self.group_cache_enabled:
            if cache_ok:
                logger.info(
                    f"[{_SELF_PLUGIN_ID}] 群信息缓存已启用: ttl={self.group_cache_ttl:.0f}s "
                    f"max={self.group_cache_max} 群（只缓存成功响应，失败不缓存）"
                )
            else:
                logger.warning(
                    f"[{_SELF_PLUGIN_ID}] 无法挂钩 get_group_info（KiraAI 版本可能不同）；"
                    f"群信息缓存已跳过，其余功能不受影响"
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
        self._restore_group_cache()
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

    # ── 动作 3：群信息缓存（砍掉每条群消息一次 WS 往返） ────────────────

    @staticmethod
    def _napcat_client_class():
        """定位 `NapCatWebSocketClient`；拿不到就返回 None（功能自动跳过）。"""
        try:
            from core.adapter.src.qq import napcat_client as pkg

            module = importlib.import_module(f"{pkg.__name__}.client")
            return getattr(module, "NapCatWebSocketClient", None)
        except Exception:
            return None

    @staticmethod
    def _group_cacheable(resp: Any) -> bool:
        """只缓存「明确成功」的响应；失败/异常一律不缓存。"""
        if not isinstance(resp, dict):
            return False
        if resp.get("status") not in (None, "ok"):
            return False
        if resp.get("retcode") not in (None, 0):
            return False
        return isinstance(resp.get("data"), dict)

    def _make_group_info_wrapper(self, orig):
        """构造带缓存的 `get_group_info` 包装函数。

        安全边界（逐条都有自检覆盖）：
        * 只缓存成功响应；异常原样抛出，绝不缓存、绝不吞掉；
        * 命中时返回 **deepcopy**，调用方改返回的 dict 也不会污染缓存；
        * 缓存挂在**客户端实例**上（每个 bot 账号独立、上限固定），命中判断
          纯内存：一次 dict 查找 + deepcopy，无锁、无 await、无阻塞；
        * 未命中时完全按原逻辑 `await orig(...)`，超时/异常语义不变；
        * 只影响 `get_group_info` 的重复读取，其它 API 与消息路径一律不碰。
        """
        ttl = self.group_cache_ttl
        max_entries = self.group_cache_max
        enabled = self.group_cache_enabled
        cacheable = self._group_cacheable

        async def cached_get_group_info(client_self, *args, **kwargs):
            if not enabled:
                return await orig(client_self, *args, **kwargs)

            gid = kwargs.get("group_id")
            if gid is None and args:
                gid = args[0]
            if gid is None:
                return await orig(client_self, *args, **kwargs)

            key = str(gid)
            now = time.monotonic()
            cache = client_self.__dict__.get("_wskeep_group_info_cache")
            if not isinstance(cache, dict):
                cache = {}
                try:
                    client_self._wskeep_group_info_cache = cache
                except Exception:
                    # 实例不允许写属性时退回无缓存（绝不影响主流程）
                    return await orig(client_self, *args, **kwargs)

            hit = cache.get(key)
            # 防御：条目结构异常（旧版本残留/外部误写）时当作未命中，绝不让
            # 缓存里的脏数据把 get_group_info 变成异常源。
            if isinstance(hit, tuple) and len(hit) == 2 and hit[0] > now:
                return copy.deepcopy(hit[1])

            resp = await orig(client_self, *args, **kwargs)
            if cacheable(resp):
                # 过期时间从**响应返回时**起算（慢请求不会白吃 TTL）
                cache[key] = (time.monotonic() + ttl, copy.deepcopy(resp))
                if len(cache) > max_entries:
                    # 有界：按插入顺序淘汰最旧的（dict 保序，不额外记账）
                    for old_key in list(cache.keys())[: len(cache) - max_entries]:
                        cache.pop(old_key, None)
            return resp

        # 标记 + 原始实现：热重载/卸载时据此识别并还原，绝不套娃
        cached_get_group_info.__wskeep_group_cache__ = True
        cached_get_group_info.__wrapped__ = orig
        return cached_get_group_info

    def _patch_group_cache(self) -> bool:
        """把带缓存的 `get_group_info` 装到客户端类上（新老实例同时生效）。

        关闭时不是简单 return：如果类上还残留**上一个插件实例**的包装
        （热重载时旧实例可能来不及 terminate），这里把它拆回原始实现，
        确保「改成关闭」在重载后立即生效。
        """
        cls = self._napcat_client_class()
        if cls is None:
            return False
        cur = cls.__dict__.get("get_group_info")
        if cur is None:
            return False
        if not self.group_cache_enabled:
            if getattr(cur, "__wskeep_group_cache__", False):
                orig = getattr(cur, "__wrapped__", None)
                if orig is not None:
                    cls.get_group_info = orig
            return False
        if getattr(cur, "__wskeep_group_cache__", False):
            # 热重载残留的旧包装：拆回原始实现再重包，避免缓存套缓存
            cur = getattr(cur, "__wrapped__", None)
            if cur is None:
                return False
        self._orig_get_group_info = cur
        cls.get_group_info = self._make_group_info_wrapper(cur)
        self._group_cache_patched = True
        return True

    def _restore_group_cache(self) -> None:
        """卸载时还原；只还原我们自己的包装（别人后来覆盖的不动）。"""
        if not self._group_cache_patched or self._orig_get_group_info is None:
            return
        try:
            cls = self._napcat_client_class()
            if cls is not None:
                cur = cls.__dict__.get("get_group_info")
                if getattr(cur, "__wskeep_group_cache__", False):
                    cls.get_group_info = self._orig_get_group_info
        except Exception:
            pass
        self._group_cache_patched = False

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
