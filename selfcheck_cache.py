"""group_info 缓存自检（离线，不依赖 KiraAI 本体）。

覆盖：命中/穿透、深拷贝隔离、TTL 过期、异常不缓存、失败响应不缓存、
有界淘汰、关闭开关、卸载还原、热重载不套娃、并发安全。
"""
import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

PLUGIN = Path(__file__).resolve().parent
FAIL = []


def check(name, cond, detail=""):
    print(("✓ " if cond else "✗ ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


# ── 桩：core.plugin / napcat_client ─────────────────────────────────────
class FakeBasePlugin:
    def __init__(self, ctx=None, cfg=None):
        self.ctx = ctx
        self.plugin_cfg = cfg or {}


class FakeClient:
    """记录调用次数；可切换返回/异常。"""

    def __init__(self):
        self.calls = []
        self.mode = "ok"

    async def get_group_info(self, group_id, **kwargs):
        self.calls.append(group_id)
        if self.mode == "raise":
            raise TimeoutError("boom")
        if self.mode == "fail":
            return {"status": "failed", "retcode": 1200, "data": None}
        return {"status": "ok", "retcode": 0,
                "data": {"group_id": group_id, "group_name": f"群{group_id}"}}


def install_stubs():
    core = types.ModuleType("core"); core.__path__ = []
    plugin = types.ModuleType("core.plugin")
    plugin.BasePlugin = FakeBasePlugin
    plugin.logger = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        debug=lambda *a, **k: None, error=lambda *a, **k: None)
    utils = types.ModuleType("core.utils"); utils.__path__ = []
    path_utils = types.ModuleType("core.utils.path_utils")
    path_utils.get_config_path = lambda: Path("/tmp")
    adapter = types.ModuleType("core.adapter"); adapter.__path__ = []
    src = types.ModuleType("core.adapter.src"); src.__path__ = []
    qq = types.ModuleType("core.adapter.src.qq"); qq.__path__ = []
    napcat = types.ModuleType("core.adapter.src.qq.napcat_client"); napcat.__path__ = []
    client_mod = types.ModuleType("core.adapter.src.qq.napcat_client.client")
    client_mod.NapCatWebSocketClient = FakeClient
    for name, mod in {
        "core": core, "core.plugin": plugin, "core.utils": utils,
        "core.utils.path_utils": path_utils, "core.adapter": adapter,
        "core.adapter.src": src, "core.adapter.src.qq": qq,
        "core.adapter.src.qq.napcat_client": napcat,
        "core.adapter.src.qq.napcat_client.client": client_mod,
    }.items():
        sys.modules[name] = mod
    return client_mod


client_mod = install_stubs()
sys.path.insert(0, str(PLUGIN))
mod = importlib.import_module("main")
mod.time = types.SimpleNamespace(monotonic=lambda: clock[0])   # 可控时钟
clock = [1000.0]

CFG = {"section_group_cache": {"enabled": True, "ttl": 600, "max_entries": 512}}


def fresh(**over):
    c = json.loads(json.dumps(CFG))
    c["section_group_cache"].update(over)
    return mod.WsKeepalivePlugin(ctx=None, cfg=c)


async def main():
    # 1. 命中：两次读取只打一次 WS
    p = fresh(); p._patch_group_cache()
    c = FakeClient()
    a = await c.get_group_info(123)
    b = await c.get_group_info("123")          # int/str 同键
    check("1 二次读取命中缓存", len(c.calls) == 1, f"calls={c.calls}")
    check("1b 返回值一致", a == b)
    p._restore_group_cache()

    # 2. 深拷贝：改返回值不污染缓存
    p = fresh(); p._patch_group_cache(); c = FakeClient()
    a = await c.get_group_info(7); a["data"]["group_name"] = "被改了"
    b = await c.get_group_info(7)
    check("2 修改返回值不污染缓存", b["data"]["group_name"] == "群7")
    p._restore_group_cache()

    # 3. TTL 过期
    p = fresh(ttl=5); p._patch_group_cache(); c = FakeClient()
    await c.get_group_info(1); clock[0] += 4.9
    await c.get_group_info(1)
    check("3a TTL 内命中", len(c.calls) == 1)
    clock[0] += 0.2
    await c.get_group_info(1)
    check("3b TTL 过期后重新请求", len(c.calls) == 2)
    p._restore_group_cache()

    # 4. 异常不缓存、原样抛出
    p = fresh(); p._patch_group_cache(); c = FakeClient(); c.mode = "raise"
    try:
        await c.get_group_info(9); ok = False
    except TimeoutError:
        ok = True
    check("4a 异常原样抛出", ok)
    c.mode = "ok"
    await c.get_group_info(9)
    check("4b 异常未被缓存（下次成功请求）", len(c.calls) == 2)
    p._restore_group_cache()

    # 5. 失败响应不缓存
    p = fresh(); p._patch_group_cache(); c = FakeClient(); c.mode = "fail"
    await c.get_group_info(5)
    check("5a 失败响应返回原样", True)
    c.mode = "ok"
    await c.get_group_info(5)
    check("5b 失败响应未缓存", len(c.calls) == 2)
    p._restore_group_cache()

    # 6. 有界淘汰
    p = fresh(max_entries=16); p._patch_group_cache(); c = FakeClient()
    for g in range(20):
        await c.get_group_info(g)
    await c.get_group_info(0)
    check("6 超出上限淘汰最旧", len(c.calls) == 21, f"calls={len(c.calls)}")
    p._restore_group_cache()

    # 7. 关闭开关 = 完全原逻辑
    p = fresh(enabled=False); patched = p._patch_group_cache()
    c = FakeClient()
    await c.get_group_info(1); await c.get_group_info(1)
    check("7 关闭时不打补丁", patched is False and len(c.calls) == 2)

    # 8. 卸载还原
    p = fresh(); orig = FakeClient.get_group_info
    p._patch_group_cache()
    check("8a 已打补丁", FakeClient.get_group_info is not orig)
    p._restore_group_cache()
    check("8b 已还原", FakeClient.get_group_info is orig)

    # 9. 热重载不套娃
    p1 = fresh(); p1._patch_group_cache()
    p2 = fresh(); p2._patch_group_cache()
    c = FakeClient()
    await c.get_group_info(3); await c.get_group_info(3)
    check("9 双实例不套娃（仍只 1 次）", len(c.calls) == 1)
    p2._restore_group_cache()

    # 10. 并发：同一群同时 10 个请求不应报错/丢数据
    p = fresh(); p._patch_group_cache(); c = FakeClient()
    rs = await asyncio.gather(*[c.get_group_info(42) for _ in range(10)])
    check("10a 并发全部返回",
          all(r["data"]["group_name"] == "群42" for r in rs))
    before = len(c.calls)
    await c.get_group_info(42)  # 并发结束后再读一次，应命中
    check("10b 并发后缓存生效", len(c.calls) == before, f"calls={len(c.calls)}")
    p._restore_group_cache()

    # 11. 热重载：开 → 关，旧包装必须被拆掉（否则「关闭」不生效）
    p1 = fresh(enabled=True); p1._patch_group_cache()
    orig = getattr(FakeClient.get_group_info, "__wrapped__", FakeClient.get_group_info)
    p2 = fresh(enabled=False)                 # 新实例配置=关闭
    patched2 = p2._patch_group_cache()        # initialize 会无条件调用
    c = FakeClient()
    await c.get_group_info(7); await c.get_group_info(7)
    check("11a 关闭重载后不再走缓存", patched2 is False and len(c.calls) == 2)
    check("11b 类方法已还原为原始实现", FakeClient.get_group_info is orig)

    # 12. 缓存里的脏数据不能让 get_group_info 变成异常源
    p = fresh(); p._patch_group_cache(); c = FakeClient()
    c._wskeep_group_info_cache = {1: "junk"}      # 结构异常的旧条目
    r = await c.get_group_info(1)
    check("12a 脏条目按未命中处理", r["data"]["group_name"] == "群1" and len(c.calls) == 1)
    c._wskeep_group_info_cache = "not-a-dict"     # 整个缓存字段类型错误
    r = await c.get_group_info(2)
    check("12b 缓存字段类型错误也不炸", r["data"]["group_name"] == "群2")
    p._restore_group_cache()

    print("\n" + ("ALL PASSED" if not FAIL else f"FAILED: {FAIL}"))
    return 0 if not FAIL else 1


sys.exit(asyncio.run(main()))
