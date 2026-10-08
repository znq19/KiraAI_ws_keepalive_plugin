#!/usr/bin/env python3
"""ws-keepalive 自校验：证明插件真的把参数送到了 websockets.connect。

存在意义
--------
这个插件历史上出过一次「日志说一套、实际另一套」的 bug：`open_timeout` 用了
``kwargs.setdefault(...)``，而 KiraAI 适配器是**显式**传 ``open_timeout=5.0``
的 —— setdefault 只在键缺失时生效，于是永远输给调用方：插件日志宣布 10 秒，
socket 却是 5 秒建起来的。这类 bug 靠读代码很难发现，必须**实测最终参数**。

本脚本用桩替换 ``websockets.connect``，按适配器的真实调用形态跑一遍，断言
最终落在 kwargs 里的值 == 插件配置值。

用法（在插件目录下）:
    python3 selfcheck.py                 # 用默认配置 300/20/10
    python3 selfcheck.py 300 20 10       # 指定 ping_timeout ping_interval open_timeout

退出码 0 = 全部通过；1 = 有参数没送达。
"""
from __future__ import annotations

import os
import sys
import types

FAIL = []


def build_stub_env() -> dict:
    """桩掉 core.plugin 与 napcat_client，返回 kwargs 捕获字典。"""
    core = types.ModuleType("core")
    plugin_mod = types.ModuleType("core.plugin")

    class _Logger:
        def _f(self, lvl):
            def f(msg, *a, **kw):
                try:
                    text = msg % a if a else msg
                except Exception:
                    text = f"{msg} {a}"
                print(f"    [{lvl}] {text}")
            return f

        def info(self, m, *a, **k): self._f("INFO")(m, *a, **k)
        def warning(self, m, *a, **k): self._f("WARN")(m, *a, **k)
        def error(self, m, *a, **k): self._f("ERR")(m, *a, **k)
        def debug(self, m, *a, **k): pass

    class BasePlugin:
        def __init__(self, ctx, cfg):
            self.ctx, self.cfg = ctx, cfg
            self.plugin_cfg = cfg

    plugin_mod.BasePlugin = BasePlugin
    plugin_mod.logger = _Logger()
    sys.modules["core"] = core
    sys.modules["core.plugin"] = plugin_mod

    # ── 桩 core.utils.path_utils（迁移要用的 get_config_path，指向临时目录）──
    import tempfile
    from pathlib import Path
    utils_mod = types.ModuleType("core.utils")
    path_utils_mod = types.ModuleType("core.utils.path_utils")
    _tmp = Path(tempfile.mkdtemp(prefix="ws_keepalive_selfcheck_"))
    path_utils_mod.get_config_path = lambda: _tmp
    sys.modules["core.utils"] = utils_mod
    sys.modules["core.utils.path_utils"] = path_utils_mod

    captured: dict = {}

    def ws_compatible_connect(uri, *, extra_headers, **kwargs):
        captured.clear()
        captured.update(kwargs)
        return object()

    for name in ("core.adapter", "core.adapter.src", "core.adapter.src.qq",
                 "core.adapter.src.qq.napcat_client"):
        m = types.ModuleType(name)
        m.__path__ = []
        sys.modules[name] = m
    client = types.ModuleType("core.adapter.src.qq.napcat_client.client")
    client.ws_compatible_connect = ws_compatible_connect
    sys.modules["core.adapter.src.qq.napcat_client.client"] = client
    return captured


def main() -> int:
    want_pt = float(sys.argv[1]) if len(sys.argv) > 1 else 300.0
    want_pi = float(sys.argv[2]) if len(sys.argv) > 2 else 20.0
    want_ot = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0

    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, here)
    captured = build_stub_env()

    print("① 加载插件 …")
    import main as plug  # noqa: E402

    class Ctx:
        adapter_mgr = None

    # 与上游 schema 一致的结构（section_keepalive / section_advanced）
    p = plug.WsKeepalivePlugin(Ctx(), {
        "section_keepalive": {"ping_timeout": want_pt, "ping_interval": want_pi,
                              "open_timeout": want_ot},
        "section_advanced": {"debug_log": False},
    })

    print("② 挂钩连接函数 …")
    ok = p._patch_connect()
    print(f"    {'✅' if ok else '❌'} _patch_connect() = {ok}")
    if not ok:
        FAIL.append("_patch_connect() 返回 False（无法挂钩连接函数）")

    print("③ 按适配器真实调用形态触发一次 connect …")
    import core.adapter.src.qq.napcat_client.client as cmod
    # 这一行逐字复刻 KiraAI: core/adapter/src/qq/napcat_client/client.py
    cmod.ws_compatible_connect(
        "ws://localhost:3015", extra_headers={},
        max_size=2 ** 24, open_timeout=5.0, ping_timeout=10.0,
    )

    print("④ 断言最终参数 = 插件配置 …")
    for key, want in [("ping_timeout", want_pt), ("ping_interval", want_pi),
                      ("open_timeout", want_ot)]:
        got = captured.get(key)
        good = got is not None and abs(float(got) - want) < 1e-9
        print(f"    {'✅' if good else '❌'} {key}: 期望 {want} → 实际 {got}")
        if not good:
            FAIL.append(f"{key}: 期望 {want} 实际 {got}")

    print("⑤ 反向验证：适配器硬编码的 5.0 / 10.0 必须已被覆盖 …")
    if abs(float(captured.get("open_timeout", 0)) - 5.0) < 1e-9:
        FAIL.append("open_timeout 仍是适配器硬编码的 5.0（setdefault 回归）")
        print("    ❌ open_timeout 仍是 5.0 —— setdefault 回归！")
    else:
        print("    ✅ 适配器硬编码值已被覆盖")

    print()
    if FAIL:
        print("结果：❌ 失败")
        for f in FAIL:
            print("   -", f)
        return 1
    print("结果：✅ 全部通过 —— 插件配置确实送达 websockets.connect")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
