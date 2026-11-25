"""DNF 金币行情 · /查询金价（uu898 源）

dd373 现在对非浏览器客户端一律弹「滑动验证」滑块（无头 Edge 也过不去），
2026-11-19 起换成 uu898：抓 28 个 DNF 大区的游戏币挂单页，按跨区大区表
归并成 跨一 ~ 跨八，每个跨区取最高「1 元 = X 万金币」。

- 默认缓存 180 秒；插件加载时会先在后台预热一次，群里第一次查不用干等 20 秒。
- 只在配置的群里生效（`groups` 留空 = 所有群）。
"""

from __future__ import annotations

import asyncio
import re
import threading
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

try:                                    # 插件包内相对导入
    from .gold_source import SOURCE, ZONES, collect
except ImportError:                     # 兜底：按顶层模块导入
    from gold_source import SOURCE, ZONES, collect


@register("astrbot_plugin_dnf_gold", "Tonagi",
          "DNF 金币行情查询：按跨区排列的 uu898 实时比例", "v1.1.0")
class DnfGold(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.config = config or {}
        self._cache_at = 0.0
        self._cache_rows: list[dict] | None = None
        self._last_call: dict[str, float] = {}
        self._lock = threading.Lock()
        threading.Thread(target=self._prewarm, name="dnf_gold_prewarm", daemon=True).start()

    # ---------------------------------------------------------------- 配置
    def _cfg(self, key: str, default=None):
        try:
            value = self.config.get(key, default)
        except Exception:  # noqa: BLE001
            value = default
        return default if value is None else value

    @staticmethod
    def _as_list(raw) -> list[str]:
        if isinstance(raw, str):
            raw = re.split(r"[,，\s]+", raw)
        if isinstance(raw, (list, tuple)):
            return [str(x).strip() for x in raw if str(x).strip()]
        return []

    # ---------------------------------------------------------------- 数据
    def _prewarm(self) -> None:
        try:
            self._load()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[dnf_gold] 预热失败: {type(exc).__name__}: {exc}")

    def _load(self) -> list[dict]:
        ttl = float(self._cfg("cache_seconds", 180) or 180)
        now = time.time()
        with self._lock:
            if self._cache_rows is not None and now - self._cache_at < ttl:
                return self._cache_rows
        rows, errors = collect()
        ok = sum(1 for r in rows if r.get("best"))
        logger.info(f"[dnf_gold] 刷新 {len(rows)} 个跨区，有数 {ok} 个"
                    + (f"，失败大区 {len(errors)} 个：{', '.join(errors)}" if errors else ""))
        with self._lock:
            self._cache_rows = rows
            self._cache_at = time.time()
        return rows

    def _render(self, rows: list[dict]) -> str:
        stamp = time.strftime("%m-%d %H:%M", time.localtime(self._cache_at or time.time()))
        lines = [f"DNF 金币行情 · {SOURCE} · {stamp}"]
        for row in rows:
            zone = row["zone"]
            if not row.get("best"):
                lines.append(f"{zone}: {row.get('err') or '无数据'}")
                continue
            lines.append(f"{zone}: {row['best']:.2f}万/元")
        if all(not r.get("best") for r in rows):
            lines.append("（uu898 整轮没抓到挂单，可能限流，过会儿再试）")
        return "\n".join(lines)

    # ---------------------------------------------------------------- 命令
    @filter.command("查询金价", alias=["金价", "dnf金价"])
    async def query_gold(self, event: AstrMessageEvent):
        try:
            group_id = str(event.get_group_id() or "")
        except Exception:  # noqa: BLE001
            group_id = ""
        groups = self._as_list(self._cfg("groups", []))
        if groups and group_id and group_id not in groups:
            return                      # 群号不在白名单：静默忽略

        cooldown = float(self._cfg("cooldown_seconds", 120) or 120)
        key = group_id or "private"
        now = time.time()
        if now - self._last_call.get(key, 0.0) < cooldown:
            return
        self._last_call[key] = now

        loop = asyncio.get_running_loop()
        try:
            rows = await loop.run_in_executor(None, self._load)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[dnf_gold] 抓取失败: {exc}")
            yield event.plain_result(f"金价抓取失败：{type(exc).__name__}（uu898 可能限流，稍后再试）")
            return
        yield event.plain_result(self._render(rows))
