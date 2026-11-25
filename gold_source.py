"""DNF 金币行情取数（uu898 源）。

dd373 从 2026-11-19 起对非浏览器客户端一律弹「滑动验证」滑块，无头 Edge 也拦，
urllib / UA 伪装全部拿不到挂单，所以换成 uu898：

- uu898 的 DNF 游戏币页按「大区」分页（广东区、北京区……共 28 个大区，每页 20 条挂单）；
- 每条挂单自带区服名（如「广东区 / 广州1／2区」）和「1元=X万金」，是服务端渲染的 UTF-8 HTML；
- 按 DNF 跨区大区表把区服映射回 跨一 ~ 跨八，每个跨区取最高比例（1 元换到的万金币越多越划算）。

纯标准库实现，不依赖第三方包。
"""

from __future__ import annotations

import gzip
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

SOURCE = "uu898"

URL_TMPL = "https://www.uu898.com/tgWeb/newTradeTG.aspx?gm=95&b=4&c=-3&chan=3&p=1&area={area_id}"

# uu898 的 DNF 大区 id（游戏区服栏里抄来的，体验区 1153 用不上，跳过）
AREAS = {
    2322: "广东区", 2323: "北京区", 2324: "四川区", 2325: "江苏区",
    2326: "湖北区", 2327: "广西区", 2328: "东北区", 2329: "浙江区",
    2330: "山东区", 2331: "上海区", 2332: "西南区", 2333: "安徽区",
    2334: "河北区", 2335: "辽宁区", 2336: "福建区", 2337: "华北区",
    2338: "重庆区", 2339: "黑龙江区", 2340: "江西区", 2341: "西北区",
    2342: "陕西区", 2343: "吉林区", 2344: "山西区", 2345: "天津区",
    2346: "河南区", 2347: "湖南区", 2348: "新疆区", 2349: "内蒙古区",
    2350: "云贵区",
}

ZONES = ["跨一", "跨二", "跨三A", "跨三B", "跨四", "跨五", "跨六", "跨七", "跨八"]

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
    "Referer": "https://www.uu898.com/",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# 合理水位过滤：DNF 各区 1 元换 60~300 万金币，超出当脏数据/钓鱼单丢掉
MIN_RATIO = 60.0
MAX_RATIO = 300.0

RE_ROW = re.compile(r'class="qf_txt">游戏区服：(.*?)</p>', re.S)
RE_RATIO = re.compile(r"1元=([\d.]+)万金")

# DNF 跨区大区表（2025 版，见 README）：
# 连续号段用 _RANGES，跳号的（北京 1/2/4、山东 1/2/7、东北 4~6 之类）用 _EXACT。
_EXACT = {
    "北京": {1: "跨六", 2: "跨六", 3: "跨八", 4: "跨六"},
    "东北": {1: "跨六", 2: "跨六", 3: "跨六", 4: "跨七", 5: "跨七", 6: "跨七", 7: "跨六"},
    "山东": {1: "跨六", 2: "跨六", 3: "跨八", 4: "跨八", 5: "跨八", 6: "跨八", 7: "跨六"},
    "吉林": {1: "跨六", 2: "跨六"},
    "辽宁": {1: "跨六", 2: "跨七", 3: "跨七"},
    "山西": {1: "跨六", 2: "跨八"},
    "天津": {1: "跨八"},
    "内蒙古": {1: "跨八"},
    "重庆": {1: "跨三B", 2: "跨三B"},
    "云南": {1: "跨三B"},
    "贵州": {1: "跨三B"},
    "云贵": {1: "跨三B"},
    "广州": {1: "跨一", 2: "跨一"},
    "新疆": {1: "跨三A"},
}

_RANGES = [
    ("广东", 1, 13, "跨一"), ("广西", 1, 5, "跨一"),
    ("湖北", 1, 8, "跨二"), ("湖南", 1, 7, "跨二"),
    ("四川", 1, 6, "跨三A"), ("西北", 1, 3, "跨三A"),
    ("西南", 1, 3, "跨三B"), ("陕西", 1, 3, "跨三B"),
    ("江苏", 5, 8, "跨四"), ("浙江", 4, 7, "跨四"), ("上海", 4, 5, "跨四"),
    ("江西", 3, 3, "跨四"), ("安徽", 3, 3, "跨四"), ("福建", 3, 4, "跨四"),
    ("江苏", 1, 4, "跨五"), ("浙江", 1, 3, "跨五"), ("上海", 1, 3, "跨五"),
    ("江西", 1, 2, "跨五"), ("安徽", 1, 2, "跨五"), ("福建", 1, 2, "跨五"),
    ("河北", 1, 1, "跨六"), ("河南", 1, 2, "跨六"), ("黑龙江", 1, 1, "跨六"),
    ("华北", 1, 1, "跨六"),
    ("河北", 2, 5, "跨七"), ("河南", 3, 7, "跨七"),
    ("黑龙江", 2, 3, "跨八"), ("华北", 4, 4, "跨八"),
]

RE_SERVER = re.compile(r"^([\u4e00-\u9fa5]+?)([\d/]+)$")


def _fetch(area_id: int, timeout: float = 20.0) -> str:
    """抓一个大区的挂单页，gzip 自己解（uu898 会带 Content-Encoding: gzip）。"""
    req = urllib.request.Request(URL_TMPL.format(area_id=area_id), headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        gz = resp.headers.get("Content-Encoding", "").lower() == "gzip"
    if gz or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")


def parse_items(html: str) -> list[tuple[str, float]]:
    """(区服名, 1元=X万金) 列表；只认「1元=」，48元=4800万金 那种整单价不参与比较。"""
    out: list[tuple[str, float]] = []
    for blk in html.split('class="splb_list"')[1:]:
        m = RE_ROW.search(blk)
        q = RE_RATIO.search(blk)
        if not (m and q):
            continue
        parts = [re.sub(r"\s+", "", x)
                 for x in re.sub(r"<[^>]+>", "|", m.group(1)).split("|") if x.strip()]
        if not parts:
            continue
        try:
            out.append((parts[-1], float(q.group(1))))
        except ValueError:
            continue
    return out


def zone_of(server: str) -> str | None:
    """把「广州1／2区」这种区服名映射成 跨X；表里没有的新服返回 None。"""
    name = server.replace("／", "/").replace("　", "").strip()
    if name.endswith("区"):
        name = name[:-1]
    m = RE_SERVER.match(name)
    if not m:
        return None
    prefix, nums = m.group(1), m.group(2)
    numbers = [int(x) for x in nums.split("/") if x.isdigit()]
    if not numbers:
        return None

    exact = _EXACT.get(prefix)
    if exact:
        for n in numbers:
            if n in exact:
                return exact[n]
    for pre, lo, hi, zone in _RANGES:
        if pre == prefix and any(lo <= n <= hi for n in numbers):
            return zone
    return None


def collect() -> tuple[list[dict], list[str]]:
    """跑一轮：返回 (每个跨区一行, 失败的大区说明)。"""
    agg: dict[str, list[float]] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_fetch, aid): aid for aid in AREAS}
        for fut in as_completed(futs):
            aid = futs[fut]
            try:
                html = fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{AREAS[aid]} {type(exc).__name__}")
                continue
            for server, ratio in parse_items(html):
                zone = zone_of(server)
                if not zone or not (MIN_RATIO <= ratio <= MAX_RATIO):
                    continue
                agg.setdefault(zone, []).append(ratio)

    rows: list[dict] = []
    for zone in ZONES:
        vals = agg.get(zone) or []
        if vals:
            rows.append({"zone": zone, "best": max(vals), "count": len(vals), "err": ""})
        else:
            rows.append({"zone": zone, "best": None, "count": 0, "err": "无数据"})
    return rows, errors
