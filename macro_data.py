# -*- coding: utf-8 -*-
"""Dynamic macro and industry data pipeline shared by all covered equities.

The pipeline discovers current NSO publications through its public WordPress
search API, retrieves annual comparison series from the World Bank API, and
adds industry-specific publications from the same source catalogue. It never
stores hard-coded observations or ticker-specific macro values.

Cache is shared through MongoDB when supplied by the caller, with an in-process
fallback. Source publication dates and reference periods remain separate.
"""
from __future__ import annotations

import html
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import quote, urljoin

import requests


NSO_BASE_URL = os.getenv("MACRO_NSO_BASE_URL", "https://www.nso.gov.vn").rstrip("/")
NSO_SEARCH_URL = f"{NSO_BASE_URL}/wp-json/wp/v2/search"
WORLD_BANK_API = os.getenv("MACRO_WORLD_BANK_API", "https://api.worldbank.org/v2")
CACHE_TTL_SECONDS = max(900, int(os.getenv("MACRO_CACHE_TTL_SECONDS", "21600")))
REQUEST_TIMEOUT = (8, 25)
MAX_GENERAL_FACTS = max(5, int(os.getenv("MACRO_MAX_GENERAL_FACTS", "24")))
MAX_INDUSTRY_FACTS = max(3, int(os.getenv("MACRO_MAX_INDUSTRY_FACTS", "12")))
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VNEquityMacroResearch/2.0)",
    "Accept": "application/json,text/html,application/xhtml+xml,*/*",
}
_CACHE = {}
_CACHE_LOCK = threading.RLock()
_KEY_LOCKS = {}
_SESSION = requests.Session()
_SESSION.headers.update(_HEADERS)


# These are indicator definitions/search intents, not observations or ticker
# assignments. Values are always retrieved from the external source at runtime.
GENERAL_PUBLICATION_QUERIES = (
    ("GDP", "tổng sản phẩm trong nước GDP tăng trưởng kinh tế"),
    ("CPI", "chỉ số giá tiêu dùng CPI lạm phát cơ bản"),
    ("IIP", "chỉ số sản xuất công nghiệp IIP"),
    ("PPI", "chỉ số giá sản xuất giá nguyên liệu đầu vào"),
    ("TRADE", "xuất nhập khẩu cán cân thương mại"),
    ("FDI", "vốn đầu tư trực tiếp nước ngoài FDI"),
    ("PUBLIC_INVESTMENT", "vốn đầu tư công giải ngân đầu tư phát triển"),
    ("RETAIL", "tổng mức bán lẻ hàng hóa doanh thu dịch vụ tiêu dùng"),
    ("LABOUR", "lao động việc làm thu nhập tiền lương"),
    ("CONSTRUCTION", "hoạt động xây dựng thị trường bất động sản"),
    ("MONETARY", "tín dụng lãi suất tăng trưởng tín dụng"),
    ("EXCHANGE_RATE", "tỷ giá đô la Mỹ USD"),
    ("ENERGY", "sản lượng điện tiêu thụ năng lượng giá xăng dầu"),
)

# Industry search intents are generic sector vocabularies. Actual company
# classification is read from its metadata and is not mapped by ticker here.
# Generic dimensions applied to every industry label discovered from live company
# metadata. No ticker allow-list or sector-to-query mapping is maintained.
INDUSTRY_QUERY_DIMENSIONS = (
    ("INDUSTRY_OUTPUT", "sản lượng sản xuất công suất hoạt động"),
    ("INDUSTRY_PRICING", "giá bán giá nguyên liệu chi phí đầu vào biên lợi nhuận"),
    ("INDUSTRY_DEMAND", "doanh thu tiêu thụ nhu cầu thị trường đơn hàng"),
    ("INDUSTRY_TRADE", "xuất khẩu nhập khẩu thị trường quốc tế"),
    ("INDUSTRY_CAPEX", "đầu tư mở rộng công suất dự án"),
    ("INDUSTRY_INVENTORY", "tồn kho hàng tồn kho nguyên vật liệu"),
    ("INDUSTRY_POLICY", "quy định chính sách pháp lý thuế"),
    ("INDUSTRY_LABOUR", "lao động tiền lương nhân công"),
)


# World Bank annual series complement, not replace, current official monthly
# Vietnamese releases. Codes are stable public indicator identifiers.
WORLD_BANK_INDICATORS = (
    ("GDP_GROWTH", "NY.GDP.MKTP.KD.ZG", "GDP growth (annual %)"),
    ("INFLATION", "FP.CPI.TOTL.ZG", "Inflation, consumer prices (annual %)"),
    ("OFFICIAL_EXCHANGE_RATE", "PA.NUS.FCRF", "Official exchange rate (LCU per US$, period average)"),
    ("LENDING_RATE", "FR.INR.LEND", "Lending interest rate (%)"),
    ("FDI_NET_INFLOW", "BX.KLT.DINV.WD.GD.ZS", "Foreign direct investment, net inflows (% of GDP)"),
    ("MANUFACTURING_VALUE_ADDED", "NV.IND.MANF.ZS", "Manufacturing, value added (% of GDP)"),
    ("TRADE_OPENNESS", "NE.TRD.GNFS.ZS", "Trade (% of GDP)"),
    ("GROSS_CAPITAL_FORMATION", "NE.GDI.FTOT.ZS", "Gross capital formation (% of GDP)"),
)


class _NSDPTableParser(HTMLParser):
    """Read the official NSDP table and discover its SDMX download URLs."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self._row = None
        self._cell = None
        self._anchor = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = {"text": [], "links": []}
        elif tag == "a" and self._cell is not None:
            self._anchor = {"href": attrs.get("href"), "text": []}

    def handle_data(self, data):
        value = data.strip()
        if not value:
            return
        if self._cell is not None:
            self._cell["text"].append(value)
        if self._anchor is not None:
            self._anchor["text"].append(value)

    def handle_endtag(self, tag):
        if tag == "a" and self._anchor is not None:
            self._cell["links"].append({
                "href": self._anchor.get("href"),
                "text": " ".join(self._anchor["text"]).strip(),
            })
            self._anchor = None
        elif tag in ("td", "th") and self._cell is not None:
            self._cell["text"] = " ".join(self._cell["text"]).strip()
            self._row.append(self._cell)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


class _ArticleParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.meta = {}
        self._skip = 0
        self._capture = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in ("script", "style", "nav", "footer", "header", "svg"):
            self._skip += 1
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name") or attrs.get("itemprop")
            value = attrs.get("content")
            if key and value:
                self.meta[key.lower()] = value.strip()
        if tag in ("p", "h1", "h2", "h3", "li"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "nav", "footer", "header", "svg") and self._skip:
            self._skip -= 1
        if tag in ("p", "h1", "h2", "h3", "li"):
            self.parts.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        value = html.unescape(data).strip()
        if value:
            self.parts.append(value + " ")

    @property
    def text(self):
        return re.sub(r"\n{3,}", "\n\n", " ".join(self.parts)).strip()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _get(url, params=None):
    response = _SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


def _plain(value):
    if isinstance(value, dict):
        value = value.get("rendered", "")
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def _parse_date(value):
    if not value:
        return None
    value = str(value).strip()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        pass
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value[:10], fmt).date().isoformat()
        except Exception:
            continue
    return None


def _article_date(parser, api_item=None):
    api_item = api_item or {}
    for key in ("article:published_time", "datepublished", "date", "dc.date", "pubdate"):
        found = _parse_date(parser.meta.get(key))
        if found:
            return found
    for key in ("date", "modified"):
        found = _parse_date(api_item.get(key))
        if found:
            return found
    match = re.search(r"(?:ngày đăng|date of issue|published on|ngày phát hành)\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{4})", parser.text, re.I)
    return _parse_date(match.group(1)) if match else None


def _extract_reference_period(text):
    """Extract an explicitly stated statistical reference period, not publication date."""
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    patterns = (
        (r"(\d{1,2})\s*tháng\s*(?:đầu\s*)?(?:năm\s*)?(\d{4})", lambda m: f"{m.group(1)} tháng năm {m.group(2)}"),
        (r"tháng\s*(\d{1,2})\s*năm\s*(\d{4})", lambda m: f"{m.group(1)}-{m.group(2)}"),
        (r"quý\s*([1-4ivx]+)\s*(?:năm|/)\s*(\d{4})", lambda m: f"Q{m.group(1).upper()}-{m.group(2)}"),
        (r"tháng\s*(\d{1,2})\s*[/\-]\s*(\d{4})", lambda m: f"{m.group(1)}-{m.group(2)}"),
        (r"năm\s*(\d{4})", lambda m: m.group(1)),
    )
    for pattern, formatter in patterns:
        match = re.search(pattern, value, re.I)
        if match:
            return formatter(match)
    return None


def _discover_nso_publications(query, per_page=4):
    """Discover current NSO publications from its public WordPress search API."""
    try:
        response = _get(NSO_SEARCH_URL, {
            "search": query,
            "per_page": min(10, max(1, int(per_page))),
            "_locale": "user",
        })
        payload = response.json()
        return payload if isinstance(payload, list) else []
    except Exception:
        # Some WordPress installations disable the search endpoint; fail closed.
        return []


def _fetch_nso_publication(item, category, query):
    url = item.get("url") or item.get("link")
    if not url or not str(url).startswith(("https://", "http://")):
        return None
    try:
        response = _get(url)
        parser = _ArticleParser()
        parser.feed(response.text)
        body = parser.text
        if not body:
            return None
        title = _plain(item.get("title")) or "Công bố thống kê"
        published = _article_date(parser, item)
        # Publication date is not confused with the statistical reference period.
        excerpt = body[:1800].strip()
        age_days = None
        freshness = "PUBLISH_DATE_UNVERIFIED"
        if published:
            try:
                age_days = (datetime.now(timezone.utc).date() - datetime.fromisoformat(published).date()).days
                freshness = "RECENT_PUBLICATION" if age_days <= 90 else "STALE_PUBLICATION"
            except Exception:
                age_days = None
        return {
            "category": category,
            "title": title,
            "query": query,
            "published_at": published,
            "publication_age_days": age_days,
            "period_or_date": _extract_reference_period(title + " " + body) or "Chưa xác định kỳ tham chiếu từ metadata",
            "excerpt": excerpt,
            "source": url,
            "source_name": "Cục Thống kê Việt Nam",
            "retrieved_at": _now_iso(),
            "freshness": freshness,
        }
    except Exception as exc:
        return {
            "category": category,
            "title": _plain(item.get("title")) or "Công bố thống kê",
            "query": query,
            "source": url,
            "source_name": "Cục Thống kê Việt Nam",
            "error": f"Không tải được bài nguồn ({type(exc).__name__}).",
            "retrieved_at": _now_iso(),
        }


def _fetch_nso_search_pack(searches, category_prefix, per_query=3, limit=24):
    candidates = []
    seen = set()
    warnings = []
    for category, query in searches:
        items = _discover_nso_publications(query, per_page=per_query)
        for item in items:
            url = item.get("url") or item.get("link")
            if not url or url in seen:
                continue
            seen.add(url)
            candidates.append((category_prefix + category, query, item))
    # Fetch the newest discovery results first; API order is relevance, so sort
    # by API date when available and otherwise preserve the source ranking.
    candidates.sort(key=lambda row: str(row[2].get("date") or row[2].get("modified") or ""), reverse=True)
    facts = []
    for category, query, item in candidates[:limit]:
        fact = _fetch_nso_publication(item, category, query)
        if fact:
            facts.append(fact)
    if not facts:
        warnings.append("NSO: API tìm kiếm không trả về bài thống kê tải được; không thay thế bằng dữ liệu tự tạo.")
    return facts, warnings


def _nsdp_category(title):
    low = str(title or "").casefold()
    rules = (
        (("national accounts", "gross domestic product", "gdp"), "GDP"),
        (("consumer price", "cpi"), "CPI"),
        (("interest rate",), "INTEREST_RATE"),
        (("exchange rate",), "EXCHANGE_RATE"),
        (("merchandise trade", "trade"), "TRADE"),
        (("production index", "production"), "INDUSTRIAL_PRODUCTION"),
        (("producer price", "ppi"), "PPI"),
        (("balance of payments",), "BALANCE_OF_PAYMENTS"),
        (("government operations",), "FISCAL"),
        (("gross debt", "external debt"), "PUBLIC_DEBT"),
        (("labor market", "labour market"), "LABOUR"),
        (("financial soundness",), "FINANCIAL_STABILITY"),
        (("stock market",), "CAPITAL_MARKET"),
    )
    for terms, category in rules:
        if any(term in low for term in terms):
            return category
    return "MACRO_OTHER"


def _local_tag(element):
    return str(element.tag).split("}")[-1].casefold()


def _element_value(element):
    for key, value in element.attrib.items():
        if key.casefold() in ("value", "obs_value", "observation_value"):
            return value
    return (element.text or "").strip()


def _parse_sdmx_observations(xml_content, dataset_title, category, source_url):
    """Parse common SDMX-ML compact/generic observation layouts conservatively."""
    try:
        root = ET.fromstring(xml_content)
    except Exception:
        return []

    facts = []
    series_nodes = [node for node in root.iter() if _local_tag(node) == "series"]
    for series_index, series in enumerate(series_nodes[:12], 1):
        series_key = dict(series.attrib)
        observations = []
        for node in series.iter():
            if _local_tag(node) == "value" and node.attrib.get("id"):
                series_key[node.attrib.get("id")] = node.attrib.get("value", "")
            if _local_tag(node) != "obs":
                continue
            period = None
            value = None
            status = None
            for key, item in node.attrib.items():
                normalized = key.casefold()
                if normalized in ("time_period", "time", "period", "obs_dimension"):
                    period = item
                elif normalized in ("obs_value", "value", "observation_value"):
                    value = item
                elif normalized in ("obs_status", "status"):
                    status = item
            for child in node.iter():
                tag = _local_tag(child)
                if tag in ("obsdimension", "timedimension") and not period:
                    period = _element_value(child)
                elif tag in ("obsvalue", "observationvalue") and value is None:
                    value = _element_value(child)
                elif tag in ("attribute", "obsattribute") and child.attrib.get("id", "").casefold() in ("obs_status", "status"):
                    status = child.attrib.get("value") or _element_value(child)
            if period and value not in (None, ""):
                observations.append((str(period), str(value), status))
        if not observations:
            continue
        observations.sort(key=lambda item: item[0])
        recent = observations[-3:]
        key_text = ", ".join(f"{k}={v}" for k, v in list(series_key.items())[:8])
        values_text = "; ".join(
            f"{period}: {value}" + (f" (status {status})" if status else "")
            for period, value, status in recent
        )
        facts.append({
            "category": category,
            "title": dataset_title,
            "period_or_date": recent[-1][0],
            "series_key": key_text[:400],
            "excerpt": f"Chuỗi SDMX {dataset_title}; khóa chuỗi: {key_text}; 3 quan sát mới nhất: {values_text}.",
            "source": source_url,
            "source_name": "Vietnam National Summary Data Page (IMF e-GDDS)",
            "retrieved_at": _now_iso(),
            "freshness": "OBSERVATION_PERIOD_VERIFIED",
        })
    return facts


def _fetch_nsdp_sdmx_facts():
    """Discover and retrieve official Vietnamese macro SDMX datasets from NSDP."""
    nsdp_url = os.getenv("MACRO_NSDP_URL", "https://nsdp.nso.gov.vn/index.htm")
    facts = []
    warnings = []
    try:
        response = _get(nsdp_url)
        parser = _NSDPTableParser()
        parser.feed(response.text)
    except Exception as exc:
        return facts, [f"NSDP: không tải được danh mục SDMX ({type(exc).__name__})."]

    datasets = []
    for row in parser.rows:
        if not row:
            continue
        title = row[0].get("text", "")
        category = _nsdp_category(title)
        for cell in row:
            for link in cell.get("links", []):
                label = link.get("text", "").casefold()
                href = link.get("href")
                if "sdmx" in label and href:
                    url = urljoin(nsdp_url, href)
                    if url.startswith(("https://", "http://")):
                        datasets.append((title, category, url))
    seen = set()
    for title, category, url in datasets:
        if url in seen:
            continue
        seen.add(url)
        try:
            response = _get(url)
            parsed = _parse_sdmx_observations(response.content, title, category, url)
            facts.extend(parsed)
        except Exception as exc:
            warnings.append(
                f"NSDP {title}: không tải/đọc được SDMX ({type(exc).__name__}); "
                "không thay bằng số liệu ước tính."
            )
    if not facts and not warnings:
        warnings.append("NSDP: chưa phát hiện được liên kết SDMX có dữ liệu đọc được.")
    return facts[:48], warnings


def _fetch_world_bank_facts():
    facts = []
    warnings = []
    for category, indicator, label in WORLD_BANK_INDICATORS:
        url = f"{WORLD_BANK_API}/country/VNM/indicator/{indicator}"
        try:
            response = _get(url, {"format": "json", "per_page": 8})
            payload = response.json()
            rows = payload[1] if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list) else []
            row = next((item for item in rows if item.get("value") is not None), None)
            if not row:
                warnings.append(f"World Bank: chưa có quan sát khả dụng cho {label}.")
                continue
            value = row.get("value")
            unit = row.get("unit") or ""
            facts.append({
                "category": category,
                "title": label,
                "period_or_date": str(row.get("date") or "Không rõ"),
                "published_at": None,
                "value": value,
                "unit": unit,
                "excerpt": f"{label}: {value} {unit} (quan sát năm {row.get('date')}); chuỗi dữ liệu World Bank, tần suất năm.",
                "source": f"{url}?format=json",
                "source_name": "World Bank Open Data API",
                "retrieved_at": _now_iso(),
                "freshness": "ANNUAL_SERIES",
            })
        except Exception as exc:
            warnings.append(f"World Bank {indicator}: truy xuất thất bại ({type(exc).__name__}).")
    return facts, warnings


def _industry_queries(industry, sector):
    """Build categorized discovery searches from live issuer/industry metadata.

    The same metric dimensions are applied to every discovered industry label;
    no ticker-to-sector rules or fabricated sector observations are used.
    """
    labels = []
    for value in (industry, sector):
        label = re.sub(r"\s+", " ", str(value or "")).strip()
        if label and label.casefold() not in ("unknown", "none", "n/a", "unclassified"):
            if label.casefold() not in {item.casefold() for item in labels}:
                labels.append(label)

    if not labels:
        return []

    primary = labels[0]
    queries = [("INDUSTRY_OVERVIEW", f'"{primary}" Việt Nam dữ liệu ngành')]
    queries.extend(
        (category, f'"{primary}" Việt Nam {dimension}')
        for category, dimension in INDUSTRY_QUERY_DIMENSIONS
    )
    if len(labels) > 1:
        queries.append(("INDUSTRY_CLASSIFICATION", f'"{labels[1]}" Việt Nam "{primary}" ngành'))

    deduped = []
    seen = set()
    for category, query in queries:
        normalized = re.sub(r"\s+", " ", query).strip()
        if normalized and normalized.casefold() not in seen:
            seen.add(normalized.casefold())
            deduped.append((category, normalized))
    return deduped[:9]


def _fetch_industry_pack(industry, sector):
    searches = _industry_queries(industry, sector)
    if not searches:
        return [], ["Không có phân loại ngành doanh nghiệp; chỉ áp dụng dữ liệu vĩ mô chung."]
    facts, warnings = _fetch_nso_search_pack(
        searches, category_prefix="", per_query=2, limit=MAX_INDUSTRY_FACTS
    )
    # Keep the raw industry classification on every fact for auditability.
    for fact in facts:
        fact["industry_classification"] = industry or sector
    return facts, warnings


def _summarize_industry_coverage(industry, pack):
    """Create an auditable coverage summary without treating missing dates as fresh."""
    facts = [fact for fact in (pack or {}).get("facts", []) if isinstance(fact, dict)]
    warnings = list((pack or {}).get("warnings") or [])
    sources = sorted({
        str(fact.get("source_name") or fact.get("source") or "").strip()
        for fact in facts
        if fact.get("source_name") or fact.get("source")
    })
    freshness_counts = {}
    for fact in facts:
        label = str(fact.get("freshness") or "UNKNOWN").upper()
        freshness_counts[label] = freshness_counts.get(label, 0) + 1

    dated_facts = sum(
        1 for fact in facts
        if fact.get("published_at") or (
            fact.get("period_or_date")
            and str(fact.get("period_or_date")).casefold() not in (
                "chưa xác định kỳ tham chiếu từ metadata", "không rõ", "unknown"
            )
        )
    )
    stale_facts = freshness_counts.get("STALE_PUBLICATION", 0)
    verified_facts = sum(
        freshness_counts.get(key, 0)
        for key in ("RECENT_PUBLICATION", "OBSERVATION_PERIOD_VERIFIED", "ANNUAL_SERIES")
    )
    if not facts:
        status = "MISSING"
    elif stale_facts == len(facts):
        status = "STALE"
    elif verified_facts:
        status = "COVERED"
    else:
        status = "PARTIAL"

    periods = sorted({
        str(fact.get("period_or_date")).strip()
        for fact in facts
        if fact.get("period_or_date")
        and str(fact.get("period_or_date")).casefold() not in (
            "chưa xác định kỳ tham chiếu từ metadata", "không rõ", "unknown"
        )
    })
    published_dates = sorted({
        str(fact.get("published_at")).strip()
        for fact in facts if fact.get("published_at")
    })
    return {
        "industry": str(industry or "Chưa phân loại"),
        "status": status,
        "fact_count": len(facts),
        "source_count": len(sources),
        "sources": sources,
        "dated_fact_count": dated_facts,
        "stale_fact_count": stale_facts,
        "verified_fact_count": verified_facts,
        "freshness_counts": freshness_counts,
        "reference_periods": periods[-8:],
        "latest_publication_date": published_dates[-1] if published_dates else None,
        "warnings": warnings[:12],
    }


def _get_cached_bundle(cache_key, fetcher, force_refresh=False, cache_store=None):
    # Per-key lock prevents 100 parallel symbol analyses from stampeding the
    # same public API while preserving concurrency between distinct industries.
    with _CACHE_LOCK:
        key_lock = _KEY_LOCKS.setdefault(cache_key, threading.Lock())

    with key_lock:
        now = time.time()
        with _CACHE_LOCK:
            cached = _CACHE.get(cache_key)
            if cached and not force_refresh and now - cached[0] < CACHE_TTL_SECONDS:
                return cached[1]

        if cache_store is not None and not force_refresh:
            try:
                stored = cache_store.find_one({"_id": cache_key})
                if stored and isinstance(stored.get("payload"), dict):
                    fetched_at = stored.get("fetched_epoch", 0)
                    if fetched_at and now - float(fetched_at) < CACHE_TTL_SECONDS:
                        payload = stored["payload"]
                        with _CACHE_LOCK:
                            _CACHE[cache_key] = (now, payload)
                        return payload
            except Exception:
                pass

        payload = fetcher()
        payload["cache_key"] = cache_key
        payload["fetched_at"] = _now_iso()
        with _CACHE_LOCK:
            _CACHE[cache_key] = (now, payload)
        if cache_store is not None:
            try:
                cache_store.update_one(
                    {"_id": cache_key},
                    {"$set": {"payload": payload, "fetched_epoch": now, "updated_at": payload["fetched_at"]}},
                    upsert=True,
                )
            except Exception:
                pass
        return payload


def _fetch_global_bundle():
    nso_facts, nso_warnings = _fetch_nso_search_pack(
        GENERAL_PUBLICATION_QUERIES, category_prefix="", per_query=3,
        limit=MAX_GENERAL_FACTS
    )
    nsdp_facts, nsdp_warnings = _fetch_nsdp_sdmx_facts()
    wb_facts, wb_warnings = _fetch_world_bank_facts()
    return {
        "scope": "GLOBAL_VIETNAM_MACRO",
        "facts": nso_facts + nsdp_facts + wb_facts,
        "warnings": nso_warnings + nsdp_warnings + wb_warnings,
    }


def get_macro_context(symbol, sector, industry=None, metadata=None,
                      force_refresh=False, cache_store=None):
    """Return shared macro facts plus a dynamically discovered industry pack.

    The ticker is retained for traceability only; cache keys are global/industry,
    so all 100 symbols reuse the same source pulls rather than scraping 100 times.
    """
    metadata = metadata or {}
    industry = industry or metadata.get("industry") or metadata.get("industry_name") or metadata.get("icb_name")
    global_pack = _get_cached_bundle(
        "GLOBAL_VIETNAM_MACRO", _fetch_global_bundle,
        force_refresh=force_refresh, cache_store=cache_store
    )
    industry_key = re.sub(r"\s+", " ", str(industry or sector or "UNCLASSIFIED")).strip().casefold()
    industry_key = "INDUSTRY:" + industry_key[:160]
    industry_pack = _get_cached_bundle(
        industry_key, lambda: _fetch_industry_pack(industry, sector),
        force_refresh=force_refresh, cache_store=cache_store
    )
    facts = list(global_pack.get("facts") or []) + list(industry_pack.get("facts") or [])
    warnings = list(global_pack.get("warnings") or []) + list(industry_pack.get("warnings") or [])
    return {
        "symbol": str(symbol or "").upper(),
        "industry": industry or sector or "Chưa phân loại",
        "fetched_at": _now_iso(),
        "facts": facts,
        "warnings": warnings,
        "coverage": {
            "global_fact_count": len(global_pack.get("facts") or []),
            "industry_fact_count": len(industry_pack.get("facts") or []),
            "global_cache_key": "GLOBAL_VIETNAM_MACRO",
            "industry_cache_key": industry_key,
        },
    }


def refresh_macro_contexts(industry_labels, cache_store=None, force_refresh=True):
    """Scheduled refresh for the shared macro pack and every active industry.

    industry_labels accepts (industry_label, sector_fallback) pairs and is
    normally built from the live stocks collection, not a ticker allow-list.
    """
    global_pack = _get_cached_bundle(
        "GLOBAL_VIETNAM_MACRO", _fetch_global_bundle,
        force_refresh=force_refresh, cache_store=cache_store
    )
    refreshed = {
        "global_facts": len(global_pack.get("facts") or []),
        "industries": {},
        "industry_coverage": [],
    }
    seen = set()
    for item in industry_labels or []:
        if isinstance(item, (tuple, list)):
            industry = item[0] if item else None
            sector = item[1] if len(item) > 1 else None
        else:
            industry, sector = item, None
        label = industry or sector
        normalized = re.sub(r"\s+", " ", str(label or "UNCLASSIFIED")).strip().casefold()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        cache_key = "INDUSTRY:" + normalized[:160]
        def fetch_industry(industry=industry, sector=sector):
            facts, warnings = _fetch_industry_pack(industry, sector)
            return {"facts": facts, "warnings": warnings}

        pack = _get_cached_bundle(
            cache_key, fetch_industry,
            force_refresh=force_refresh,
            cache_store=cache_store,
        )
        refreshed["industries"][normalized] = len(pack.get("facts") or [])
        refreshed["industry_coverage"].append(
            _summarize_industry_coverage(label, pack)
        )

    statuses = [item["status"] for item in refreshed["industry_coverage"]]
    refreshed["coverage"] = {
        "industry_count": len(statuses),
        "covered_industries": statuses.count("COVERED"),
        "partial_industries": statuses.count("PARTIAL"),
        "missing_industries": statuses.count("MISSING"),
        "stale_industries": statuses.count("STALE"),
    }
    refreshed["refreshed_at"] = _now_iso()

    # Persist one compact audit document alongside per-industry source caches.
    refreshed["coverage_report_persisted"] = False
    if cache_store is not None:
        try:
            report = {
                "generated_at": refreshed["refreshed_at"],
                "global_fact_count": refreshed["global_facts"],
                "coverage": refreshed["coverage"],
                "industries": refreshed["industry_coverage"],
            }
            cache_store.update_one(
                {"_id": "MACRO_COVERAGE_REPORT"},
                {"$set": {
                    "payload": report,
                    "fetched_epoch": time.time(),
                    "updated_at": refreshed["refreshed_at"],
                }},
                upsert=True,
            )
            refreshed["coverage_report_persisted"] = True
        except Exception as exc:
            refreshed["coverage_report_error"] = type(exc).__name__
    return refreshed


def format_macro_context(bundle):
    """Format source facts for the AI with dates, reference periods and provenance."""
    if not isinstance(bundle, dict):
        return "Không có dữ liệu vĩ mô/ngành được truy xuất."
    lines = [
        f"Thời điểm tổng hợp: {bundle.get('fetched_at', 'không rõ')}.",
        f"Phân loại ngành đầu vào: {bundle.get('industry', 'Chưa phân loại')}.",
        f"Độ phủ: {json.dumps(bundle.get('coverage', {}), ensure_ascii=False)}.",
        "DỮ KIỆN VĨ MÔ/NGÀNH TỪ NGUỒN BÊN NGOÀI:",
    ]
    facts = bundle.get("facts") or []
    for fact in facts:
        if fact.get("excerpt"):
            lines.append(
                f"- [{fact.get('category')}] {fact.get('title')} | "
                f"Kỳ tham chiếu: {fact.get('period_or_date', 'chưa xác định')} | "
                f"Ngày công bố: {fact.get('published_at') or 'chưa xác minh'} | "
                f"Trạng thái ngày: {fact.get('freshness', 'UNKNOWN')} | "
                f"{fact['excerpt']} | Nguồn: {fact.get('source_name')} | URL: {fact.get('source')}"
            )
        elif fact.get("error"):
            lines.append(
                f"- [{fact.get('category')}] {fact.get('title')}: {fact['error']} | "
                f"Nguồn: {fact.get('source')}"
            )
    for warning in bundle.get("warnings") or []:
        lines.append(f"- CẢNH BÁO: {warning}")
    if not facts:
        lines.append("- Không có dữ liệu định lượng đủ điều kiện xác minh.")
    return "\n".join(lines)
