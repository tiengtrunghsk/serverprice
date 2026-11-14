import os
import sys
import time
import json
import base64
import threading
import concurrent.futures
import logging
import numpy as np
import pandas as pd
import re
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

import requests
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ===================== LOGGING CHẨN ĐOÁN =====================
logger = logging.getLogger("price_api")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    _formatter.converter = lambda ts: (datetime.utcfromtimestamp(ts) + timedelta(hours=7)).timetuple()
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
logger.propagate = False

def log_exception(context: str, exc: Exception):
    logger.error("%s | %s: %s", context, type(exc).__name__, str(exc), exc_info=True)

# ===================== VNSTOCK THẾ HỆ 6 =====================
from vnstock import Market

try:
    from ptkt import ProfessionalTechnicalAnalyzer
except ImportError:
    logger.warning("Không tìm thấy file ptkt.py. Điểm kỹ thuật sẽ mặc định = 50.")
    ProfessionalTechnicalAnalyzer = None

# ===================== CONFIG =====================
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
if not MONGO_URI:
    logger.critical("Thiếu biến môi trường MONGO_URI. Server dừng khởi động.")
    sys.exit(1)

SSI_API_KEY = os.environ.get("SSI_API_KEY", "").strip()
SSI_API_SECRET = os.environ.get("SSI_API_SECRET", "").strip()
if SSI_API_KEY and SSI_API_SECRET:
    os.environ["SSI_API_KEY"] = SSI_API_KEY
    os.environ["SSI_API_SECRET"] = SSI_API_SECRET
else:
    logger.warning("⚠️ CẢNH BÁO: Thiếu SSI_API_KEY hoặc SSI_API_SECRET. Có thể không lấy được giá realtime!")

ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY")
ENV_FHSC_API_KEY = os.environ.get("FHSC_API_KEY", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

SYMBOLS_JSON_FILE = "vn100_symbols.json"
VN100_INFO_FILE = "vn100_full_info.json"
SCREENER_JSON_FILE = "screener_results.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

VOLUME_MULTIPLIER = 10
_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

ALIBABA_API_KEY = os.environ.get("ALIBABA_API_KEY", "").strip()
AI_API_URL = "https://ws-8lcqaxwxdv0h9xzy.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions"
AI_MODEL_NAME = os.environ.get("AI_MODEL_NAME", "qwen3.7-max-2026-06-08")
AI_TEMPERATURE = 0.15

# ===================== ĐỌC METADATA TỪ FILE JSON =====================
VN100_METADATA = {}
if os.path.exists(VN100_INFO_FILE):
    try:
        with open(VN100_INFO_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                sym = str(item.get("symbol", "")).upper().strip()
                if sym:
                    VN100_METADATA[sym] = {
                        "company_name": item.get("company_name", "") or item.get("organName", "") or "",
                        "market_cap": float(item.get("market_cap", 0) or item.get("capitalization", 0) or 0),
                        "total_shares": int(item.get("total_shares", 0) or item.get("shares", 0) or 0)
                    }
        logger.info(f"Đã tải metadata cho {len(VN100_METADATA)} mã từ {VN100_INFO_FILE}")
    except Exception as e:
        logger.warning(f"Lỗi đọc {VN100_INFO_FILE}: {e}")

# ===================== DATABASE =====================
logger.info("Đang kết nối MongoDB...")
try:
    mongo_client = MongoClient(
        MONGO_URI, 
        serverSelectionTimeoutMS=30000, 
        connectTimeoutMS=15000, 
        socketTimeoutMS=60000,
        appname="price_api"
    )
    mongo_client.admin.command("ping")
    db = mongo_client["quant_db"]
    collection = db["stocks"]
    history_col = db["history_data"]
    bctc_col = db["bctc_data"]
    screener_col = db["screener_results"]
    settings_col = db["settings"]
    logger.info("MongoDB đã kết nối thành công!")
    
    if os.path.exists(SYMBOLS_JSON_FILE):
        with open(SYMBOLS_JSON_FILE, "r", encoding="utf-8") as f:
            json_symbols = set(str(s).upper().strip() for s in json.load(f))
        if json_symbols:
            db_symbols = set(doc["symbol"] for doc in collection.find({}, {"symbol": 1}) if doc.get("symbol"))
            for sym in (json_symbols - db_symbols): collection.insert_one({"symbol": sym})
            if db_symbols - json_symbols: collection.delete_many({"symbol": {"$in": list(db_symbols - json_symbols)}})
except Exception as e:
    log_exception("[MONGODB STARTUP] Lỗi kết nối MongoDB", e)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

# === BIẾN TRẠNG THÁI ===
is_crawling = False
is_full_data_running = False
is_screener_running = False
is_ai_running = False
LAST_TELEGRAM_TIME = 0
stop_crawl_requested = False
is_auto_crawl_paused = False

def get_current_fhsc_key():
    setting = settings_col.find_one({"_id": "fhsc_key"})
    if setting and setting.get("value"): return setting["value"]
    return ENV_FHSC_API_KEY

# ===================== HELPERS =====================
def _now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M:%S")

def _get_trading_date_str() -> str:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.hour < 9: vn_now -= timedelta(days=1)
    while vn_now.weekday() >= 5: vn_now -= timedelta(days=1)
    return vn_now.strftime("%Y-%m-%d")

def _is_after_15h() -> bool:
    """
    Trả về True nếu đã qua 15h05 — thời điểm an toàn để chốt sổ.
    Đã dời từ 15h00 -> 15h05 để tránh delay dữ liệu ATC từ API.
    """
    vn_now = datetime.utcnow() + timedelta(hours=7)
    return vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 5)

def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: log_exception("[TELEGRAM]", e)

# ===================== HÀM TÍNH CAGR & GIÁ TRUNG BÌNH QUÝ (5 NĂM) =====================
def get_historical_return_rate_with_details(symbol):
    try:
        hist_data = history_col.find_one({"symbol": symbol})
        if not hist_data or not hist_data.get("history_d1"): return None
        history_bars = hist_data["history_d1"]
        if len(history_bars) < 30: return None
            
        df = pd.DataFrame(history_bars)
        df['date'] = pd.to_datetime(df['time'])
        df = df.sort_values('date')
        
        max_date = df['date'].max()
        cutoff_date = max_date - pd.DateOffset(years=5)
        df_5years = df.loc[df['date'] >= cutoff_date].copy()
        
        if len(df_5years) < 30: return None
            
        df_5years['year_quarter'] = df_5years['date'].dt.to_period('Q')
        df_quarterly = df_5years.groupby('year_quarter').agg({'close': 'mean', 'date': 'first'}).reset_index()
        
        if len(df_quarterly) < 2: return None
            
        start_row, end_row = df_quarterly.iloc[0], df_quarterly.iloc[-1]
        days = (end_row['date'] - start_row['date']).days
        if days < 30: return None
            
        years = days / 365.25
        cagr = (end_row['close'] / start_row['close']) ** (1 / years) - 1
        cagr = max(-0.5, min(cagr, 1.0))
        
        return {
            "annual_rate_percent": round(cagr * 100, 2),
            "start_quarter": str(start_row['year_quarter']),
            "start_price_mean": round(start_row['close'], 2),
            "end_quarter": str(end_row['year_quarter']),
            "end_price_mean": round(end_row['close'], 2)
        }
    except Exception as e: return None

# ===================== HÀM BỔ SUNG: BÓC TÁCH DỮ LIỆU TÀI CHÍNH TỪ BCTC =====================
def _safe_float(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0

def _get_bctc_value(report_array, target_ids, period=None):
    """Tìm giá trị của một chỉ tiêu BCTC theo ID (hỗ trợ nhiều ID)"""
    if not report_array: return 0.0
    if isinstance(target_ids, str): target_ids = [target_ids]
    for item in report_array:
        if item.get("id") in target_ids:
            if period is None or item.get("period") == period:
                return _safe_float(item.get("value"))
    return 0.0

def extract_metrics_from_bctc_data(bctc_doc):
    """Trích xuất CFO, LNST TTM, D/E, Biên gộp từ document BCTC"""
    if not bctc_doc: return {}
    
    metrics = {}
    income = bctc_doc.get("income_statement", [])
    cashflow = bctc_doc.get("cash_flow", [])
    balance = bctc_doc.get("balance_sheet", [])
    
    periods = sorted(list(set([item.get("period") for item in income if item.get("period")])), reverse=True)
    npat_ttm = 0
    for p in periods[:4]:
        npat_ttm += _get_bctc_value(income, ["IS_NET_PROFIT_AFTER_TAX", "IS_PROFIT_AFTER_TAX", "IS_NET_PROFIT"], p)
    metrics["net_profit_ttm"] = round(npat_ttm / 1e9, 2)
    
    cf_periods = sorted(list(set([item.get("period") for item in cashflow if item.get("period")])), reverse=True)
    cfo_ttm = 0
    for p in cf_periods[:4]:
        cfo_ttm += _get_bctc_value(cashflow, "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", p)
    metrics["cfo_latest"] = round(cfo_ttm / 1e9, 2)
    
    latest_period = periods[0] if periods else None
    if latest_period:
        rev = _get_bctc_value(income, "IS_NET_REVENUE", latest_period)
        cogs = _get_bctc_value(income, "IS_COST_OF_GOODS_SOLD", latest_period)
        if rev > 0:
            metrics["gross_margin"] = round((rev + cogs) / rev * 100, 1)
            
    bs_periods = sorted(list(set([item.get("period") for item in balance if item.get("period")])), reverse=True)
    latest_bs_period = bs_periods[0] if bs_periods else None
    if latest_bs_period:
        eq = _get_bctc_value(balance, ["BS_OWNERS_EQUITY", "BS_EQUITY"], latest_bs_period)
        total_assets = _get_bctc_value(balance, "BS_TOTAL_ASSETS", latest_bs_period)
        if total_assets == 0:
            total_assets = _get_bctc_value(balance, "BS_SHORT_TERM_ASSETS", latest_bs_period) + _get_bctc_value(balance, "BS_LONG_TERM_ASSETS", latest_bs_period)
            
        liab = _get_bctc_value(balance, "BS_LIABILITIES", latest_bs_period)
        if liab == 0 and total_assets > 0 and eq > 0:
            liab = total_assets - eq
            
        if eq > 0:
            metrics["debt_to_equity"] = round(liab / eq, 2)
            
    return metrics

# ===================== CÀO GIÁ =====================
def _fetch_one(symbol: str) -> Optional[dict]:
    print(f"    [FETCH] Đang lấy giá {symbol}...", flush=True)
    def _do():
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            past_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            df = Market().equity(symbol).ohlcv(
                start=past_7d, 
                end=today, 
                interval="1m", 
                source="ssi"
            )
            
            if df is not None and not df.empty:
                df['time'] = pd.to_datetime(df['time'])
                df['date_str'] = df['time'].dt.strftime('%Y-%m-%d')
                
                df_today = df[df['date_str'] == today]
                
                if not df_today.empty:
                    latest_candle = df_today.iloc[-1]
                    total_volume_today = int(df_today['volume'].sum())
                    
                    return {
                        "close": float(latest_candle["close"]), 
                        "open": float(latest_candle["open"]), 
                        "high": float(latest_candle["high"]), 
                        "low": float(latest_candle["low"]), 
                        "volume": total_volume_today
                    }
                else:
                    latest_candle = df.iloc[-1]
                    return {
                        "close": float(latest_candle["close"]), 
                        "open": float(latest_candle["open"]), 
                        "high": float(latest_candle["high"]), 
                        "low": float(latest_candle["low"]), 
                        "volume": int(latest_candle["volume"])
                    }
            return None
        except Exception as e:
            print(f"    ❌ [FETCH ERROR] Lỗi gọi Vnstock cho {symbol}: {e}", flush=True)
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        try: 
            data = ex.submit(_do).result(timeout=15)
        except concurrent.futures.TimeoutError:
            print(f"    ❌ [FETCH TIMEOUT] Quá 15s không lấy được giá {symbol}", flush=True)
            return None
        except Exception as e:
            print(f"    ❌ [FETCH THREAD ERROR] {symbol}: {e}", flush=True)
            return None

    if not data or data["close"] <= 0 or data["open"] <= 0: 
        return None
        
    if data["close"] >= 1000:
        for k in ["close", "open", "high", "low"]: 
            data[k] /= 1000.0
            
    if 0 < data["volume"] < 100000: 
        data["volume"] *= VOLUME_MULTIPLIER
        
    return data

def _fetch_history_ssi(symbol: str, start_date: str, end_date: str):
    try:
        df = Market().equity(symbol).ohlcv(start=start_date, end=end_date, source="ssi")
        return df if df is not None and not df.empty else None
    except Exception: return None

# ===================== CÀO BCTC TRỰC TIẾP TỪ VCI =====================
def _fetch_bctc_ssi(symbol: str):
    symbol = str(symbol).upper().strip()
    if not symbol or not re.fullmatch(r"[A-Z0-9]{2,10}", symbol):
        logger.warning("[BCTC VCI] Mã cổ phiếu không hợp lệ: %r", symbol)
        return None

    CORE_IDS = {
        "IS_NET_REVENUE", "IS_COST_OF_GOODS_SOLD", "IS_NET_PROFIT_AFTER_TAX",
        "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", "BS_TOTAL_ASSETS",
        "BS_SHORT_TERM_ASSETS", "BS_LONG_TERM_ASSETS", "BS_OWNERS_EQUITY", "BS_LIABILITIES"
    }

    base_url = "https://iq.vietcap.com.vn/api/iq-insight-service"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://iq.vietcap.com.vn/",
        "Origin": "https://iq.vietcap.com.vn",
    }
    session = requests.Session()
    session.headers.update(headers)

    def get_json(path, params=None, retries=3):
        url = f"{base_url}{path}"
        last_error = None
        for attempt in range(retries):
            try:
                response = session.get(url, params=params, timeout=(8, 25))
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, (dict, list)):
                    raise ValueError("VCI trả về JSON không hợp lệ")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt < retries - 1:
                    time.sleep((attempt + 1) * 2)
        raise RuntimeError(f"VCI API lỗi {path}: {last_error}") from last_error

    def unwrap(payload):
        if not isinstance(payload, dict):
            return payload
        data = payload.get("data", payload)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        return data

    def get_period(row):
        year = (
            row.get("yearReport") or row.get("year")
            or row.get("year_report")
        )
        quarter = (
            row.get("quarter") or row.get("quarterReport")
            or row.get("quarter_report")
        )
        length = row.get("lengthReport") or row.get("length_report")
        try:
            year = int(year)
            if quarter not in (None, ""):
                quarter = int(quarter)
            elif length not in (None, ""):
                quarter = int(length)
            else:
                return None
            if not 1 <= quarter <= 4:
                return None
            return f"{year}-Q{quarter}"
        except (TypeError, ValueError):
            return None

    def canonical_id(report_name, field_code, title):
        import unicodedata

        label = str(title or field_code or "").strip().lower()
        label = "".join(
            ch for ch in unicodedata.normalize("NFD", label)
            if unicodedata.category(ch) != "Mn"
        ).replace("đ", "d")
        label = re.sub(r"[^a-z0-9]+", " ", label).strip()
        field = str(field_code).upper() if field_code else ""

        if report_name == "income_statement":
            if "doanh thu thuan" in label or "net revenue" in label:
                return "IS_NET_REVENUE"
            if "gia von hang ban" in label or "cost of goods sold" in label:
                return "IS_COST_OF_GOODS_SOLD"
            
            if field == "ISA20" or "loi nhuan sau thue thu nhap doanh nghiep" in label:
                return "IS_NET_PROFIT_AFTER_TAX"
            
            if field in ["IS_NET_PROFIT_AFTER_TAX", "IS_PROFIT_AFTER_TAX"] or "loi nhuan sau thue" in label:
                if "co dong thieu so" not in label and "khong kiem soat" not in label:
                    return "IS_NET_PROFIT_AFTER_TAX"

        elif report_name == "cash_flow":
            if field == "CFA18":
                return "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES"
            
            if "luu chuyen tien te rong" in label or "net cash flows from operating activities" in label:
                return "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES"

        elif report_name == "balance_sheet":
            if "tong cong tai san" in label or "tong tai san" in label or "total assets" in label:
                return "BS_TOTAL_ASSETS"
            if "tai san ngan han" in label or "current assets" in label:
                return "BS_SHORT_TERM_ASSETS"
            if "tai san dai han" in label or "non current assets" in label:
                return "BS_LONG_TERM_ASSETS"
            
            if "von chu so huu" in label or "owners equity" in label or "owner equity" in label:
                return "BS_OWNERS_EQUITY"
            if "von va cac quy" in label:
                return "BS_OWNERS_EQUITY"
                
            if "no phai tra" in label or "total liabilities" in label:
                return "BS_LIABILITIES"

        return None
    try:
        logger.info("[BCTC VCI] Bắt đầu lấy BCTC cho %s", symbol)

        metadata = {}
        try:
            meta = unwrap(get_json(
                f"/v1/company/{symbol}/financial-statement/metrics"
            ))
            if isinstance(meta, dict):
                for section in (
                    "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"
                ):
                    metadata[section] = {
                        str(item.get("field")): (
                            item.get("titleVi")
                            or item.get("titleEn")
                            or item.get("field")
                        )
                        for item in (meta.get(section) or [])
                        if isinstance(item, dict) and item.get("field")
                    }
        except Exception as meta_error:
            logger.warning(
                "[BCTC VCI] Không lấy được metadata %s: %s",
                symbol, meta_error
            )

        report_map = {
            "balance_sheet": "BALANCE_SHEET",
            "income_statement": "INCOME_STATEMENT",
            "cash_flow": "CASH_FLOW",
        }
        reports = {}
        latest_period = None
        ignored_fields = {
            "organCode", "ticker", "createDate", "updateDate",
            "yearReport", "lengthReport", "publicDate", "quarter",
            "quarterReport", "year", "year_report", "length_report",
            "quarter_report",
        }

        for report_name, section in report_map.items():
            try:
                payload = unwrap(get_json(
                    f"/v1/company/{symbol}/financial-statement",
                    params={"section": section},
                ))
                if isinstance(payload, dict):
                    rows = (
                        payload.get("quarters")
                        or payload.get("quarter")
                        or []
                    )
                elif isinstance(payload, list):
                    rows = payload
                else:
                    rows = []

                records = []
                titles = metadata.get(section, {})

                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    period = get_period(row)
                    if not period:
                        continue

                    if latest_period is None or period > latest_period:
                        latest_period = period

                    for field_code, raw_value in row.items():
                        if field_code in ignored_fields or raw_value is None:
                            continue
                        if isinstance(raw_value, (dict, list, bool)):
                            continue
                        try:
                            value = float(raw_value)
                        except (TypeError, ValueError):
                            continue
                        if not np.isfinite(value):
                            continue

                        title = titles.get(str(field_code), str(field_code))
                        item_id = canonical_id(report_name, field_code, title)
                        
                        if not item_id or item_id not in CORE_IDS:
                            continue

                        if item_id == "IS_COST_OF_GOODS_SOLD" and value > 0:
                            value = -value

                        records.append({
                            "id": item_id,
                            "name": str(title),
                            "period": period,
                            "value": value,
                            "unit": "VND",
                            "source_field": str(field_code),
                        })

                if records:
                    unique_periods = sorted(list(set([item["period"] for item in records])), reverse=True)
                    keep_periods = set(unique_periods[:8])
                    records = [item for item in records if item["period"] in keep_periods]

                    reports[report_name] = records
                    logger.info(
                        "[BCTC VCI] %s | %s: %d chỉ tiêu, %d kỳ",
                        symbol, report_name, len(records),
                        len({item["period"] for item in records})
                    )
                else:
                    logger.warning(
                        "[BCTC VCI] %s | %s không có dữ liệu cốt lõi",
                        symbol, report_name
                    )

            except Exception as report_error:
                logger.warning(
                    "[BCTC VCI] %s | lỗi %s: %s",
                    symbol, report_name, report_error
                )

        if reports:
            reports["latest_period"] = latest_period or "Chưa xác định"
            reports["source"] = "VCI_DIRECT"
            logger.info(
                "[BCTC VCI] Hoàn tất %s | kỳ mới nhất: %s",
                symbol, reports["latest_period"]
            )
            return reports

        logger.error("[BCTC VCI] Không lấy được BCTC cho %s", symbol)
        return None

    except Exception as exc:
        log_exception(f"[BCTC VCI] Lỗi lấy BCTC {symbol}", exc)
        return None

    finally:
        session.close()


# ===================== CÀO RATIO TỪ VNDIRECT =====================
VNDIRECT_RATIOS = {
    "pe": "PRICE_TO_EARNINGS",
    "pb": "PRICE_TO_BOOK",
    "ps": "PRICE_TO_SALES",
    "eps": "EPS_TR",
    "roe": "ROAE_TR_AVG5Q",
    "roa": "ROAA_TR_AVG5Q",
    "dividend_yield": "DIVIDEND_YIELD"
}

def get_vndirect_ratio(symbol, ratio_code):
    url = "https://api-finfo.vndirect.com.vn/v4/ratios"
    params = {"q": f"code:{symbol}~ratioCode:{ratio_code}", "size": 1, "sort": "reportDate:desc"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                val = data[0].get("value")
                if val is not None:
                    return float(val)
    except Exception:
        pass
    return None

# ===================== CORE LOGIC CÀO =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    """
    Cào giá realtime. Từ 15h05 trở đi sẽ chốt sổ (ghi nến vào history_d1).
    Dời từ 15h00 -> 15h05 để tránh delay dữ liệu ATC.
    """
    global is_crawling, LAST_TELEGRAM_TIME, stop_crawl_requested
    if is_crawling: return
    is_crawling = True
    stop_crawl_requested = False 
    
    print(f"\n[CRAWL] 🚀 [{'CÀO BÙ' if is_retry else ('CÀO TAY' if is_manual else 'REAL-TIME')}] Bắt đầu xử lý {len(symbols)} mã...", flush=True)
    try:
        vn_now = datetime.utcnow() + timedelta(hours=7)
        real_today = vn_now.strftime("%Y-%m-%d")
        current_hour = vn_now.hour
        current_minute = vn_now.minute

        # === ĐIỀU KIỆN CHỐT SỔ: sau 15h05 (thay vì 15h00) ===
        is_closing_time = (current_hour > 15) or (current_hour == 15 and current_minute >= 5)

        alert_batch, pending_history_files = [], {}

        for chunk_idx in range(0, len(symbols), 5):
            if stop_crawl_requested:
                print("[CRAWL] 🛑 Đã nhận lệnh dừng khẩn cấp!", flush=True)
                break

            chunk = symbols[chunk_idx:chunk_idx+5]
            for sym in chunk:
                if stop_crawl_requested: break
                
                doc = collection.find_one({"symbol": sym})
                data = _fetch_one(sym)
                
                if data:
                    hist_doc = history_col.find_one({"symbol": sym})
                    prev_close = 0.0
                    avg_vol_20 = 0.0
                    latest_history_date = None 
                    
                    if hist_doc and hist_doc.get("history_d1"):
                        bars = hist_doc["history_d1"]
                        bars_sorted = sorted(bars, key=lambda x: x.get("time", ""))
                        
                        if bars_sorted and bars_sorted[-1]["time"] == real_today:
                            if len(bars_sorted) >= 2:
                                prev_close = float(bars_sorted[-2]["close"])
                                latest_history_date = bars_sorted[-2]["time"]
                        else:
                            prev_close = float(bars_sorted[-1]["close"])
                            latest_history_date = bars_sorted[-1]["time"]
                        
                        valid_vols = [b.get("volume", 0) for b in bars_sorted[-20:] if b.get("volume", 0) > 0]
                        if valid_vols: avg_vol_20 = sum(valid_vols) / len(valid_vols)

                    if prev_close <= 0 and doc:
                        prev_close = float(doc.get("close", 0))

                    vol_pct = round(data["volume"] / avg_vol_20 * 100, 2) if avg_vol_20 > 0 else 0
                    pct_change = round(((data["close"] - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0
                    
                    arrow, color = "—", "slate"
                    if pct_change > 0: arrow, color = "▲", "emerald"
                    elif pct_change < 0: arrow, color = "▼", "rose"

                    is_locked = doc.get("is_locked", True) if doc else True
                    if current_hour >= 9 and is_locked:
                        if data["close"] != prev_close:
                            is_locked = False
                            print(f"  🔓 [MỞ SỔ] {sym}: Giá biến động từ {prev_close} -> {data['close']}", flush=True)

                    # === TÍNH TOÁN OPEN, HIGH, LOW TÍCH LŨY ===
                    today_record = collection.find_one({"symbol": sym, "trading_date": real_today})
                    
                    if today_record and today_record.get("trading_date") == real_today:
                        final_open = today_record.get("open", data["open"])
                        final_high = max(today_record.get("high", data["high"]), data["high"])
                        final_low = min(today_record.get("low", data["low"]), data["low"])
                    else:
                        final_open = data["open"]
                        final_high = data["high"]
                        final_low = data["low"]
                    # ========================================================

                    collection.update_one({"symbol": sym}, {"$set": {
                        "close": data["close"], "prev_close": prev_close, 
                        "open": final_open,
                        "high": final_high,
                        "low": final_low,
                        "volume": data["volume"],
                        "volume_percent": vol_pct, "avg_volume_20": round(avg_vol_20, 2),
                        "price_change_percent": pct_change,
                        "price_change_available": prev_close > 0,
                        "price_arrow": arrow, "price_color": color,
                        "trading_date": real_today, 
                        "updated_at": _now_str(), "is_locked": is_locked, "is_incomplete": False
                    }}, upsert=True)
                    
                    print(f"  ✅ [OK] {sym}: Giá {data['close']:,} (O:{final_open} H:{final_high} L:{final_low}) | KL: {data['volume']:,} | Vol%: {vol_pct}%", flush=True)
                    
                    if prev_close > 0 and abs(pct_change) >= 3.0 and vol_pct >= 120.0:
                        alert_batch.append({"sym": sym, "pct_change": pct_change, "volume_percent": vol_pct, "trend": "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG"})
                        
                    # === CHỐT SỔ SAU 15H05 ===
                    if is_closing_time:
                        has_traded_today = (not is_locked) or (data.get("volume", 0) > 0)
                        
                        if has_traded_today:
                            d1_bars = hist_doc.get("history_d1", []) if hist_doc else []
                            d1_bars = [b for b in d1_bars if b.get("time") != real_today]
                            
                            d1_bars.append({
                                "time": real_today, 
                                "open": final_open, 
                                "high": final_high, 
                                "low": final_low, 
                                "close": data["close"], 
                                "volume": data["volume"]
                            })
                            d1_bars.sort(key=lambda x: x["time"])
                            
                            history_col.update_one({"symbol": sym}, {"$set": {"history_d1": d1_bars}}, upsert=True)
                            collection.update_one({"symbol": sym}, {"$set": {"is_locked": True, "chot_so_today": real_today}})
                            
                            if hist_doc is None: hist_doc = {"symbol": sym}
                            hist_doc["history_d1"] = d1_bars
                            pending_history_files[f"history/{sym}.json"] = hist_doc
                        else:
                            collection.update_one({"symbol": sym}, {"$set": {"is_locked": True, "chot_so_today": real_today}})
                else:
                    print(f"  ❌ [LỖI] {sym}: Không lấy được dữ liệu.", flush=True)
                    collection.update_one({"symbol": sym}, {"$set": {"is_incomplete": True, "last_error_at": _now_str()}}, upsert=True)
            time.sleep(1)

        if is_closing_time and pending_history_files:
            files_to_push = list(pending_history_files.keys())
            for i in range(0, len(files_to_push), 10):
                chunk_files = files_to_push[i:i+10]
                chunk_data = {k: pending_history_files[k] for k in chunk_files}
                commit_multiple_files_to_github(chunk_data, f"Cập nhật nến {real_today}")
                time.sleep(2)

        if alert_batch:
            now_ts = time.time()
            if (now_ts - LAST_TELEGRAM_TIME) >= 7200:
                alert_batch.sort(key=lambda x: x["volume_percent"], reverse=True)
                msg_lines = [f"🚨 <b>CẢNH BÁO DÒNG TIỀN ĐỘT BIẾN</b>"]
                msg_lines.append(f"⏰ Lúc: {_now_str()}")
                for item in alert_batch:
                    msg_lines.append(f"• <b>{item['sym']}</b>: {item['pct_change']:+.2f}% {item['trend']} | Vol: {item['volume_percent']:.0f}%")
                send_telegram("\n".join(msg_lines))
                print(f"[TELEGRAM] 🚀 Đã gửi cảnh báo {len(alert_batch)} mã.", flush=True)
                LAST_TELEGRAM_TIME = now_ts
            else:
                print(f"[TELEGRAM] ⏳ Có {len(alert_batch)} mã đột biến nhưng chưa đủ 2 tiếng, bỏ qua gửi tin.", flush=True)
    finally:
        is_crawling = False
        stop_crawl_requested = False

# ===================== GITHUB HELPER =====================
def get_github_file_data(file_path):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return None, None
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200: return json.loads(base64.b64decode(res.json()["content"]).decode("utf-8")), res.json()["sha"]
        return None, None
    except: return None, None

def commit_multiple_files_to_github(files_to_update: dict, commit_message: str, max_retries: int = 3):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]) or not files_to_update: return False
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    base_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}"

    for attempt in range(max_retries):
        try:
            ref_res = requests.get(f"{base_url}/git/ref/heads/{GITHUB_BRANCH}", headers=headers, timeout=10)
            if ref_res.status_code != 200: time.sleep(2); continue
            base_commit_sha = ref_res.json()["object"]["sha"]

            tree_items = []
            for path, content_dict in files_to_update.items():
                b64_content = base64.b64encode(json.dumps(content_dict, ensure_ascii=False, indent=4).encode("utf-8")).decode("utf-8")
                blob_res = requests.post(f"{base_url}/git/blobs", headers=headers, json={"content": b64_content, "encoding": "base64"}, timeout=10)
                if blob_res.status_code != 201: break
                tree_items.append({"path": path, "mode": "100644", "type": "blob", "sha": blob_res.json()["sha"]})
            else:
                tree_res = requests.post(f"{base_url}/git/trees", headers=headers, json={"base_tree": base_commit_sha, "tree": tree_items}, timeout=15)
                if tree_res.status_code != 201: time.sleep(2); continue
                commit_res = requests.post(f"{base_url}/git/commits", headers=headers, json={"message": commit_message, "tree": tree_res.json()["sha"], "parents": [base_commit_sha]}, timeout=10)
                if commit_res.status_code != 201: time.sleep(2); continue
                update_res = requests.patch(f"{base_url}/git/refs/heads/{GITHUB_BRANCH}", headers=headers, json={"sha": commit_res.json()["sha"], "force": True}, timeout=10)
                if update_res.status_code == 200: return True
            time.sleep(2)
        except Exception: time.sleep(2)
    return False

# ===================== CHẤM ĐIỂM =====================
FALLBACK_SECTOR_MAPPING = {
    "NGAN_HANG": ["ACB","BID","CTG","TCB","MBB","STB","VPB","HDB","VIB","TPB","SHB","SSB","LPB","MSB","OCB","EIB","VCB"],
    "CHUNG_KHOAN": ["SSI","VCI","HCM","VIX","MBS","FTS","BSI","CTS","ORS"],
    "BAT_DONG_SAN": ["VHM","VIC","VRE","NVL","PDR","DIG","NLG","KDH","CEO","DXG","TCH","HDG","KBC","IDC","SZC","BCM","PHR","SJS","VPI"],
    "CONG_NGHE_BAN_LE": ["FPT","MWG","PNJ","FRT","DGW"],
    "DIEN_NUOC_HA_TANG": ["POW","REE","PC1","NT2","GAS","GEG","BWE","TDM"],
    "NGUYEN_VAT_LIEU_CHU_KY": ["HPG","HSG","NKG","DGC","DCM","DPM","GVR","PHR","DHC"],
    "SAN_XUAT_TIEU_DUNG_PHONG_THU": ["VNM","MSN","SAB","VHC","ANV","BVH","PLX","PVD","PVS","BSR","GMD","HAH","PVT","HVN","APH","ASM","DBC","KDC","MSH","PAN","SBT","DRC","CSM","BMP","DHG","CTD","VCG","HHV","GEX"],
}
SECTOR_MAPPING = FALLBACK_SECTOR_MAPPING
SUB_SECTOR_MAPPING = {
    "INSURANCE": ["BVH"], "PETROLEUM": ["PLX", "PVD", "PVS", "BSR"], "MARITIME": ["GMD", "HAH", "PVT"],
    "AVIATION": ["VJC", "HVN"], "CONSTRUCTION": ["CTD", "VCG", "HHV", "GEX", "DRC", "CSM", "BMP"],
    "AGRI": ["VHC", "ANV", "DBC", "SBT", "ASM", "APH"],
}
SUB_SECTOR_CONFIG = {
    "NGAN_HANG": {"cap_pe": 14, "cap_pb": 1.8, "mos": 0.20, "primary_metric": "PB"},
    "CHUNG_KHOAN": {"cap_pe": 14, "cap_pb": 1.8, "mos": 0.25, "primary_metric": "PB"},
    "BAT_DONG_SAN": {"cap_pe": 12, "cap_pb": 2.0, "mos": 0.30, "primary_metric": "PB"},
    "CONG_NGHE_BAN_LE": {"cap_pe": 20, "cap_pb": 3.0, "mos": 0.20, "primary_metric": "PE"},
    "DIEN_NUOC_HA_TANG": {"cap_pe": 15, "cap_pb": 2.0, "mos": 0.15, "primary_metric": "PE"},
    "NGUYEN_VAT_LIEU_CHU_KY": {"cap_pe": 12, "cap_pb": 2.0, "mos": 0.30, "primary_metric": "PB"},
    "SAN_XUAT_TIEU_DUNG_PHONG_THU": {"cap_pe": 18, "cap_pb": 2.5, "mos": 0.25, "primary_metric": "PE"},
    "INSURANCE": {"cap_pe": 16, "cap_pb": 2.0, "mos": 0.25, "primary_metric": "PB"},
    "PETROLEUM": {"cap_pe": 12, "cap_pb": 1.5, "mos": 0.30, "primary_metric": "PE"},
    "MARITIME": {"cap_pe": 10, "cap_pb": 1.5, "mos": 0.30, "primary_metric": "PB"},
    "AVIATION": {"cap_pe": 15, "cap_pb": 3.0, "mos": 0.35, "primary_metric": "PB"},
    "CONSTRUCTION": {"cap_pe": 12, "cap_pb": 1.5, "mos": 0.30, "primary_metric": "PB"},
    "AGRI": {"cap_pe": 14, "cap_pb": 2.0, "mos": 0.25, "primary_metric": "PE"},
}

def get_sector(symbol):
    for sector, symbols in SECTOR_MAPPING.items():
        if symbol in symbols: return sector
    return "SAN_XUAT_TIEU_DUNG_PHONG_THU"

def get_sub_sector(symbol):
    for sub, symbols in SUB_SECTOR_MAPPING.items():
        if symbol in symbols: return sub
    return get_sector(symbol)

def normalize_stock_data(symbol, data):
    close_price = float(data.get("close", data.get("close_price", 0)) or 0)
    pe, pb, ps = float(data.get("pe", 0) or 0), float(data.get("pb", 0) or 0), float(data.get("ps", 0) or 0)
    roe, roa, eps = float(data.get("roe", 0) or 0), float(data.get("roa", 0) or 0), float(data.get("eps", 0) or 0)
    
    if eps >= 1000:
        eps = eps / 1000.0
    if close_price > 0 and eps > (close_price * 5): 
        eps = eps / 1000.0
        print(f"    [CẢNH BÁO] Đã sửa lỗi EPS cho {symbol} (chia 1000): {eps}", flush=True)

    bctc = data.get("bctc_history", [])
    profit_growth_yoy = float(bctc[0].get("profit_growth_yoy", 0)) if bctc else 0.0
    credit_growth_yoy = float(bctc[0].get("credit_growth_yoy", 0)) if bctc else 0.0
    bvps = (close_price / pb) if pb > 0 and close_price > 0 else 0
    if eps == 0 and pe > 0 and close_price > 0: eps = close_price / pe
    
    return {"close_price": close_price, "pe": pe, "pb": pb, "ps": ps, "roe": roe, "roa": roa, "eps": eps, "bvps": bvps,
            "dividend_yield": float(data.get("dividend_yield", 0)), "cfo_latest": float(data.get("cfo_latest", 0)),
            "net_profit_ttm": float(data.get("net_profit_ttm", 0)), "debt_to_equity": float(data.get("debt_to_equity", 0)),
            "gross_margin": float(data.get("gross_margin", 0)), "profit_growth_yoy": profit_growth_yoy, "credit_growth_yoy": credit_growth_yoy}

def compute_sector_medians(normalized_db):
    buckets = {}
    for symbol, nd in normalized_db.items():
        for key in (get_sub_sector(symbol), get_sector(symbol)):
            b = buckets.setdefault(key, {"pe": [], "pb": [], "ps": [], "roe": []})
            if nd["pe"] > 0: b["pe"].append(nd["pe"])
            if nd["pb"] > 0: b["pb"].append(nd["pb"])
            if nd["ps"] > 0: b["ps"].append(nd["ps"])
            if nd["roe"] != 0: b["roe"].append(nd["roe"])
    medians = {}
    for key, b in buckets.items():
        if len(b["pe"]) + len(b["pb"]) < 3: continue
        medians[key] = {"pe": float(np.median(b["pe"])) if b["pe"] else 0.0, "pb": float(np.median(b["pb"])) if b["pb"] else 0.0, "ps": float(np.median(b["ps"])) if b["ps"] else 0.0, "roe": float(np.median(b["roe"])) if b["roe"] else 0.0, "n": len(b["pe"]) + len(b["pb"])}
    return medians

def get_median_for_symbol(symbol, sector_medians):
    sub, sector = get_sub_sector(symbol), get_sector(symbol)
    if sub in sector_medians and sector_medians[sub]["n"] >= 3: return sector_medians[sub], sub
    if sector in sector_medians: return sector_medians[sector], sector
    return {"pe": 12.0, "pb": 2.0, "ps": 1.5, "roe": 12.0, "n": 0}, sector

def compute_fair_value_zone(symbol, nd, sector_median, config, group_label):
    eps, bvps, close = nd["eps"], nd["bvps"], nd["close_price"]
    cfo, npat = nd["cfo_latest"], nd["net_profit_ttm"]
    ps = nd.get("ps", 0)
    roe = nd.get("roe", 0)
    methods = {}
    med_pe, med_pb, med_ps, med_roe = sector_median.get("pe") or 12.0, sector_median.get("pb") or 2.0, sector_median.get("ps") or 0.0, sector_median.get("roe") or 0.0
    cap_pe, cap_pb, mos = config["cap_pe"], config["cap_pb"], config["mos"]
    is_financial = group_label in ("NGAN_HANG", "CHUNG_KHOAN", "INSURANCE")
    
    if eps > 0 and med_pe > 0: 
        roe_bonus = 1.15 if (roe > 0 and roe > med_roe) else 1.0
        methods["PE_method"] = round(eps * min(med_pe * roe_bonus, cap_pe), 0)
    if bvps > 0 and med_pb > 0: 
        methods["PB_method"] = round(bvps * min(med_pb, cap_pb), 0)
    if not is_financial and eps > 0 and bvps > 0: 
        methods["Graham"] = round((22.5 * eps * bvps) ** 0.5, 0)
    if not is_financial and npat > 0 and cfo > 0 and (cfo / npat) >= 0.8 and eps > 0 and med_pe > 0: 
        methods["CFO_boost"] = round(eps * min(med_pe, cap_pe) * 1.10, 0)
    
    if config.get("primary_metric") == "PB" and ps > 0 and med_ps > 0:
        sales_per_share = close / ps if ps > 0 else 0
        target_ps = min(med_ps, 2.5)
        if sales_per_share > 0:
            methods["PS_method"] = round(sales_per_share * target_ps, 0)
            
    valid_methods = {}
    for k, v in methods.items():
        if v > 0 and close > 0:
            if (v / close) >= 0.2 and (v / close) <= 3.0:
                valid_methods[k] = v
            else:
                print(f"    [CẢNH BÁO] Loại bỏ PP {k} của {symbol} do lệch quá lớn: {v} (Giá hiện tại: {close})", flush=True)

    if not valid_methods: 
        return {"fair_low": 0, "fair_high": 0, "graham": 0, "methods": {}, "mos_pct": round(mos * 100, 1), "rationale": "Không đủ DL.", "discount_to_fair_pct": 0, "verdict": "KHÔNG ĐỦ DỮ LIỆU"}
    
    weights = {"PE_method": 2, "PB_method": 1.5, "Graham": 2, "CFO_boost": 1.5, "PS_method": 1}
    weighted = []
    for k, v in valid_methods.items(): weighted.extend([v] * int(weights.get(k, 1) * 2))
    weighted.sort()
    n = len(weighted)
    fair_high = weighted[n // 2] if n % 2 else (weighted[n // 2 - 1] + weighted[n // 2]) / 2
    
    fair_low = fair_high * (1 - mos)
    discount_to_fair = ((fair_low - close) / close * 100) if close > 0 else 0.0
    verdict = "KHÔNG CÓ GIÁ" if close <= 0 else ("RẺ - TRONG VÙNG MUA" if close <= fair_low else ("HỢP LÝ - CHỜ ĐIỀU CHỈNH" if close <= fair_high else ("HƠI ĐẮT - KHÔNG MUA" if close <= fair_high * 1.2 else "ĐẮT - TRÁNH")))
    
    return {
        "fair_low": round(fair_low, 0), 
        "fair_high": round(fair_high, 0), 
        "graham": round(valid_methods.get("Graham", 0), 0), 
        "methods": {k: round(v, 0) for k, v in valid_methods.items()}, 
        "mos_pct": round(mos * 100, 1), 
        "rationale": f"[{group_label}]", 
        "discount_to_fair_pct": round(discount_to_fair, 1), 
        "verdict": verdict
    }

def compute_final_decision(fund_score, tech_score, val_score, total_score, verdict, cfo, npat, roe, debt_to_equity, discount_to_fair_pct=0.0, close_price=0.0, fair_high=0.0, fair_low=0.0):
    is_bank_or_sec = roe > 0 and debt_to_equity > 3.0
    if fund_score < 20: return ("❌ TRÁNH", "rose", f"PTCB yếu")
    if not is_bank_or_sec and cfo < 0 and npat > 0 and fund_score < 25: return ("⚠️ CẢNH BÁO LỢI NHUẬN ẢO", "amber", f"CFO âm.")
    if close_price > 0 and fair_high > 0 and close_price > fair_high * 1.2: return ("❌ TRÁNH", "rose", f"Giá vượt xa fair_high.")
    if close_price > 0 and fair_high > 0 and close_price > fair_high: return ("⚠️ CHỜ VỀ VÙNG MUA", "amber", f"Giá cao hơn fair_high.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 40 and tech_score >= 15 and val_score >= 24 and total_score >= 75 and discount_to_fair_pct >= 5): return ("🎯 MUA MẠNH", "emerald", "ĐỦ 4 YẾU TỐ.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 30 and tech_score >= 10 and val_score >= 18 and total_score >= 60): return ("✅ MUA TÍCH SẢN", "emerald", "ĐỦ 4 YẾU TỐ.")
    if (fund_score >= 30 and val_score >= 18 and verdict in ("HỢP LÝ - CHỜ ĐIỀU CHỈNH", "HƠI ĐẮT - KHÔNG MUA")): return ("⚠️ CHỜ VỀ VÙNG MUA", "amber", "DN tốt nhưng giá chưa rẻ.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 30 and val_score >= 18 and tech_score < 10): return ("⏳ CHỜ XÁC NHẬN KỸ THUẬT", "amber", "Chờ tín hiệu.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 30 and val_score < 18): return ("⏳ CHỜ ĐỊNH GIÁ TỐT HƠN", "amber", "Định giá chưa hấp dẫn.")
    return ("👀 THEO DÕI", "slate", "Chưa đủ ĐK.")

def evaluate_stock_from_db(symbol, nd, sector_median, group_label, config):
    sector = get_sector(symbol)
    pe, pb, ps, roe, roa = nd["pe"], nd["pb"], nd["ps"], nd["roe"], nd["roa"]
    close_price, cfo_latest, net_profit_ttm = nd["close_price"], nd["cfo_latest"], nd["net_profit_ttm"]
    debt_to_equity, profit_growth_yoy, credit_growth_yoy = nd["debt_to_equity"], nd["profit_growth_yoy"], nd["credit_growth_yoy"]
    
    data_warnings = []
    if close_price <= 0: data_warnings.append("Thiếu Giá")
    if pe <= 0 and sector not in ["NGAN_HANG", "CHUNG_KHOAN"]: data_warnings.append("Thiếu P/E")
    if pb <= 0: data_warnings.append("Thiếu P/B")
    if roe == 0: data_warnings.append("Thiếu ROE")
    is_data_incomplete = len(data_warnings) > 0

    fund_score = 0
    fund_notes = []
    
    if sector == "NGAN_HANG":
        if credit_growth_yoy >= 12: fund_score += 18; fund_notes.append("Tín dụng mạnh")
        elif credit_growth_yoy >= 8: fund_score += 10
        if roe >= 18: fund_score += 17; fund_notes.append("ROE >18%")
        elif roe >= 14: fund_score += 10
    elif sector == "CHUNG_KHOAN":
        if profit_growth_yoy >= 20: fund_score += 20; fund_notes.append("LN tăng mạnh")
        elif profit_growth_yoy >= 10: fund_score += 10
        if roe >= 15: fund_score += 15; fund_notes.append("ROE cao")
        elif roe >= 10: fund_score += 8
    elif sector == "BAT_DONG_SAN":
        if profit_growth_yoy >= 10: fund_score += 15
        if roe >= 12: fund_score += 20; fund_notes.append("ROE chuẩn")
        elif roe > 0: fund_score += 10
    elif sector == "CONG_NGHE_BAN_LE":
        if profit_growth_yoy >= 20: fund_score += 18; fund_notes.append("Tăng trưởng lõi")
        elif profit_growth_yoy >= 10: fund_score += 10
        if roe >= 18: fund_score += 17
        elif roe >= 12: fund_score += 10
    elif sector == "DIEN_NUOC_HA_TANG":
        if nd["dividend_yield"] >= 6.0: fund_score += 20; fund_notes.append("Cổ tức cao")
        elif nd["dividend_yield"] > 0: fund_score += 10
        if roe >= 10: fund_score += 15
        else: fund_score += 8
    elif sector == "NGUYEN_VAT_LIEU_CHU_KY":
        if profit_growth_yoy >= 15: fund_score += 18
        elif profit_growth_yoy >= 0: fund_score += 10
        else: fund_score -= 15; fund_notes.append("Suy thoái chu kỳ")
        if roe >= 12: fund_score += 17
        else: fund_score += 8
    else:
        if profit_growth_yoy >= 10: fund_score += 15
        elif profit_growth_yoy >= 0: fund_score += 8
        else: fund_score -= 10
        if roe >= 14: fund_score += 20; fund_notes.append("ROE cao")
        elif roe >= 10: fund_score += 10
        
    if roa >= 5.0: fund_score += 5
    elif roa >= 2.5: fund_score += 3

    if sector not in ["NGAN_HANG", "CHUNG_KHOAN"]:
        if cfo_latest > 0 and (net_profit_ttm <= 0 or cfo_latest >= net_profit_ttm * 0.8): fund_score += 5
        elif cfo_latest < 0: fund_score -= 5; fund_notes.append("CFO âm")
        if 0 < debt_to_equity <= 1.0: fund_score += 5
        elif debt_to_equity > 2.5: fund_score -= 5; fund_notes.append("D/E cao")
    else:
        fund_score += 5
    fund_score = int(np.clip(fund_score, 0, 50))

    val_score = 0
    if sector == "BAT_DONG_SAN":
        if (0 < pb < 1.5) or (0 < ps < 2.0): val_score = 30
        elif (0 < pb < 2.2) or (0 < ps < 3.5): val_score = 18
        else: val_score = 8
    elif sector == "NGUYEN_VAT_LIEU_CHU_KY":
        if (0 < ps < 1.2) or (0 < pe < 12): val_score = 30
        elif (0 < ps < 2.0) or (0 < pe < 18): val_score = 18
        else: val_score = 8
    elif sector == "CONG_NGHE_BAN_LE":
        if 0 < pe < 18 or (profit_growth_yoy > 20 and pe < 25): val_score = 30
        elif 0 < pe < 25: val_score = 18
        else: val_score = 8
    elif sector in ["NGAN_HANG", "CHUNG_KHOAN"]:
        if 0 < pb < 1.3 or (roe >= 16 and pb < 1.8): val_score = 30
        elif 0 < pb < 2.0: val_score = 18
        else: val_score = 8
    else:
        if 0 < pe < 15 or nd["dividend_yield"] >= 5.0: val_score = 30
        elif 0 < pe < 20 or nd["dividend_yield"] >= 3.0: val_score = 18
        else: val_score = 8
    val_score = int(np.clip(val_score, 0, 30))

    tech_score_raw, tech_status, pattern = 50, "Tích lũy", "Tích lũy"
    if ProfessionalTechnicalAnalyzer:
        hist_data = history_col.find_one({"symbol": symbol})
        if hist_data and hist_data.get("history_d1"):
            try:
                analyzer = ProfessionalTechnicalAnalyzer(hist_data["history_d1"]) 
                ptkt_res = analyzer.analyze()
                tech_score_raw = ptkt_res.get("technical_score", 50)
                tech_status = ptkt_res.get("technical_status", "Tích lũy")
                pattern = ptkt_res.get("pattern", "Tích lũy")
            except Exception as e: pass

    tech_score = int(np.clip(int(round((tech_score_raw / 100.0) * 20)), 0, 20))
    total_score = fund_score + val_score + tech_score
    potential_zone = "VÙNG TÍCH SẢN TỐI ƯU (A+)" if total_score >= 75 else ("VÙNG MUA AN TOÀN (B)" if total_score >= 60 else "THEO DÕI THÊM")
    
    fv = compute_fair_value_zone(symbol, nd, sector_median, config, group_label)
    decision, color, reason = compute_final_decision(fund_score, tech_score, val_score, total_score, fv["verdict"], cfo_latest, net_profit_ttm, roe, debt_to_equity, fv["discount_to_fair_pct"], close_price, fv["fair_high"], fv["fair_low"])

    sim_data = get_historical_return_rate_with_details(symbol)
    
    meta = VN100_METADATA.get(symbol, {})

    ai_data = {
        "bctc": "", "catalyst": "", "risks": "", "outlook": "", 
        "recommendation": "", "price_zone": "", "macro_view": "", 
        "ai_updated_at": "Chưa phân tích", "ai_source": ""
    }
    
    stock_doc = collection.find_one({"symbol": symbol}, {
        "ai_bctc": 1, "ai_catalyst": 1, "ai_risks": 1, "ai_macro_view": 1, 
        "ai_updated_at": 1, "ai_source": 1
    })
    
    if stock_doc and stock_doc.get("ai_bctc"):
        ai_data["bctc"] = stock_doc.get("ai_bctc", "")
        ai_data["catalyst"] = stock_doc.get("ai_catalyst", "")
        ai_data["risks"] = stock_doc.get("ai_risks", "")
        ai_data["macro_view"] = stock_doc.get("ai_macro_view", "")
        ai_data["ai_updated_at"] = stock_doc.get("ai_updated_at", "Chưa phân tích")
        ai_data["ai_source"] = stock_doc.get("ai_source", "AI")
        
        metrics_for_template = {
            **nd,
            "fair_value_low": fv["fair_low"],
            "fair_value_high": fv["fair_high"],
            "valuation_verdict": fv["verdict"],
            "discount_to_fair_pct": fv["discount_to_fair_pct"],
            "margin_of_safety_pct": fv["mos_pct"]
        }
        
        temp_template = build_fallback_narrative(symbol, metrics_for_template, {
            "fundamental_score": fund_score, 
            "valuation_score": val_score, 
            "total_score": total_score,
            "profit_growth_yoy": profit_growth_yoy, 
            "final_decision": decision
        }, sector)
        
        ai_data["outlook"] = temp_template.get("outlook", "")
        ai_data["recommendation"] = temp_template.get("recommendation", "")
        ai_data["price_zone"] = temp_template.get("price_zone", "")

    return {
        "symbol": symbol, "sector": sector, "sub_sector": group_label, "close_price": close_price, "pe": pe, "pb": pb, "ps": ps,
        "roe": roe, "roa": roa, "eps": round(nd["eps"], 0), "bvps": round(nd["bvps"], 0), "dividend_yield": nd["dividend_yield"], "profit_growth_yoy": profit_growth_yoy,
        "cfo_latest": cfo_latest, "net_profit_ttm": net_profit_ttm, "debt_to_equity": debt_to_equity, "gross_margin": nd["gross_margin"],
        "fundamental_score": fund_score, "valuation_score": val_score, "technical_score": tech_score, "total_score": total_score,
        "potential_zone": potential_zone, "pattern": pattern, "technical_status": tech_status,
        "fundamental_notes": " | ".join(fund_notes), "fair_value_high": fv["fair_high"], "fair_value_low": fv["fair_low"], "graham_number": fv["graham"],
        "margin_of_safety_pct": fv["mos_pct"], "fair_value_methods": fv["methods"], "fair_value_rationale": fv["rationale"],
        "discount_to_fair_pct": fv["discount_to_fair_pct"], "valuation_verdict": fv["verdict"], "final_decision": decision,
        "final_decision_color": color, "final_decision_reason": reason,
        
        "sim_annual_rate": sim_data["annual_rate_percent"] if sim_data else 15.0,
        "sim_start_quarter": sim_data["start_quarter"] if sim_data else "",
        "sim_start_price": sim_data["start_price_mean"] if sim_data else 0,
        "sim_end_quarter": sim_data["end_quarter"] if sim_data else "",
        "sim_end_price": sim_data["end_price_mean"] if sim_data else 0,
        
        "company_name": meta.get("company_name", ""),
        "market_cap": meta.get("market_cap", 0),
        "total_shares": meta.get("total_shares", 0),
        
        "data_warnings": data_warnings,
        "is_data_incomplete": is_data_incomplete,
        
        "bctc_evaluation": ai_data["bctc"],
        "outlook": ai_data["outlook"],
        "ai_recommendation": ai_data["recommendation"],
        "ai_price_zone": ai_data["price_zone"],
        "macro_view": ai_data["macro_view"],
        "ai_updated_at": ai_data["ai_updated_at"],
        "ai_source": ai_data["ai_source"]
    }

def aggregate_candles(data, interval):
    if not data or interval == 'D1': return data
    groups = {}
    for bar in data:
        date = datetime.strptime(bar['time'], '%Y-%m-%d')
        key = (date - timedelta(days=date.weekday())).strftime('%Y-%m-%d') if interval == 'W1' else date.strftime('%Y-%m-01')
        if key not in groups: groups[key] = {"time": key, "open": bar['open'], "high": bar['high'], "low": bar['low'], "close": bar['close'], "volume": bar['volume']}
        else:
            groups[key]['high'] = max(groups[key]['high'], bar['high']); groups[key]['low'] = min(groups[key]['low'], bar['low']); groups[key]['close'] = bar['close']; groups[key]['volume'] += bar['volume']
    return sorted(list(groups.values()), key=lambda x: x['time'])

# ===================== AI HELPERS =====================
SECTOR_NAMES = {"NGAN_HANG": "Ngân hàng", "CHUNG_KHOAN": "Chứng khoán", "BAT_DONG_SAN": "Bất động sản", "CONG_NGHE_BAN_LE": "Công nghệ & Bán lẻ", "DIEN_NUOC_HA_TANG": "Điện, Nước & Hạ tầng", "NGUYEN_VAT_LIEU_CHU_KY": "Vật liệu & Chu kỳ", "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Sản xuất & Tiêu dùng"}

DECISION_TO_RECOMMENDATION = {
    "🎯 MUA MẠNH": ("MUA MẠNH", "Tỷ trọng 20-30% — Giải ngân toàn bộ hoặc theo 2-3 đợt"),
    "✅ MUA TÍCH SẢN": ("MUA TÍCH SẢN", "Tỷ trọng 10-15% — Giải ngân từng phần (DCA)"),
    "⚠️ CHỜ VỀ VÙNG MUA": ("CHỜ VỀ VÙNG MUA", "Tỷ trọng 0% — Chờ giá về fair_low mới mua"),
    "⏳ CHỜ XÁC NHẬN KỸ THUẬT": ("CHỜ XÁC NHẬN KỸ THUẬT", "Tỷ trọng 0% — Chờ breakout có volume xác nhận"),
    "⏳ CHỜ ĐỊNH GIÁ TỐT HƠN": ("CHỜ ĐỊNH GIÁ TỐT HƠN", "Tỷ trọng 0% — Định giá chưa hấp dẫn"),
    "❌ TRÁNH": ("TRÁNH", "Tỷ trọng 0% — Không mua"),
    "⚠️ CẢNH BÁO LỢI NHUẬN ẢO": ("CẢNH BÁO", "Tỷ trọng 0% — Lợi nhuận không có tiền mặt thật"),
    "👀 THEO DÕI": ("NẮM GIỮ / THEO DÕI", "Tỷ trọng 0% — Chưa đủ điều kiện mua"),
}

def get_sector_guidance(sector):
    guidance = {"NGAN_HANG": "D/E cao là bản chất. Trọng tâm: ROE, ROA, tín dụng.", "CHUNG_KHOAN": "Phụ thuộc thanh khoản. Trọng tâm: ROE, LNST YoY.", "BAT_DONG_SAN": "Ghi nhận theo dự án. Trọng tâm: P/B, ROE, dòng tiền trả trước.", "CONG_NGHE_BAN_LE": "Trọng tâm: ROE, biên gộp, LNST.", "DIEN_NUOC_HA_TANG": "CFO ổn định. Trọng tâm: ROE, cổ tức, CFO.", "NGUYEN_VAT_LIEU_CHU_KY": "Bẫy P/E thấp. Trọng tâm: P/B, ROE, biên gộp.", "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Trọng tâm: ROE, biên gộp, CFO/LNST."}
    return guidance.get(sector, guidance["SAN_XUAT_TIEU_DUNG_PHONG_THU"])

def build_verified_facts(symbol, sm, si, sector):
    lines = [
        f"MÃ {symbol} — NGÀNH {sector}",
        f"QUYẾT ĐỊNH: {si.get('final_decision')}",
        f"GIÁ: {sm.get('close',0):,.0f} | P/E {sm.get('pe',0):.2f} | P/B {sm.get('pb',0):.2f}",
        f"VÙNG MUA: Fair value {sm.get('fair_value_high',0):,.0f} | Vùng mua {sm.get('fair_value_low',0):,.0f} | Verdict: {sm.get('valuation_verdict')}",
        f"SINH LỜI: ROE {sm.get('roe',0)}% | ROA {sm.get('roa',0)}% | CFO {sm.get('cfo_latest',0):,.0f} | LNST {sm.get('net_profit_ttm',0):,.0f}",
        f"ĐIỂM: Tổng {si.get('total_score')}/100"
    ]
    return "\n".join(lines)

def build_fallback_narrative(symbol, stock_metrics, screener_item, sector):
    close = float(stock_metrics.get("close", stock_metrics.get("close_price", 0)) or 0)
    pe = float(stock_metrics.get("pe", 0) or 0)
    pb = float(stock_metrics.get("pb", 0) or 0)
    roe = float(stock_metrics.get("roe", 0) or 0)
    roa = float(stock_metrics.get("roa", 0) or 0)
    eps = float(stock_metrics.get("eps", 0) or 0)
    cfo = float(stock_metrics.get("cfo_latest", 0) or 0)
    npat = float(stock_metrics.get("net_profit_ttm", 0) or 0)
    de = float(stock_metrics.get("debt_to_equity", 0) or 0)
    gm = float(stock_metrics.get("gross_margin", 0) or 0)
    div_yield = float(stock_metrics.get("dividend_yield", 0) or 0)

    fair_low = float(stock_metrics.get("fair_value_low", 0) or 0)
    fair_high = float(stock_metrics.get("fair_value_high", 0) or 0)
    discount_pct = float(stock_metrics.get("discount_to_fair_pct", 0) or 0)
    verdict = stock_metrics.get("valuation_verdict", "CHƯA TÍNH")
    mos_pct = float(stock_metrics.get("margin_of_safety_pct", 0) or 0)
    
    fund_score = screener_item.get("fundamental_score", 0)
    val_score = screener_item.get("valuation_score", 0)
    tech_score = screener_item.get("technical_score", 0)
    total_score = screener_item.get("total_score", 0)
    growth_yoy = float(screener_item.get("profit_growth_yoy", 0) or 0)
    final_decision = screener_item.get("final_decision", "CHƯA XÁC ĐỊNH")

    sector_name = SECTOR_NAMES.get(sector, sector)
    is_bank_or_sec = sector in ("NGAN_HANG", "CHUNG_KHOAN")

    bctc_parts = [f"Chỉ số sinh lời: ROE đạt {roe:.1f}%, ROA {roa:.1f}%."]
    if is_bank_or_sec:
        bctc_parts.append(f"P/E {pe:.2f}x và P/B {pb:.2f}x phản ánh mức định giá của định chế tài chính. D/E {de:.2f} là bản chất ngân hàng.")
    else:
        bctc_parts.append(f"Định giá P/E {pe:.2f}x và P/B {pb:.2f}x.")
        if gm > 0: bctc_parts.append(f"Biên lợi nhuận gộp {gm:.1f}%.")

    if npat > 0 and cfo != 0:
        cfo_ratio = cfo / npat
        if cfo_ratio >= 1.0:
            bctc_parts.append(f"Chất lượng lợi nhuận tốt với CFO {cfo:,.0f} tỷ ≥ LNST {npat:,.0f} tỷ.")
        elif cfo_ratio >= 0.5:
            bctc_parts.append(f"CFO {cfo:,.0f} tỷ so với LNST {npat:,.0f} tỷ — chấp nhận được.")
        else:
            bctc_parts.append(f"⚠️ Cảnh báo: CFO/LNST chỉ {cfo_ratio:.2f}x.")

    bctc_parts.append(f"EPS đạt {eps:,.0f} VNĐ.")
    if div_yield > 0: bctc_parts.append(f"Cổ tức {div_yield}%/năm.")
    bctc = " ".join(bctc_parts)

    moat_map = {
        "NGAN_HANG": "tệp khách hàng bán lẻ trung thành và chi phí vốn thấp",
        "CHUNG_KHOAN": "thị phần môi giới ổn định và quan hệ với nhà đầu tư tổ chức",
        "BAT_DONG_SAN": "quỹ đất sạch tại vị trí đắc địa và thương hiệu uy tín",
        "CONG_NGHE_BAN_LE": "thương hiệu mạnh và chuỗi cung ứng tối ưu",
        "DIEN_NUOC_HA_TANG": "vị thế độc quyền tự nhiên và dòng tiền ổn định",
        "NGUYEN_VAT_LIEU_CHU_KY": "quy mô sản xuất lớn và chi phí biên thấp",
        "SAN_XUAT_TIEU_DUNG_PHONG_THU": "thương hiệu quốc dân và mạng lưới phân phối rộng",
    }
    moat = moat_map.get(sector, "lợi thế cạnh tranh dài hạn")
    catalyst = f"Lợi thế cạnh tranh cốt lõi nằm ở {moat}. LNST tăng {growth_yoy:.1f}% YoY."

    risk_map = {
        "NGAN_HANG": f"Rủi ro nợ xấu và áp lực thu hẹp NIM. D/E hiện ở mức {de:.2f}.",
        "CHUNG_KHOAN": f"Rủi ro thanh khoản thị trường và danh mục tự doanh.",
        "BAT_DONG_SAN": "Rủi ro pháp lý dự án và đòn bẩy nợ cao.",
        "CONG_NGHE_BAN_LE": "Rủi ro cạnh tranh và biên lợi nhuận bị thu hẹp.",
        "DIEN_NUOC_HA_TANG": "Rủi ro chính sách giá và chi phí vốn tăng.",
        "NGUYEN_VAT_LIEU_CHU_KY": f"Rủi ro giá hàng hóa đảo chiều làm biên gộp {gm:.1f}% co lại.",
        "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Rủi ro cạnh tranh từ hàng nhập khẩu và sức mua yếu.",
    }
    risks = risk_map.get(sector, f"Rủi ro chung của ngành {sector_name}.")

    if verdict == "RẺ - TRONG VÙNG MUA":
        outlook = f"Giá {close:,.0f} VNĐ nằm dưới vùng mua an toàn {fair_low:,.0f} VNĐ, biên an toàn {discount_pct:.1f}% (MoS {mos_pct:.0f}%). Đây là vùng định giá hấp dẫn cho tích sản dài hạn. Quyết định: {final_decision}."
    elif verdict == "HỢP LÝ - CHỜ ĐIỀU CHỈNH":
        outlook = f"Giá {close:,.0f} VNĐ giữa vùng mua {fair_low:,.0f} và fair value {fair_high:,.0f}. Chờ điều chỉnh về vùng mua. Quyết định: {final_decision}."
    elif verdict in ("HƠI ĐẮT - KHÔNG MUA", "ĐẮT - TRÁNH"):
        outlook = f"Giá {close:,.0f} VNĐ đã vượt fair value {fair_high:,.0f}. Định giá chưa hấp dẫn. Quyết định: {final_decision}."
    else:
        outlook = f"Chưa đủ dữ liệu định giá. Verdict: {verdict}."

    rec_text, rec_weight = DECISION_TO_RECOMMENDATION.get(final_decision, ("NẮM GIỮ / THEO DÕI", "Tỷ trọng 0%"))
    recommendation = f"{rec_text} — {rec_weight}"

    price_zone = f"Vùng mua: {fair_low:,.0f} — {fair_high:,.0f} | Giá: {close:,.0f} | Verdict: {verdict}"

    macro_map = {
        "NGAN_HANG": "Chu kỳ tín dụng phục hồi khi lãi suất thấp. Rủi ro nợ xấu BĐS còn hiện hữu.",
        "CHUNG_KHOAN": "Thanh khoản thị trường quyết định doanh thu ngành. Chính sách nâng hạng tạo catalyst.",
        "BAT_DONG_SAN": "Pháp lý BĐS đang dần tháo gỡ. Chu kỳ phục hồi phụ thuộc niềm tin người mua.",
        "CONG_NGHE_BAN_LE": "Sức mua tiêu dùng phục hồi. Xu hướng chuyển đổi số tạo dư địa tăng trưởng.",
        "DIEN_NUOC_HA_TANG": "Nhu cầu điện tăng theo GDP. Chính sách năng lượng tái tạo tạo cơ hội.",
        "NGUYEN_VAT_LIEU_CHU_KY": "Chu kỳ giá hàng hóa phụ thuộc kinh tế Trung Quốc và chính sách bảo hộ.",
        "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Tiêu dùng nội địa phục hồi chậm. Cơ hội từ xuất khẩu nếu tỷ giá thuận lợi.",
    }
    macro_view = macro_map.get(sector, f"Ngành {sector_name} chịu ảnh hưởng chung từ vĩ mô.")

    return {
        "bctc": bctc,
        "catalyst": catalyst,
        "risks": risks,
        "valuation_verdict": verdict,
        "outlook": outlook,
        "recommendation": recommendation,
        "price_zone": price_zone,
        "macro_view": macro_view,
        "_source": "TEMPLATE",
    }

def _call_ai_for_symbol(symbol, sm, si, sector):
    try:
        verified_facts = build_verified_facts(symbol, sm, si, sector)
        sector_guidance = get_sector_guidance(sector)
        
        final_decision = si.get("final_decision", "CHƯA XÁC ĐỊNH")
        rec_text, rec_weight = DECISION_TO_RECOMMENDATION.get(final_decision, ("NẮM GIỮ / THEO DÕI", "Tỷ trọng 0%"))
        
        fair_low = float(sm.get("fair_value_low", 0) or 0)
        fair_high = float(sm.get("fair_value_high", 0) or 0)
        verdict = sm.get("valuation_verdict", "CHƯA TÍNH")
        discount_pct = float(sm.get("discount_to_fair_pct", 0) or 0)
        close_price = float(sm.get("close", sm.get("close_price", 0)) or 0)
        
        if close_price > 0 and close_price < 1000:
            close_price *= 1000
        if fair_low > 0 and fair_low < 1000:
            fair_low *= 1000
        if fair_high > 0 and fair_high < 1000:
            fair_high *= 1000

        prompt = f"""Bạn là Chuyên gia Phân tích Đầu tư Giá trị (Value Investing) với 20 năm kinh nghiệm tại các quỹ lớn ở Việt Nam.

NHIỆM VỤ: Diễn giải dữ liệu đã tính sẵn cho mã {symbol}. KHÔNG được bịa số liệu. Mọi con số phải TRÍCH DẪN TRỰC TIẾP từ khối DỮ KIỆN bên dưới.

--- DỮ KIỆN ĐÃ KIỂM CHỨNG ---
{verified_facts}

--- HƯỚNG DẪN THEO NGÀNH ---
{sector_guidance}

--- QUY TẮC BẮT BUỘC ---
1. MỌI nhận định phải có SỐ CỤ THỂ từ dữ kiện.
2. VỀ KHUYẾN NGHỊ: Hệ thống đã tính → "{final_decision}". BẠN PHẢI trả về recommendation = "{rec_text}" và weight = "{rec_weight}". 
TUYỆT ĐỐI KHÔNG ĐƯỢC TỰ ĐỔI SANG CHỮ "TRÁNH" HAY BẤT KỲ TỪ NÀO KHÁC.
3. VỀ "ĐỊNH GIÁ & HÀNH ĐỘNG GIÁ" (outlook): Phải làm nổi bật "Biên an toàn" (Margin of Safety). Nếu giá đang trong vùng mua, phải kết luận rõ đây là cơ hội. Nếu giá đắt, phải nói rõ rủi ro.
4. VỀ "VĨ MÔ & SÓNG NGÀNH" (macro_view): Phân tích dựa trên chu kỳ giá hàng hóa (đối với ngành Vật liệu/Phân bón/Hóa chất/Thép) hoặc chính sách vĩ mô (đối với Ngân hàng/BĐS). KHÔNG nói chung chung về VN-Index.
5. VỀ ĐƠN VỊ: Giá cổ phiếu phải ghi rõ đơn vị là "VNĐ" (Ví dụ: 24.000 VNĐ, chứ không phải 24 VNĐ).

--- ĐỊNH DẠNG: JSON THUẦN TÚY (KHÔNG ```json) ---
{{
  "bctc": "6-8 câu phân tích BCTC, trích số: ROE={sm.get('roe',0)}%, ROA={sm.get('roa',0)}%, P/E={sm.get('pe',0)}, P/B={sm.get('pb',0)}, CFO/LNST, D/E={sm.get('debt_to_equity',0)}. Kết luận lợi nhuận thật/ảo.",
  "catalyst": "3-4 câu. Economic Moat cụ thể + động lực tăng trưởng 2-3 năm.",
  "risks": "3 câu. Rủi ro trọng yếu nhất phù hợp ngành.",
  "valuation_verdict": "Copy nguyên văn: '{verdict}'",
  "outlook": "4-5 câu. So sánh giá {close_price:,.0f} VNĐ với vùng mua {fair_low:,.0f} VNĐ - {fair_high:,.0f} VNĐ. Giải thích quyết định '{final_decision}'. Nhấn mạnh biên an toàn {discount_pct:.1f}%.",
  "recommendation": "BẮT BUỘC: '{rec_text}' — {rec_weight}",
  "macro_view": "4-5 câu. Phân tích chu kỳ hàng hóa hoặc vĩ mô tác động trực tiếp đến doanh nghiệp."
}}"""

        headers = {"Authorization": f"Bearer {ALIBABA_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": AI_MODEL_NAME,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": AI_TEMPERATURE,
        }

        for attempt in range(3):
            try:
                res = requests.post(AI_API_URL, headers=headers, json=payload, timeout=60)
                
                if res.status_code == 200:
                    content = res.json()["choices"][0]["message"]["content"].strip()
                    content = re.sub(r"^```json\s*", "", content)
                    content = re.sub(r"^```\s*", "", content)
                    content = re.sub(r"\s*```$", "", content)
                    
                    parsed = json.loads(content)
                    
                    if fair_low > 0 and fair_high > 0:
                        parsed["price_zone"] = (
                            f"Vùng mua: {fair_low:,.0f} VNĐ — {fair_high:,.0f} VNĐ | "
                            f"Giá hiện tại: {close_price:,.0f} VNĐ | Verdict: {verdict}"
                        )
                    else:
                        parsed["price_zone"] = "Chưa đủ dữ liệu định giá (Cần cào lại BCTC/Chỉ số)"

                    ai_rec = parsed.get("recommendation", "").upper()
                    expected_rec = rec_text.upper()
                    if expected_rec not in ai_rec:
                        print(f"  ⚠️ [{symbol}] AI tự đổi khuyến nghị → Ghi đè: '{rec_text}'", flush=True)
                        parsed["recommendation"] = f"{rec_text} — {rec_weight}"

                    parsed["_source"] = "AI"
                    return parsed
                else:
                    print(f"  ❌ [{symbol}] AI trả về HTTP {res.status_code} (Lần {attempt + 1}/3)", flush=True)
                    time.sleep(3)
                    
            except requests.exceptions.Timeout:
                print(f"  ⏳ [{symbol}] AI Timeout (Lần {attempt + 1}/3), thử lại...", flush=True)
                time.sleep(3)
            except Exception as e:
                print(f"  ❌ [{symbol}] Lỗi AI (Lần {attempt + 1}/3): {e}", flush=True)
                time.sleep(3)

        print(f"  ⚠️ [{symbol}] AI lỗi hoàn toàn (sau 3 lần thử). Bỏ qua mã này.", flush=True)
        return None
        
    except Exception as e:
        print(f"  ❌ Lỗi nghiêm trọng ở _call_ai_for_symbol cho {symbol}: {e}", flush=True)
        return None

# ===================== BACKGROUND WORKERS =====================
def auto_crawler_worker():
    """
    Auto crawler với logic:
    - Cào 9h-15h (nghỉ trưa 11h30-13h)
    - Vẫn cào 15h00-15h05 để bắt giá ATC muộn
    - Chốt sổ sau 15h05
    - Tự phát hiện ngày lễ
    """
    print("⏰ [AUTO] Đã bật chế độ cào Real-time (Tự động phát hiện ngày nghỉ)!", flush=True)
    while True:
        try:
            global is_auto_crawl_paused
            if is_auto_crawl_paused:
                time.sleep(10)
                continue

            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            current_hour = vn_now.hour
            current_minute = vn_now.minute
            weekday = vn_now.weekday()

            is_weekend = weekday >= 5
            if is_weekend:
                tomorrow = vn_now + timedelta(days=1)
                while tomorrow.weekday() >= 5:
                    tomorrow += timedelta(days=1)
                wake_up_time = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                sleep_seconds = (wake_up_time - vn_now).total_seconds()
                if sleep_seconds > 0:
                    print(f"🌙 [AUTO] Hôm nay là cuối tuần. Tạm nghỉ đến {wake_up_time.strftime('%H:%M %d/%m')}...", flush=True)
                    time.sleep(min(sleep_seconds, 3600))
                    continue

            is_lunch_break = (current_hour == 11 and current_minute >= 30) or (current_hour == 12)
            is_trading_time = (9 <= current_hour < 15) and (not is_lunch_break)
            
            # === BỔ SUNG: cửa sổ chốt sổ 15h00-15h05 ===
            # Trong khoảng này vẫn chạy để bắt giá ATC muộn
            is_closing_window = (current_hour == 15 and current_minute < 5)

            if not (is_trading_time or is_closing_window):
                if is_lunch_break:
                    wake_up_time = vn_now.replace(hour=13, minute=0, second=0, microsecond=0)
                else:
                    # Nếu đã qua 15h05 → ngủ tới 8h55 hôm sau
                    # Nếu đang trong 15h00-15h05 → ngủ ngắn 15s rồi check lại
                    if current_hour == 15 and current_minute < 5:
                        time.sleep(15)
                        continue
                    else:
                        wake_up_time = vn_now.replace(hour=8, minute=55, second=0, microsecond=0)
                        if current_hour >= 15:
                            wake_up_time += timedelta(days=1)
                        while wake_up_time.weekday() >= 5:
                            wake_up_time += timedelta(days=1)
                        
                sleep_seconds = (wake_up_time - vn_now).total_seconds()
                if sleep_seconds > 0:
                    print(f"🌙 [AUTO] Ngoài giờ giao dịch. Tạm nghỉ đến {wake_up_time.strftime('%H:%M %d/%m')}...", flush=True)
                    time.sleep(min(sleep_seconds, 3600))
                    continue

            if current_hour == 10 and current_minute == 0:
                all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                any_changed = False
                
                print(f"🔍 [AUTO] Kiểm tra toàn bộ {len(all_symbols)} mã (Ngày lễ hay ngày làm việc?)...", flush=True)
                
                for sym in all_symbols:
                    doc = collection.find_one({"symbol": sym})
                    if doc:
                        if doc.get("close") != doc.get("prev_close") or doc.get("volume", 0) > 0:
                            any_changed = True
                            print(f"  ✅ [AUTO] Thị trường đang giao dịch (Mã {sym} đã biến động).", flush=True)
                            break
                
                if not any_changed:
                    print(f"🌙 [AUTO] Toàn bộ {len(all_symbols)} mã đều đứng giá. Kết luận: Hôm nay là NGÀY NGHỈ.", flush=True)
                    tomorrow = vn_now + timedelta(days=1)
                    while tomorrow.weekday() >= 5:
                        tomorrow += timedelta(days=1)
                    wake_up_time = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                    sleep_seconds = (wake_up_time - vn_now).total_seconds()
                    print(f"🌙 [AUTO] Tạm nghỉ đến {wake_up_time.strftime('%H:%M %d/%m')}...", flush=True)
                    time.sleep(min(sleep_seconds, 3600))
                    continue

            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            crawled_count = collection.count_documents({"chot_so_today": today_str})

            if crawled_count >= len(all_symbols) and len(all_symbols) > 0:
                # Nếu đang trong 15h00-15h05 mà đã chốt sổ → ngủ 15s chờ 15h05 để re-check
                if current_hour == 15 and current_minute < 5:
                    time.sleep(15)
                    continue
                time.sleep(60)
                continue

            if not is_crawling:
                symbols_to_crawl = []
                for sym in all_symbols:
                    doc = collection.find_one({"symbol": sym}, {"chot_so_today": 1})
                    if not doc or doc.get("chot_so_today") != today_str: 
                        symbols_to_crawl.append(sym)
                
                if not symbols_to_crawl: 
                    symbols_to_crawl = all_symbols 
                    
                _execute_crawl(symbols_to_crawl, is_manual=False)
                
            time.sleep(15)
            
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            time.sleep(10)

def full_data_crawler_worker():
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            is_bctc_time = (0 <= vn_now.hour < 6 and vn_now.minute == 0 and vn_now.second < 10)
            if is_bctc_time:
                target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                thirty_days_ago = datetime.utcnow() - timedelta(days=30)
                for symbol in target_symbols:
                    bctc_doc = bctc_col.find_one({"symbol": symbol}, {"updated_at": 1})
                    need = False
                    if not bctc_doc or "updated_at" not in bctc_doc: need = True
                    else:
                        try:
                            if datetime.strptime(bctc_doc["updated_at"], "%d/%m/%Y %H:%M:%S") < thirty_days_ago: need = True
                        except: need = True
                    if need:
                        res = _fetch_bctc_ssi(symbol)
                        if res:
                            res.update({"symbol": symbol, "updated_at": _now_str()})
                            bctc_col.update_one({"symbol": symbol}, {"$set": res}, upsert=True)
                            
                            extracted = extract_metrics_from_bctc_data(res)
                            if extracted:
                                collection.update_one({"symbol": symbol}, {"$set": extracted})
                                
                        time.sleep(3.0)
                time.sleep(3600) 
            else: time.sleep(30)
        except Exception: time.sleep(60)

def screener_worker():
    """
    Screener worker: chấm điểm sau khi chốt sổ (15h05+).
    """
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            current_hour = vn_now.hour
            current_minute = vn_now.minute
            
            all_symbols_count = collection.count_documents({})
            crawled_count = collection.count_documents({"chot_so_today": today_str})
            
            is_1630 = (current_hour == 16 and vn_now.minute == 30 and vn_now.second < 10)
            
            # === SỬA: chỉ chấm điểm sau 15h05 ===
            is_after_close = (current_hour > 15) or (current_hour == 15 and current_minute >= 5)
            is_just_closed = (is_after_close and crawled_count >= all_symbols_count and all_symbols_count > 0)

            if is_1630 or is_just_closed:
                today_screener_check = screener_col.find_one({"date": today_str})
                if today_screener_check:
                    time.sleep(60)
                    continue

                print(f"\n[SCREENER] ⏰ Kích hoạt chấm điểm (Lý do: {'16h30' if is_1630 else 'Vừa chốt sổ sau 15h05'})", flush=True)
                
                stock_db = {}
                print("[SCREENER] 📥 Đang nạp dữ liệu BCTC để tính toán các chỉ số...", flush=True)
                for doc in collection.find({}):
                    sym = doc["symbol"]
                    bctc = bctc_col.find_one({"symbol": sym}, {"_id": 0})
                    doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                    
                    if bctc:
                        extracted = extract_metrics_from_bctc_data(bctc)
                        if extracted:
                            doc.update(extracted) 
                            collection.update_one({"symbol": sym}, {"$set": extracted})
                    
                    stock_db[sym] = doc
                
                normalized_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
                sector_medians = compute_sector_medians(normalized_db)
                results = []
                
                for symbol, nd in normalized_db.items():
                    grp = get_sub_sector(symbol)
                    cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(symbol))
                    med, _ = get_median_for_symbol(symbol, sector_medians)
                    res = evaluate_stock_from_db(symbol, nd, med, grp, cfg)
                    if res: 
                        res["date"] = today_str
                        results.append(res)
                
                if results:
                    print(f"[SCREENER] 🔄 Đang tính lại final_decision lần 2 để đảm bảo tính nhất quán...", flush=True)
                    for r in results:
                        sym = r["symbol"]
                        nd_final = normalized_db.get(sym, {})
                        
                        fd_label, fd_color, fd_reason = compute_final_decision(
                            fund_score=r.get("fundamental_score", 0),
                            tech_score=r.get("technical_score", 0),
                            val_score=r.get("valuation_score", 0),
                            total_score=r.get("total_score", 0),
                            verdict=r.get("valuation_verdict", "CHƯA TÍNH"),
                            cfo=nd_final.get("cfo_latest", 0),
                            npat=nd_final.get("net_profit_ttm", 0),
                            roe=nd_final.get("roe", 0),
                            debt_to_equity=nd_final.get("debt_to_equity", 0),
                            discount_to_fair_pct=r.get("discount_to_fair_pct", 0),
                            close_price=r.get("close_price", 0),
                            fair_high=r.get("fair_value_high", 0),
                            fair_low=r.get("fair_value_low", 0)
                        )
                        r["final_decision"] = fd_label
                        r["final_decision_color"] = fd_color
                        r["final_decision_reason"] = fd_reason
                    print(f"[SCREENER] ✅ Đã tính lại final_decision cho {len(results)} mã.", flush=True)

                if results:
                    print(f"[SCREENER] 📝 Đang tạo/cập nhật Template cho các mã chưa có AI thật...", flush=True)
                    template_count = 0
                    for r in results:
                        sym = r["symbol"]
                        existing = collection.find_one({"symbol": sym}, {"ai_bctc": 1, "ai_source": 1})
                        if existing and existing.get("ai_bctc") and existing.get("ai_source") == "AI":
                            continue
                        
                        nd_for_template = normalized_db.get(sym, {})
                        template_data = build_fallback_narrative(sym, nd_for_template, r, get_sector(sym))
                        
                        if template_data:
                            collection.update_one({"symbol": sym}, {"$set": {
                                "ai_bctc": template_data.get("bctc", ""),
                                "ai_catalyst": template_data.get("catalyst", ""),
                                "ai_risks": template_data.get("risks", ""),
                                "ai_outlook": template_data.get("outlook", ""),
                                "ai_recommendation": template_data.get("recommendation", ""),
                                "ai_price_zone": template_data.get("price_zone", ""),
                                "ai_macro_view": template_data.get("macro_view", ""),
                                "ai_updated_at": _now_str(),
                                "ai_source": "TEMPLATE"
                            }})
                            template_count += 1
                    print(f"[SCREENER] ✅ Đã tạo/cập nhật Template cho {template_count} mã (chưa có AI thật).", flush=True)
                        
                if results:
                    results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                    screener_col.delete_many({}) 
                    screener_col.insert_many(results) 
                    commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Update Screener {_now_str()}")
                    print(f"[SCREENER] ✅ Đã chấm điểm xong cho {len(results)} mã.", flush=True)

                    try:
                        up_count = 0; down_count = 0; ref_count = 0
                        buy_strong = 0; buy_accumulate = 0; avoid = 0
                        strong_movers = []; value_stocks = []; danger_stocks = []
                        missing_price_count = 0; missing_pe_count = 0; missing_pb_count = 0; missing_roe_count = 0
                        missing_pe_symbols = []; missing_pb_symbols = []; missing_roe_symbols = []; missing_price_symbols = []

                        for r in results:
                            decision = r.get("final_decision", "")
                            if "MUA MẠNH" in decision: buy_strong += 1
                            elif "MUA TÍCH SẢN" in decision: buy_accumulate += 1
                            elif "TRÁNH" in decision: 
                                avoid += 1
                                if len(danger_stocks) < 3:
                                    danger_stocks.append(f"  • {r['symbol']} (Điểm: {r.get('total_score', 0)})")

                            stock_data = stock_db.get(r['symbol'], {})
                            pct_change = stock_data.get("price_change_percent", 0)
                            vol_pct = stock_data.get("volume_percent", 0)
                            close_price = stock_data.get("close", 0)

                            if pct_change > 0.05: up_count += 1
                            elif pct_change < -0.05: down_count += 1
                            else: ref_count += 1

                            if abs(pct_change) >= 3.0 and vol_pct >= 120.0:
                                arrow = "🔴" if pct_change < 0 else "🟢"
                                strong_movers.append(f"  {arrow} <b>{r['symbol']}</b>: {pct_change:+.2f}% (Vol: {vol_pct:.0f}%)")

                            if "RẺ" in r.get("valuation_verdict", "") and r.get("fundamental_score", 0) >= 30:
                                if len(value_stocks) < 3:
                                    value_stocks.append(f"  • <b>{r['symbol']}</b> ({r.get('sector')}): Giá {close_price:,.0f} - Vùng mua ≤ {r.get('fair_value_low',0):,.0f}")
                            
                            warnings = r.get("data_warnings", [])
                            if "Thiếu Giá" in warnings: 
                                missing_price_count += 1
                                if len(missing_price_symbols) < 5: missing_price_symbols.append(r['symbol'])
                            if "Thiếu P/E" in warnings: 
                                missing_pe_count += 1
                                if len(missing_pe_symbols) < 5: missing_pe_symbols.append(r['symbol'])
                            if "Thiếu P/B" in warnings: 
                                missing_pb_count += 1
                                if len(missing_pb_symbols) < 5: missing_pb_symbols.append(r['symbol'])
                            if "Thiếu ROE" in warnings: 
                                missing_roe_count += 1
                                if len(missing_roe_symbols) < 5: missing_roe_symbols.append(r['symbol'])

                        data_alert_lines = []
                        if missing_pe_count > 0:
                            sample = f" (VD: {', '.join(missing_pe_symbols)}...)" if missing_pe_symbols else ""
                            data_alert_lines.append(f"  • <b>{missing_pe_count} mã</b> thiếu P/E{sample}")
                        if missing_pb_count > 0:
                            sample = f" (VD: {', '.join(missing_pb_symbols)}...)" if missing_pb_symbols else ""
                            data_alert_lines.append(f"  • <b>{missing_pb_count} mã</b> thiếu P/B{sample}")
                        if missing_roe_count > 0:
                            sample = f" (VD: {', '.join(missing_roe_symbols)}...)" if missing_roe_symbols else ""
                            data_alert_lines.append(f"  • <b>{missing_roe_count} mã</b> thiếu ROE{sample}")
                        if missing_price_count > 0:
                            sample = f" (VD: {', '.join(missing_price_symbols)}...)" if missing_price_symbols else ""
                            data_alert_lines.append(f"  • <b>{missing_price_count} mã</b> bị thiếu Giá (Lỗi cào giá){sample}")

                        total_up_down = up_count + down_count
                        if total_up_down > 0:
                            ratio_up = (up_count / total_up_down) * 100
                        else:
                            ratio_up = 50

                        if ratio_up >= 65:
                            market_view = "🟢 <b>TÂM LÝ HƯNG PHẤN:</b> Dòng tiền lan tỏa mạnh, ưu tiên nắm giữ cổ phiếu."
                        elif ratio_up <= 35:
                            market_view = "🔴 <b>TÂM LÝ BI QUAN:</b> Áp lực bán chiếm ưu thế, hạn chế mua mới."
                        else:
                            market_view = "🟡 <b>THỊ TRƯỜNG PHÂN HÓA:</b> Dòng tiền có sự chọn lọc, chỉ nên mua các mã có nền tảng tốt."

                        msg_lines = []
                        if data_alert_lines:
                            msg_lines.append(f"⚠️ <b>CẢNH BÁO LỖI DỮ LIỆU API!</b>")
                            msg_lines.append(f"Phát hiện các mã bị thiếu thông số để chấm điểm:")
                            msg_lines.extend(data_alert_lines)
                            msg_lines.append(f"  👉 <i>Hãy chạy lại API /api/crawl-ratios hoặc /api/force-bctc!</i>")
                            msg_lines.append(f"━━━━━━━━━━━━━━━━━━")

                        msg_lines.extend([
                            f"📊 <b>BÁO CÁO THỊ TRƯỜNG TỰ ĐỘNG</b>",
                            f"⏰ Cập nhật: {_now_str()}",
                            f"━━━━━━━━━━━━━━━━━━",
                            f"<b>1️⃣ TỔNG QUAN ĐỘ RỘNG</b>",
                            f"  🟢 Tăng: <b>{up_count}</b> | 🔴 Giảm: <b>{down_count}</b> | ⚪ Đứng: {ref_count}",
                            f"  👉 {market_view}",
                            f"━━━━━━━━━━━━━━━━━━",
                            f"<b>2️⃣ DÒNG TIỀN ĐỘT BIẾN (Hot Money)</b>"
                        ])
                        
                        if strong_movers:
                            msg_lines.extend(strong_movers)
                        else:
                            msg_lines.append("  💤 Không có mã nào biến động quá 3%.")

                        msg_lines.append(f"━━━━━━━━━━━━━━━━━━")
                        msg_lines.append(f"<b>3️⃣ CƠ HỘI GIẢI NGÂN (Vùng mua an toàn)</b>")
                        if value_stocks:
                            msg_lines.extend(value_stocks)
                        else:
                            msg_lines.append("  ⏳ Hiện chưa có mã nào thỏa mãn (Rẻ + Cơ bản tốt).")

                        msg_lines.append(f"━━━━━━━━━━━━━━━━━━")
                        msg_lines.append(f"<b>4️⃣ CẢNH BÁO RỦI RO</b>")
                        msg_lines.append(f"  ❌ Có <b>{avoid}</b> mã bị đánh giá TRÁNH (Giá đắt, nền tảng yếu).")
                        if danger_stocks:
                             msg_lines.append(f"  ⚠️ <i>Điển hình: {', '.join(danger_stocks).replace('  • ', '')}</i>")

                        send_telegram("\n".join(msg_lines))
                        print("[SCREENER] 🚀 Đã gửi báo cáo thị trường qua Telegram.", flush=True)
                    except Exception as te:
                        print(f"[SCREENER] ❌ Lỗi khi gửi Telegram: {te}", flush=True)

                time.sleep(120)
            time.sleep(30)
        except Exception as e: 
            print(f"❌ [SCREENER] Lỗi: {e}", flush=True)
            time.sleep(60)


threading.Thread(target=auto_crawler_worker, daemon=True).start()
threading.Thread(target=full_data_crawler_worker, daemon=True).start()
threading.Thread(target=screener_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/")
def home(): 
    print("[API] 🏠 Gọi API trang chủ (/)", flush=True)
    return "API is running!", 200

@app.get("/api/status")
def api_status():
    print("[API] 📊 Gọi API /api/status", flush=True)
    return jsonify({"status": "success", "is_crawling": is_crawling, "is_paused": is_auto_crawl_paused, "message": "Đang cào real-time" if is_crawling else "Sẵn sàng"})

@app.get("/api/pause-crawl")
def pause_crawl():
    global is_auto_crawl_paused, stop_crawl_requested
    is_auto_crawl_paused = True
    stop_crawl_requested = True
    print("\n[API] ⏸️ Đã TẠM DỪNG hoàn toàn tiến trình cào tự động!", flush=True)
    return jsonify({"status": "success", "message": "Đã tạm dừng cào tự động."})

@app.get("/api/resume-crawl")
def resume_crawl():
    global is_auto_crawl_paused, stop_crawl_requested
    is_auto_crawl_paused = False
    stop_crawl_requested = False
    print("\n[API] ▶️ Đã TIẾP TỤC tiến trình cào tự động!", flush=True)
    return jsonify({"status": "success", "message": "Đã tiếp tục cào tự động."})

@app.post("/api/set-fhsc-key")
def api_set_fhsc_key():
    print("[API] 🔑 Gọi API /api/set-fhsc-key", flush=True)
    data = request.json
    new_key = data.get("key", "").strip()
    if not new_key: return jsonify({"status": "error", "message": "Key không được để trống"}), 400
    settings_col.update_one({"_id": "fhsc_key"}, {"$set": {"value": new_key, "updated_at": _now_str()}}, upsert=True)
    return jsonify({"status": "success", "message": "Đã lưu API Key mới thành công!"})

@app.get("/api/data")
def api_get_data():
    """Lấy toàn bộ giá realtime — NGUỒN CHÍNH cho Card."""
    print("[API] 📈 Gọi API /api/data (Lấy toàn bộ giá)", flush=True)
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
                # Đảm bảo prev_close luôn có trong response
                if "prev_close" not in doc:
                    doc["prev_close"] = 0
                
                if "price_change_percent" not in doc or doc["price_change_percent"] == 0:
                    close_price = doc.get("close", 0)
                    prev_close = doc.get("prev_close", 0)
                    pct = 0.0
                    avail = False
                    arrow, color = "—", "slate"
                    if close_price > 0 and prev_close > 0:
                        raw_pct = ((close_price - prev_close) / prev_close) * 100
                        if -30.0 <= raw_pct <= 30.0:
                            pct = round(raw_pct, 2)
                            avail = True
                            if pct > 0: arrow, color = "▲", "emerald"
                            elif pct < 0: arrow, color = "▼", "rose"
                    doc["price_change_percent"] = pct
                    doc["price_change_available"] = avail
                    doc["price_arrow"] = arrow
                    doc["price_color"] = color
                
                doc["volume_percent"] = doc.get("volume_percent", 0.0)
                data[sym] = doc
        return jsonify({"status": "success", "count": len(data), "data": data})
    except Exception as e: 
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/sync")
def api_sync():
    raw = request.args.get("symbols", "").strip()
    print(f"[API] 🔄 Gọi API /api/sync | Mã: {raw}", flush=True)
    if not raw: abort(400)
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    
    if is_crawling or _is_after_15h():
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc: 
                close_price = doc.get("close", 0)
                prev_close = doc.get("prev_close", 0)
                pct = 0.0
                avail = False
                arrow, color = "—", "slate"
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        pct = round(raw_pct, 2)
                        avail = True
                        if pct > 0: arrow, color = "▲", "emerald"
                        elif pct < 0: arrow, color = "▼", "rose"

                updated_data[sym] = {
                    "close": close_price, "close_price": close_price,
                    "prev_close": prev_close, "price_change_percent": pct,
                    "price_change_available": avail, "price_arrow": arrow,
                    "price_color": color, "updated_at": doc.get("updated_at", _now_str())
                }
        return jsonify({
            "status": "success", 
            "updated_at": _now_str() + " (Lấy từ DB)", 
            "updated_data": updated_data
        })

    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    return jsonify({
        "status": "success", 
        "updated_at": _now_str() + " (Bắt đầu cào)", 
        "updated_data": {}
    })

@app.get("/api/screener")
def api_get_screener():
    print("[API] 🏆 Gọi API /api/screener (Lấy điểm số)", flush=True)
    try: return jsonify({"status": "success", "data": list(screener_col.find({}, {"_id": 0}))})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/ai-analyze")
def api_ai_analyze():
    symbol = request.args.get("symbol", "").upper().strip()
    print(f"[API] 🤖 Gọi API /api/ai-analyze | Mã: {symbol}", flush=True)
    if not symbol: 
        return jsonify({"status": "error", "message": "Thiếu mã"}), 400
        
    try:
        sm = collection.find_one({"symbol": symbol}, {"_id": 0})
        si = screener_col.find_one({"symbol": symbol}, {"_id": 0})
        bctc_doc = bctc_col.find_one({"symbol": symbol}, {"_id": 0})
        
        if not sm or not si: 
            return jsonify({"status": "error", "message": "Chưa đủ dữ liệu"}), 404
        
        ai_data = {
            "bctc": sm.get("ai_bctc", ""),
            "catalyst": sm.get("ai_catalyst", ""),
            "risks": sm.get("ai_risks", ""),
            "outlook": sm.get("ai_outlook", ""),
            "recommendation": sm.get("ai_recommendation", ""),
            "price_zone": sm.get("ai_price_zone", ""),
            "macro_view": sm.get("ai_macro_view", ""),
            "valuation_verdict": sm.get("valuation_verdict", "CHƯA TÍNH"),
            "ai_updated_at": sm.get("ai_updated_at", "Chưa phân tích"),
            "_source": "DATABASE"
        }

        bctc_warning = ""
        bctc_period = "Không rõ"
        bctc_updated_at = "Chưa cào"
        if bctc_doc:
            bctc_updated_at = bctc_doc.get("updated_at", "Chưa cào")
            bctc_period = bctc_doc.get("latest_period", "Không rõ")
            try:
                last_update_date = datetime.strptime(bctc_updated_at, "%d/%m/%Y %H:%M:%S")
                if (datetime.utcnow() - last_update_date).days > 45:
                    bctc_warning = f"⚠️ CẢNH BÁO: BCTC đã cũ, kỳ mới nhất là {bctc_period}, cào lúc {bctc_updated_at}."
            except:
                pass
        
        if not ai_data["bctc"] or not ai_data["outlook"]:
            return jsonify({
                "status": "warning", 
                "message": "Mã này chưa có phân tích AI. Vui lòng đợi hệ thống chạy ngầm hoặc liên hệ Admin!",
                "data": None
            }), 200

        return jsonify({
            "status": "success", 
            "data": ai_data, 
            "bctc_period": bctc_period,
            "bctc_updated_at": bctc_updated_at,
            "warning": bctc_warning
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/admin/force-ai-single")
def admin_force_ai_single():
    if request.args.get("key") != ADMIN_SECRET_KEY:
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin!"}), 403
        
    global is_ai_running
    
    symbol = request.args.get("symbol", "").upper().strip()
    
    if not symbol:
        return jsonify({"status": "error", "message": "Vui lòng truyền tham số ?symbol=XXX"}), 400
        
    if is_ai_running:
        return jsonify({"status": "error", "message": "Hệ thống đang chạy AI hàng loạt, vui lòng đợi!"}), 400
    
    if not ALIBABA_API_KEY:
        return jsonify({"status": "error", "message": "Thiếu ALIBABA_API_KEY trong biến môi trường!"}), 500
    
    sm = collection.find_one({"symbol": symbol}, {"_id": 0})
    si = screener_col.find_one({"symbol": symbol}, {"_id": 0})
    
    if not sm or not si:
        return jsonify({
            "status": "error", 
            "message": f"Mã {symbol} chưa đủ dữ liệu nền. Hãy chạy /api/force-screener trước."
        }), 404

    is_ai_running = True
    print(f"\n[ADMIN-AI-SINGLE] 🚀 Bắt đầu chạy AI cho mã {symbol}...", flush=True)
    
    def _run():
        global is_ai_running
        try:
            sector = si.get("sector", "SAN_XUAT_TIEU_DUNG_PHONG_THU")
            ai_result = _call_ai_for_symbol(symbol, sm, si, sector)
            
            if ai_result:
                collection.update_one({"symbol": symbol}, {"$set": {
                    "ai_bctc": ai_result.get("bctc", ""),
                    "ai_catalyst": ai_result.get("catalyst", ""),
                    "ai_risks": ai_result.get("risks", ""),
                    "ai_outlook": ai_result.get("outlook", ""),
                    "ai_recommendation": ai_result.get("recommendation", ""),
                    "ai_price_zone": ai_result.get("price_zone", ""),
                    "ai_macro_view": ai_result.get("macro_view", ""),
                    "ai_updated_at": _now_str(),
                    "ai_source": "AI"
                }})
                
                screener_col.update_one({"symbol": symbol}, {"$set": {
                    "bctc_evaluation": ai_result.get("bctc", ""),
                    "outlook": ai_result.get("outlook", ""),
                    "ai_recommendation": ai_result.get("recommendation", ""),
                    "ai_price_zone": ai_result.get("price_zone", ""),
                    "macro_view": ai_result.get("macro_view", ""),
                    "ai_updated_at": _now_str(),
                    "ai_source": "AI"
                }})
                
                print(f"[ADMIN-AI-SINGLE] ✅ {symbol} - Đã lưu phân tích AI thành công vào cả 2 bảng!", flush=True)
            else:
                print(f"[ADMIN-AI-SINGLE] ❌ {symbol} - Gọi AI thất bại.", flush=True)
        finally:
            is_ai_running = False

    threading.Thread(target=_run, daemon=True).start()
    
    return jsonify({
        "status": "success", 
        "message": f"Đã bắt đầu chạy AI cho mã {symbol}. Theo dõi log Render!"
    })

@app.get("/api/admin/force-ai")
def admin_force_ai():
    global is_ai_running
    if is_ai_running:
        return jsonify({"status": "error", "message": "Đang chạy AI cho toàn bộ mã, vui lòng đợi!"}), 400
    
    if not ALIBABA_API_KEY:
        return jsonify({"status": "error", "message": "Thiếu ALIBABA_API_KEY!"}), 500
    
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    
    is_ai_running = True
    print(f"\n[ADMIN-AI] 🚀 Bắt đầu chạy AI cho toàn bộ {len(all_symbols)} mã...", flush=True)
    
    def _run():
        global is_ai_running
        try:
            total = len(all_symbols)
            count = 0
            
            for i, sym in enumerate(all_symbols, 1):
                sm = collection.find_one({"symbol": sym}, {"_id": 0})
                si = screener_col.find_one({"symbol": sym}, {"_id": 0})
                
                if not sm or not si:
                    print(f"[ADMIN-AI] [{i}/{total}] ⚠️ {sym} - Thiếu dữ liệu, bỏ qua.", flush=True)
                    continue
                
                sector = si.get("sector", "SAN_XUAT_TIEU_DUNG_PHONG_THU")
                ai_result = _call_ai_for_symbol(sym, sm, si, sector)
                
                if ai_result:
                    collection.update_one({"symbol": sym}, {"$set": {
                        "ai_bctc": ai_result.get("bctc", ""),
                        "ai_catalyst": ai_result.get("catalyst", ""),
                        "ai_risks": ai_result.get("risks", ""),
                        "ai_outlook": ai_result.get("outlook", ""),
                        "ai_recommendation": ai_result.get("recommendation", ""),
                        "ai_price_zone": ai_result.get("price_zone", ""),
                        "ai_macro_view": ai_result.get("macro_view", ""),
                        "ai_updated_at": _now_str()
                    }})
                    count += 1
                    print(f"[ADMIN-AI] [{i}/{total}] ✅ {sym} - OK", flush=True)
                else:
                    print(f"[ADMIN-AI] [{i}/{total}] ❌ {sym} - Lỗi", flush=True)
                
                time.sleep(3.0)
            
            print(f"[ADMIN-AI] 🏁 Hoàn thành: {count}/{total} mã.", flush=True)
            
            gh_data, _ = get_github_file_data(SCREENER_JSON_FILE)
            if gh_data:
                for item in gh_data:
                    sym = item.get("symbol")
                    if sym:
                        stock_doc = collection.find_one({"symbol": sym}, {"ai_bctc": 1, "ai_outlook": 1, "ai_recommendation": 1, "ai_price_zone": 1, "ai_macro_view": 1, "ai_updated_at": 1})
                        if stock_doc:
                            item.update({
                                "bctc": stock_doc.get("ai_bctc", ""),
                                "outlook": stock_doc.get("ai_outlook", ""),
                                "ai_recommendation": stock_doc.get("ai_recommendation", ""),
                                "ai_price_zone": stock_doc.get("ai_price_zone", ""),
                                "macro_view": stock_doc.get("ai_macro_view", ""),
                                "ai_updated_at": stock_doc.get("ai_updated_at", "")
                            })
                commit_multiple_files_to_github({SCREENER_JSON_FILE: gh_data}, f"Update AI toàn bộ {_now_str()}")
                print(f"[ADMIN-AI] 🚀 Đã đồng bộ AI lên Github.", flush=True)
        finally:
            is_ai_running = False
    
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({
        "status": "success", 
        "message": f"Đã bắt đầu chạy AI cho {len(all_symbols)} mã. Xem log Render!"
    })

@app.get("/api/force-history")
def force_history():
    print(f"\n[API] 📥 Gọi API /api/force-history (Ép cào nến 30 ngày)", flush=True)
    global is_full_data_running
    if is_full_data_running: return jsonify({"status": "error", "message": "Đang chạy"}), 400
    is_full_data_running = True
    def _run():
        global is_full_data_running
        try:
            target = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            end, start = datetime.now().strftime("%Y-%m-%d"), (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            print(f"[HISTORY] 🚀 Bắt đầu cào nến 30 ngày cho {len(target)} mã...", flush=True)
            for sym in target:
                df = _fetch_history_ssi(sym, start, end)
                if df is not None and not df.empty:
                    bars = [{"time": r['time'].strftime('%Y-%m-%d') if hasattr(r['time'], 'strftime') else str(r['time'])[:10], "open": float(r['open']), "high": float(r['high']), "low": float(r['low']), "close": float(r['close']), "volume": int(r['volume'])} for _, r in df.iterrows()]
                    for b in bars: history_col.update_one({"symbol": sym}, {"$pull": {"history_d1": {"time": b["time"]}}})
                    history_col.update_one({"symbol": sym}, {"$push": {"history_d1": {"$each": bars, "$slice": -2500}}}, upsert=True)
                    doc_hist = history_col.find_one({"symbol": sym})
                    if doc_hist and doc_hist.get("history_d1"):
                        d1 = sorted(doc_hist["history_d1"], key=lambda x: x["time"])
                        history_col.update_one({"symbol": sym}, {"$set": {"history_w1": aggregate_candles(d1, 'W1'), "history_mn": aggregate_candles(d1, 'MN')}})
                time.sleep(1.0)
            print(f"[HISTORY] 🏁 Đã cào xong toàn bộ nến 30 ngày.", flush=True)
        finally: is_full_data_running = False
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào 30 ngày nến..."})

@app.get("/api/crawl-full-history")
def crawl_full_history():
    print(f"\n[API] 📥 Gọi API /api/crawl-full-history (Ép cào nến 10 năm)", flush=True)
    global is_full_data_running
    if is_full_data_running: return jsonify({"status": "error", "message": "Đang chạy"}), 400
    is_full_data_running = True
    def _run():
        global is_full_data_running
        try:
            target = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            end, start = datetime.now().strftime("%Y-%m-%d"), (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
            print(f"[HISTORY] 🚀 Bắt đầu cào nến 10 năm cho {len(target)} mã...", flush=True)
            for sym in target:
                df = _fetch_history_ssi(sym, start, end)
                if df is not None and not df.empty:
                    bars = [{"time": r['time'].strftime('%Y-%m-%d') if hasattr(r['time'], 'strftime') else str(r['time'])[:10], "open": float(r['open']), "high": float(r['high']), "low": float(r['low']), "close": float(r['close']), "volume": int(r['volume'])} for _, r in df.iterrows()]
                    history_col.update_one({"symbol": sym}, {"$set": {"history_d1": bars, "history_w1": aggregate_candles(bars, 'W1'), "history_mn": aggregate_candles(bars, 'MN')}}, upsert=True)
                time.sleep(1.5)
            print(f"[HISTORY] 🏁 Đã cào xong toàn bộ nến 10 năm.", flush=True)
        finally: is_full_data_running = False
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào 10 năm nến..."})

@app.get("/api/force-bctc")
def force_bctc():
    global is_full_data_running
    if is_full_data_running: return jsonify({"status": "error", "message": "Đang chạy"}), 400
    is_full_data_running = True
    
    symbols_str = request.args.get("symbols", "").upper().strip()
    start_idx, limit_idx = int(request.args.get("start", 0)), int(request.args.get("limit", 15))
    specific = request.args.get("symbol", "").upper().strip()
    
    print(f"\n[API] 📥 Gọi API /api/force-bctc | Symbols: {symbols_str} | Specific: {specific}", flush=True)

    def _run():
        global is_full_data_running
        try:
            if symbols_str:
                target = [s.strip() for s in symbols_str.split(",") if s.strip()]
            elif specific:
                target = [specific]
            else:
                target = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})][start_idx:start_idx+limit_idx]
                
            print(f"[BCTC-CRAWL] 🚀 Bắt đầu cào BCTC cho {len(target)} mã...", flush=True)
            
            for i, sym in enumerate(target):
                print(f"\n[BCTC-CRAWL] [{i+1}/{len(target)}] Đang xử lý {sym}...", flush=True)
                res = _fetch_bctc_ssi(sym)
                
                if res: 
                    bctc_col.update_one({"symbol": sym}, {"$set": {**res, "symbol": sym, "updated_at": _now_str()}}, upsert=True)
                    period = res.get("latest_period", "Không rõ")
                    print(f"  ✅ [THÀNH CÔNG] {sym} | Kỳ BCTC: {period}", flush=True)
                    
                    extracted = extract_metrics_from_bctc_data(res)
                    if extracted:
                        collection.update_one({"symbol": sym}, {"$set": extracted})
                        print(f"  ✅ [FINANCIALS] {sym}: Đã trích xuất thành công (CFO, LNST, D/E, GM)", flush=True)
                else:
                    print(f"  ❌ [THẤT BẠI] {sym} | Không lấy được dữ liệu", flush=True)
                    
                time.sleep(3.0)
                
            print(f"\n[BCTC-CRAWL] 🏁 Đã cào xong toàn bộ {len(target)} mã.", flush=True)
        finally: 
            is_full_data_running = False
            
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": f"Đang cào BCTC cho {len(symbols_str.split(',')) if symbols_str else limit_idx} mã..."})

@app.get("/api/bctc-status")
def api_bctc_status():
    print("[API] 📋 Gọi API /api/bctc-status", flush=True)
    try:
        all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1, "_id": 0})]
        result = []
        for sym in all_symbols:
            bctc_doc = bctc_col.find_one({"symbol": sym}, {"_id": 0, "symbol": 1, "updated_at": 1, "latest_period": 1})
            if bctc_doc and bctc_doc.get("updated_at"):
                result.append({
                    "symbol": sym, "has_bctc": True,
                    "updated_at": bctc_doc.get("updated_at", "Chưa rõ"),
                    "latest_period": bctc_doc.get("latest_period", "Chưa rõ")
                })
            else:
                result.append({
                    "symbol": sym, "has_bctc": False,
                    "updated_at": "Chưa cào", "latest_period": "Chưa có"
                })
        result = sorted(result, key=lambda x: x["symbol"])
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        log_exception("[API BCTC STATUS]", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/crawl-full-bctc")
def crawl_full_bctc():
    print(f"\n[API] 📥 Gọi API /api/crawl-full-bctc", flush=True)
    global is_full_data_running
    if is_full_data_running: return jsonify({"status": "error", "message": "Đang chạy"}), 400
    is_full_data_running = True
    def _run():
        global is_full_data_running
        try:
            target = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            print(f"[BCTC-CRAWL] 🚀 Bắt đầu cào toàn bộ BCTC cho {len(target)} mã...", flush=True)
            for i, sym in enumerate(target):
                print(f"[BCTC-CRAWL] [{i+1}/{len(target)}] Đang xử lý {sym}...", flush=True)
                res = _fetch_bctc_ssi(sym)
                if res: 
                    bctc_col.update_one({"symbol": sym}, {"$set": {**res, "symbol": sym, "updated_at": _now_str()}}, upsert=True)
                    extracted = extract_metrics_from_bctc_data(res)
                    if extracted:
                        collection.update_one({"symbol": sym}, {"$set": extracted})
                time.sleep(3.0)
            print(f"[BCTC-CRAWL] 🏁 Đã cào xong toàn bộ BCTC.", flush=True)
        finally: is_full_data_running = False
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào FULL BCTC..."})

@app.get("/api/crawl-ratios")
def crawl_ratios():
    print(f"\n[API] 📥 Gọi API /api/crawl-ratios", flush=True)
    global is_full_data_running
    if is_full_data_running: return jsonify({"status": "error", "message": "Đang chạy"}), 400
    is_full_data_running = True

    def _run():
        global is_full_data_running
        try:
            target = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            print(f"[RATIO-CRAWL] 🚀 Bắt đầu cào bảng Ratio cho {len(target)} mã...", flush=True)
            
            for i, sym in enumerate(target):
                print(f"[RATIO-CRAWL] [{i+1}/{len(target)}] Đang lấy ratio cho {sym}...", flush=True)
                
                ratios = {}
                for key, code in VNDIRECT_RATIOS.items():
                    val = get_vndirect_ratio(sym, code)
                    ratios[key] = val if val is not None else 0
                    time.sleep(0.2)

                roe_val = ratios.get("roe", 0)
                if roe_val and roe_val <= 1: roe_val *= 100
                
                roa_val = ratios.get("roa", 0)
                if roa_val and roa_val <= 1: roa_val *= 100
                
                div_val = ratios.get("dividend_yield", 0)
                if div_val and div_val <= 1: div_val *= 100

                eps_raw = ratios.get("eps", 0)
                if eps_raw >= 1000:
                    eps_raw = eps_raw / 1000.0
                eps_final = round(eps_raw, 2)

                collection.update_one({"symbol": sym}, {"$set": {
                    "pe": round(ratios.get("pe", 0), 2),
                    "pb": round(ratios.get("pb", 0), 2),
                    "ps": round(ratios.get("ps", 0), 2),
                    "eps": eps_final, 
                    "roe": round(roe_val, 1),
                    "roa": round(roa_val, 1),
                    "dividend_yield": round(div_val, 2),
                    "ratio_updated_at": _now_str()
                }}, upsert=True)
                
                print(f"  ✅ [RATIO] {sym} thành công (P/E: {ratios.get('pe', 0)}, EPS: {eps_final}, ROE: {round(roe_val, 1)}%)", flush=True)
                time.sleep(0.5)

            print(f"[RATIO-CRAWL] 🏁 Đã cào xong toàn bộ bảng Ratio.", flush=True)
        finally: 
            is_full_data_running = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào bảng Ratio..."})

@app.get("/api/force-screener")
def force_screener():
    if request.args.get("key") != ADMIN_SECRET_KEY:
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin!"}), 403
        
    print(f"\n[API] 📥 Gọi API /api/force-screener", flush=True)
    global is_screener_running
    if is_screener_running: 
        return jsonify({"status": "error", "message": "Đang chạy"}), 400
        
    is_screener_running = True
    print(f"[SCREENER] 🚀 Bắt đầu chấm điểm lại toàn bộ...", flush=True)
    
    def _run():
        global is_screener_running
        try:
            stock_db = {}
            for doc in collection.find({}):
                sym = doc["symbol"]
                bctc = bctc_col.find_one({"symbol": sym}, {"_id": 0})
                doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                
                if bctc:
                    extracted = extract_metrics_from_bctc_data(bctc)
                    if extracted:
                        doc.update(extracted)
                        collection.update_one({"symbol": sym}, {"$set": extracted})
                
                stock_db[sym] = doc
            
            norm_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
            medians = compute_sector_medians(norm_db)
            results = []
            
            for sym, nd in norm_db.items():
                grp = get_sub_sector(sym)
                cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(sym))
                med, _ = get_median_for_symbol(sym, medians)
                res = evaluate_stock_from_db(sym, nd, med, grp, cfg)
                if res: results.append(res)
            
            if results:
                results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                screener_col.delete_many({}) 
                screener_col.insert_many(results) 
                commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Force Screener {_now_str()}")
                print(f"[SCREENER] ✅ Đã chấm điểm xong {len(results)} mã.", flush=True)
        except Exception as e:
            print(f"[SCREENER] ❌ Lỗi: {e}", flush=True)
        finally: 
            is_screener_running = False
            
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang chấm điểm lại..."})

@app.get("/api/force-realtime")
def force_realtime():
    print(f"\n[API] ⚡ Gọi API /api/force-realtime", flush=True)
    global is_crawling
    
    if is_crawling:
        return jsonify({"status": "error", "message": "Hệ thống đang cào, vui lòng đợi!"}), 400
        
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    
    return jsonify({
        "status": "success", 
        "message": f"Đã kích hoạt ép cào realtime cho {len(all_symbols)} mã."
    })

@app.get("/api/reset-today")
def reset_today():
    print(f"\n[API] 📥 Gọi API /api/reset-today", flush=True)
    today_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")
    result = collection.update_many({"chot_so_today": today_str}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
    return jsonify({"status": "success", "message": f"Đã xóa cờ chốt sổ của {result.modified_count} mã."})

@app.get("/api/force-fix")
def force_fix_data():
    print(f"\n[API] 📥 Gọi API /api/force-fix", flush=True)
    collection.update_many({}, {"$unset": {"chot_so_today": "", "is_locked": "", "trading_date": ""}})
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    return jsonify({"status": "success", "message": f"Đang ép cào lại toàn bộ {len(all_symbols)} mã."})

@app.get("/api/retry-now")
def retry_now():
    print(f"\n[API] 📥 Gọi API /api/retry-now", flush=True)
    if is_crawling:
        return jsonify({"status": "error", "message": "Hệ thống đang chạy!"})
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    if not incomplete_symbols:
        return jsonify({"status": "success", "message": "Không có mã nào bị lỗi!"})
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào bù {len(incomplete_symbols)} mã!"})

# ===================== API LỊCH SỬ (ĐÃ FIX ĐỒNG BỘ) =====================
@app.get("/api/history")
def api_get_history():
    """
    API lịch sử nến — ĐÃ FIX để ĐỒNG BỘ với /api/data.
    - Đọc nến cũ từ history_col
    - Nếu có giá realtime mới hơn trong collection → cập nhật/chèn nến hôm nay
    - Đảm bảo Chart và Card luôn cùng giá
    """
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"status": "error", "message": "Thiếu mã"}), 400
        
    print(f"[API] 📊 Gọi API /api/history | Mã: {symbol}", flush=True)
    
    try:
        hist_doc = history_col.find_one({"symbol": symbol}, {"_id": 0})
        history_d1 = hist_doc.get("history_d1", []) if hist_doc else []
        
        # === FIX ĐỒNG BỘ: Lấy giá realtime từ collection ===
        live_doc = collection.find_one({"symbol": symbol}, {"_id": 0})
        
        if live_doc and live_doc.get("close", 0) > 0:
            # Xác định "ngày hôm nay" của nến realtime
            today_live = live_doc.get("trading_date")
            
            if not today_live:
                vn_now = datetime.utcnow() + timedelta(hours=7)
                today_live = vn_now.strftime("%Y-%m-%d")
            
            # Chuẩn bị bar hôm nay
            bar_today = {
                "time": today_live,
                "open": float(live_doc.get("open", 0) or live_doc["close"]),
                "high": float(live_doc.get("high", 0) or live_doc["close"]),
                "low": float(live_doc.get("low", 0) or live_doc["close"]),
                "close": float(live_doc["close"]),
                "volume": int(live_doc.get("volume", 0) or 0)
            }
            
            # Tìm nến cùng ngày
            existing_idx = None
            for i, b in enumerate(history_d1):
                if b.get("time") == today_live:
                    existing_idx = i
                    break
            
            if existing_idx is not None:
                # Đã có → cập nhật close/high/low/volume mới nhất
                history_d1[existing_idx]["close"] = bar_today["close"]
                history_d1[existing_idx]["high"] = max(
                    float(history_d1[existing_idx].get("high", 0) or 0),
                    bar_today["high"]
                )
                old_low = float(history_d1[existing_idx].get("low", 0) or 0)
                history_d1[existing_idx]["low"] = (
                    min(old_low, bar_today["low"]) if old_low > 0 else bar_today["low"]
                )
                if bar_today["volume"] > 0:
                    history_d1[existing_idx]["volume"] = bar_today["volume"]
            else:
                # Chưa có → chèn nến hôm nay
                history_d1.append(bar_today)
                history_d1.sort(key=lambda x: x["time"])
        
        if not history_d1:
            return jsonify({"status": "error", "message": "Không có dữ liệu lịch sử"}), 404
        
        return jsonify({
            "status": "success", 
            "symbol": symbol,
            "history_d1": history_d1,
            "history_w1": hist_doc.get("history_w1", []) if hist_doc else [],
            "history_mn": hist_doc.get("history_mn", []) if hist_doc else []
        })
    except Exception as e:
        log_exception("[API HISTORY]", e)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
