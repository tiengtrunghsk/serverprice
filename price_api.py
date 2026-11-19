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
import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

import requests
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from flask_compress import Compress
from pymongo import MongoClient, UpdateOne
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
    _formatter.converter = lambda ts: (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=7)).timetuple()
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
logger.propagate = False

def log_exception(context: str, exc: Exception):
    logger.error("%s | %s: %s", context, type(exc).__name__, str(exc), exc_info=True)

# ===================== VNSTOCK =====================
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

def _admin_key_valid() -> bool:
    """Xác thực tác vụ quản trị bằng header; vẫn nhận query key cho client cũ."""
    configured = str(ADMIN_SECRET_KEY or "").strip()
    supplied = str(request.headers.get("X-Admin-Key", "") or request.args.get("key", "")).strip()
    return bool(configured and supplied) and secrets.compare_digest(supplied, configured)

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

LIQUID_SYMBOLS_TOP = [
    "VNM", "VCB", "VIC", "VHM", "HPG", 
    "FPT", "MWG", "TCB", "VPB", "ACB",
    "BID", "CTG", "MSN", "GAS", "PLX"
]

LIQUID_SYMBOLS_MID = [
    "SSI", "VND", "HCM", "VCI", "MBS",
    "STB", "SHB", "TPB", "MBB", "HDB",
    "VIB", "LPB", "OCB", "MSB", "EIB",
    "PNJ", "FRT", "DGW", "REE", "PC1",
    "DGC", "DPM", "DCM", "HSG", "NKG",
    "KDH", "NLG", "DXG", "CEO", "PDR"
]

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

    # Đồng bộ đúng các index đã tạo trên Atlas; dùng cùng tên để create_index
    # có thể chạy lặp an toàn khi Render khởi động lại.
    logger.info("[MONGODB] Kiểm tra index symbol cho 4 collection...")
    collection.create_index([("symbol", 1)], name="idx_symbol_1")
    history_col.create_index([("symbol", 1)], name="idx_symbol_1")
    bctc_col.create_index([("symbol", 1)], name="idx_symbol_1")
    screener_col.create_index([("symbol", 1)], name="idx_symbol_1")
    logger.info("[MONGODB] Index symbol sẵn sàng cho stocks/history_data/bctc_data/screener_results.")

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

Compress(app)
app.config['COMPRESS_MIMETYPES'] = ['application/json', 'text/html', 'text/css', 'text/javascript']
app.config['COMPRESS_LEVEL'] = 6
app.config['COMPRESS_MIN_SIZE'] = 500

# === BIẾN TRẠNG THÁI ===
is_crawling = False
is_full_data_running = False
# History backfills use a dedicated sequential queue. 30-day and 10-year jobs
# must never write history_d1 concurrently.
_history_job_lock = threading.RLock()
_history_write_lock = threading.RLock()  # Serialize D1 backfill with realtime D1 commits
_history_job_queue = []
_history_job_state = {
    "status": "idle", "job_type": None, "current_symbol": None,
    "completed": 0, "total": 0, "success": 0, "failed": 0,
    "queued": 0, "message": "Chưa có tác vụ lịch sử", "updated_at": None
}
_history_worker_running = False
is_screener_running = False
is_ai_running = False
LAST_TELEGRAM_TIME = 0
stop_crawl_requested = False
is_auto_crawl_paused = False

_realtime_progress_lock = threading.RLock()
_realtime_progress = {
    "status": "idle", "job_type": None, "message": "Chưa có phiên cào",
    "current_symbol": None, "completed": 0, "total": 0,
    "success": 0, "failed": 0, "updated_at": None
}

def _realtime_progress_update(**fields):
    with _realtime_progress_lock:
        _realtime_progress.update(fields)
        _realtime_progress["updated_at"] = _now_str()

def _realtime_progress_snapshot():
    with _realtime_progress_lock:
        return dict(_realtime_progress)

# Tiến độ tác vụ quản trị: cập nhật từ chính vòng lặp xử lý, không ước lượng.
_admin_progress_lock = threading.RLock()
_admin_progress = {
    "status": "idle", "job_type": None, "message": "Chưa có tác vụ quản trị",
    "current_symbol": None, "completed": 0, "total": 0,
    "success": 0, "failed": 0, "updated_at": None
}

def _admin_progress_update(**fields):
    with _admin_progress_lock:
        _admin_progress.update(fields)
        _admin_progress["updated_at"] = _now_str()

def _admin_progress_start(job_type, message="Đang khởi động"):
    _admin_progress_update(
        status="running", job_type=job_type, message=message,
        current_symbol=None, completed=0, total=0, success=0, failed=0
    )

def _admin_progress_snapshot():
    with _admin_progress_lock:
        return dict(_admin_progress)


# FIX BUG #1: Lock chống race giữa screener_worker và /api/force-screener
screener_lock = threading.Lock()

# === BIẾN CHO HOLIDAY CHECKER ===
holiday_checked_today = None
holiday_status = None
holiday_detected_date = None
holiday_check_lock = threading.Lock()
is_holiday_paused = False

# === CACHE CHO /api/history-all ===
_history_cache = {"data": None, "timestamp": 0, "lock": threading.Lock()}
HISTORY_CACHE_TTL = 60

def get_current_fhsc_key():
    setting = settings_col.find_one({"_id": "fhsc_key"})
    if setting and setting.get("value"): return setting["value"]
    return ENV_FHSC_API_KEY

# ===================== HELPERS =====================
# FIX BUG #7: Dùng timezone-aware datetime thay vì utcnow() deprecated
def _vn_now() -> datetime:
    """Trả về datetime hiện tại theo giờ VN (UTC+7), timezone-aware."""
    return datetime.now(timezone.utc) + timedelta(hours=7)

def _now_str() -> str:
    return _vn_now().strftime("%d/%m/%Y %H:%M:%S")


def _expected_latest_bctc_period(now=None) -> str:
    """
    Kỳ BCTC quý gần nhất thường đã đến hạn công bố theo lịch tại Việt Nam.
    Dùng để phát hiện dữ liệu bị cũ, không giả định mọi doanh nghiệp công bố cùng ngày.
    """
    now = now or _vn_now()
    year, month = now.year, now.month

    if month >= 11:
        return f"{year}-Q3"
    if month >= 8:
        return f"{year}-Q2"
    if month >= 5:
        return f"{year}-Q1"
    if month >= 4:
        return f"{year - 1}-Q4"
    return f"{year - 1}-Q3"

def _get_trading_date_str() -> str:
    vn_now = _vn_now()
    if vn_now.hour < 9: vn_now -= timedelta(days=1)
    while vn_now.weekday() >= 5: vn_now -= timedelta(days=1)
    return vn_now.strftime("%Y-%m-%d")

def _is_after_15h() -> bool:
    vn_now = _vn_now()
    return vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 5)

def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: log_exception("[TELEGRAM]", e)

# ═══════════════════════════════════════════════════════════════
# FIX BUG #1 + #2: Helper ghi screener an toàn + invalidate cache
# ═══════════════════════════════════════════════════════════════
def _safe_replace_screener_results(results: list, tag: str = "") -> bool:
    """
    Ghi đè screener_col AN TOÀN (chống race condition).
    - Acquire screener_lock
    - Ghi vào collection tạm
    - Delete collection chính (khoảng trống rất ngắn)
    - Copy từ tạm sang chính
    - Drop collection tạm
    - Invalidate _history_cache (FIX BUG #2)
    """
    global _history_cache
    
    if not results:
        logger.warning("[SAFE-SCREENER] Danh sách rỗng, bỏ qua ghi đè.")
        return False
    
    acquired = screener_lock.acquire(timeout=120)
    if not acquired:
        logger.error("[SAFE-SCREENER] Không lấy được lock sau 120s, hủy ghi đè.")
        return False
    
    tmp_col = None
    try:
        tmp_name = f"screener_results_tmp_{int(time.time())}"
        tmp_col = db[tmp_name]
        
        tmp_col.insert_many(results)
        logger.info("[SAFE-SCREENER] Đã ghi tạm %d mã vào %s", len(results), tmp_name)
        
        screener_col.delete_many({})
        
        tmp_docs = list(tmp_col.find({}, {"_id": 0}))
        if tmp_docs:
            screener_col.insert_many(tmp_docs)
        
        logger.info(
            "[SAFE-SCREENER] Đã chuyển %d mã sang collection chính%s",
            len(tmp_docs),
            f" ({tag})" if tag else ""
        )
        
        # FIX BUG #2: Invalidate history cache
        with _history_cache["lock"]:
            _history_cache["data"] = None
            _history_cache["timestamp"] = 0
        
        return True
        
    except Exception as e:
        log_exception("[SAFE-SCREENER] Lỗi ghi screener", e)
        return False
        
    finally:
        if tmp_col is not None:
            try:
                tmp_col.drop()
            except Exception:
                pass
        screener_lock.release()

# ===================== CAGR 5 NĂM =====================
def get_historical_return_rate_with_details(symbol, history_bars=None):
    """
    Tính CAGR 5 năm. Screener truyền history_bars đã preload để tránh truy vấn MongoDB theo từng mã.
    Caller cũ không truyền dữ liệu vẫn đọc MongoDB như trước.
    """
    try:
        if history_bars is None:
            hist_data = history_col.find_one({"symbol": symbol}, {"_id": 0, "history_d1": 1})
            history_bars = hist_data.get("history_d1", []) if hist_data else []
        if not history_bars: return None
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

# ===================== BÓC TÁCH BCTC =====================
def _safe_float(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0

# FIX BUG #3: Không return item đầu tiên khi period=None; ưu tiên kỳ mới nhất
def _get_bctc_value(report_array, target_ids, period=None):
    """
    Tìm giá trị của một chỉ tiêu BCTC theo ID.
    FIX BUG #3: Nếu period=None → tự động lấy kỳ MỚI NHẤT (không lấy item đầu tiên).
    """
    if not report_array: return 0.0
    if isinstance(target_ids, str): target_ids = [target_ids]
    
    # Nếu không chỉ định kỳ → tìm kỳ mới nhất
    if period is None:
        candidate_periods = sorted(
            set(item.get("period") for item in report_array if item.get("period")),
            reverse=True
        )
        if candidate_periods:
            period = candidate_periods[0]
    
    # Ưu tiên LNST hợp nhất ISA20; ISA22 là phần phân bổ cho cổ đông công ty mẹ,
    # không được gộp thành một bản ghi LNST thứ hai.
    for target_id in target_ids:
        matches = [
            item for item in report_array
            if item.get("id") == target_id
            and (period is None or item.get("period") == period)
        ]
        if not matches:
            continue
        if target_id == "IS_NET_PROFIT_AFTER_TAX":
            non_parent = [
                item for item in matches
                if str(item.get("source_field", "")).lower() != "isa22"
                and "phân bổ cho chủ sở hữu" not in str(item.get("name", "")).lower()
                and "attributable to parent" not in str(item.get("name", "")).lower()
            ]
            isa20 = [
                item for item in non_parent
                if str(item.get("source_field", "")).lower() == "isa20"
            ]
            selected = isa20 or non_parent
            if selected:
                return _safe_float(selected[0].get("value"))
            continue
        return _safe_float(matches[0].get("value"))
    return 0.0

def extract_metrics_from_bctc_data(bctc_doc):
    if not bctc_doc: return {}

    metrics = {
        "net_profit_ttm": 0.0,
        "net_profit_ttm_periods": 0,
        "cfo_latest": 0.0,
        "cfo_ttm_periods": 0,
        "profit_growth_yoy": 0.0,
        "profit_growth_periods": 0,
        "profit_growth_yoy_available": False
    }
    income = bctc_doc.get("income_statement", [])
    cashflow = bctc_doc.get("cash_flow", [])
    balance = bctc_doc.get("balance_sheet", [])

    def _periods(rows):
        return sorted(
            {item.get("period") for item in rows if item.get("period")},
            reverse=True
        )

    periods = _periods(income)
    profit_ids = ["IS_NET_PROFIT_AFTER_TAX", "IS_PROFIT_AFTER_TAX", "IS_NET_PROFIT"]

    # Chỉ gọi là TTM khi có đủ 4 quý; không cộng 1-3 quý rồi gắn nhãn TTM.
    if len(periods) >= 4:
        npat_ttm = sum(_get_bctc_value(income, profit_ids, p) for p in periods[:4])
        metrics["net_profit_ttm"] = round(npat_ttm / 1e9, 2)
        metrics["net_profit_ttm_periods"] = 4

        # Tăng trưởng lợi nhuận TTM so với 4 quý liền trước.
        if len(periods) >= 8:
            metrics["profit_growth_periods"] = 8
            previous_ttm = sum(_get_bctc_value(income, profit_ids, p) for p in periods[4:8])
            # Không tính % tăng trưởng khi mẫu số TTM trước bằng 0/âm.
            if previous_ttm > 0:
                metrics["profit_growth_yoy"] = round(
                    (npat_ttm / previous_ttm - 1) * 100, 2
                )
                metrics["profit_growth_yoy_available"] = True
    else:
        metrics["net_profit_ttm"] = 0.0
        metrics["net_profit_ttm_periods"] = len(periods)

    cf_periods = _periods(cashflow)
    if len(cf_periods) >= 4:
        cfo_ttm = sum(
            _get_bctc_value(cashflow, "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", p)
            for p in cf_periods[:4]
        )
        metrics["cfo_latest"] = round(cfo_ttm / 1e9, 2)
        metrics["cfo_ttm_periods"] = 4
    else:
        metrics["cfo_latest"] = 0.0
        metrics["cfo_ttm_periods"] = len(cf_periods)

    latest_period = periods[0] if periods else None
    if latest_period:
        rev = _get_bctc_value(income, "IS_NET_REVENUE", latest_period)
        cogs = _get_bctc_value(income, "IS_COST_OF_GOODS_SOLD", latest_period)
        if rev > 0:
            # Giá vốn đã được chuẩn hóa âm ở bước thu thập BCTC.
            metrics["gross_margin"] = round((rev + cogs) / rev * 100, 1)

    bs_periods = _periods(balance)
    latest_bs_period = bs_periods[0] if bs_periods else None
    if latest_bs_period:
        eq = _get_bctc_value(balance, ["BS_OWNERS_EQUITY", "BS_EQUITY"], latest_bs_period)
        total_assets = _get_bctc_value(balance, "BS_TOTAL_ASSETS", latest_bs_period)
        if total_assets == 0:
            total_assets = (
                _get_bctc_value(balance, "BS_SHORT_TERM_ASSETS", latest_bs_period)
                + _get_bctc_value(balance, "BS_LONG_TERM_ASSETS", latest_bs_period)
            )

        liab = _get_bctc_value(balance, "BS_LIABILITIES", latest_bs_period)
        if liab == 0 and total_assets > 0 and eq > 0:
            liab = total_assets - eq
        if eq > 0:
            metrics["debt_to_equity"] = round(liab / eq, 2)

    return metrics

# ===================== CÀO GIÁ =====================
def _fetch_one(symbol: str) -> Optional[dict]:
    """
    Lấy snapshot giá trong đúng ngày giao dịch Việt Nam từ SSI 1 phút.
    Không fallback sang nến của ngày cũ: dữ liệu cũ không được phép đóng dấu
    thành giá realtime hoặc ghi đè trading_date hiện tại.
    """
    print(f"    [FETCH] Đang lấy giá {symbol}...", flush=True)

    def _do():
        try:
            vn_today = _vn_now().strftime("%Y-%m-%d")
            past_7d = (_vn_now() - timedelta(days=7)).strftime("%Y-%m-%d")
            df = Market().equity(symbol).ohlcv(
                start=past_7d, end=vn_today, interval="1m", source="ssi"
            )
            if df is None or df.empty:
                return None

            frame = df.copy()
            if "time" not in frame.columns:
                frame = frame.reset_index()
                if "time" not in frame.columns:
                    first_col = frame.columns[0]
                    frame = frame.rename(columns={first_col: "time"})
            if "time" not in frame.columns:
                return None

            frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
            frame = frame.dropna(subset=["time"])
            if frame.empty:
                return None

            frame["date_str"] = frame["time"].dt.strftime("%Y-%m-%d")
            today_rows = frame[frame["date_str"] == vn_today].sort_values("time")
            if today_rows.empty:
                logger.warning("[FETCH STALE] %s: SSI không có nến 1m ngày %s", symbol, vn_today)
                return None

            for col in ("open", "high", "low", "close", "volume"):
                if col not in today_rows.columns:
                    return None
                today_rows[col] = pd.to_numeric(today_rows[col], errors="coerce")
            today_rows = today_rows.dropna(subset=["open", "high", "low", "close"])
            today_rows = today_rows[
                (today_rows["open"] > 0) & (today_rows["high"] > 0) &
                (today_rows["low"] > 0) & (today_rows["close"] > 0)
            ]
            if today_rows.empty:
                return None

            first, last = today_rows.iloc[0], today_rows.iloc[-1]
            return {
                "close": float(last["close"]),
                "open": float(first["open"]),
                "high": float(today_rows["high"].max()),
                "low": float(today_rows["low"].min()),
                "volume": int(today_rows["volume"].fillna(0).clip(lower=0).sum()),
                "source_time": last["time"].strftime("%Y-%m-%d %H:%M:%S"),
                "source_date": vn_today
            }
        except Exception:
            # Để exception truyền ra ngoài để lớp retry nhận diện HTTP 429/RateLimit.
            raise

    data = None
    last_error = None
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                data = ex.submit(_do).result(timeout=25)
            last_error = None
            break
        except concurrent.futures.TimeoutError as exc:
            last_error = exc
            logger.warning("[FETCH TIMEOUT] %s quá 25s (lần %d/%d)", symbol, attempt, max_attempts)
        except Exception as exc:
            last_error = exc
            error_text = str(exc)
            is_rate_limited = (
                "RateLimitError" in type(exc).__name__
                or "VNSTOCK_SOURCE_RATE_LIMITED" in error_text
                or "429" in error_text
            )
            if not is_rate_limited or attempt >= max_attempts:
                log_exception(f"[FETCH ERROR] SSI/Vnstock {symbol}", exc)
                break

            retry_after = getattr(exc, "retry_after", None)
            if retry_after is None:
                match = re.search(r"retry_after[=:\s]+([0-9]+(?:\.[0-9]+)?)", error_text, re.IGNORECASE)
                retry_after = float(match.group(1)) if match else 1.0
            try:
                wait_seconds = max(1.0, min(float(retry_after), 8.0))
            except (TypeError, ValueError):
                wait_seconds = 1.0
            wait_seconds = min(8.0, wait_seconds + (attempt - 1))
            logger.warning(
                "[FETCH RETRY] %s SSI rate-limit 429; lần %d/%d, chờ %.1fs rồi thử lại",
                symbol, attempt, max_attempts, wait_seconds
            )
            time.sleep(wait_seconds)

    if last_error is not None and data is None:
        error_text = str(last_error)
        error_code = "VNSTOCK_SOURCE_RATE_LIMITED" if (
            "VNSTOCK_SOURCE_RATE_LIMITED" in error_text or "429" in error_text
        ) else type(last_error).__name__
        try:
            collection.update_one(
                {"symbol": symbol},
                {"$set": {
                    "is_incomplete": True,
                    "last_error_at": _now_str(),
                    "last_fetch_error": error_text[:1000],
                    "last_fetch_error_code": error_code,
                    "last_fetch_attempts": max_attempts
                }},
                upsert=True
            )
        except Exception as persist_error:
            logger.warning("[FETCH ERROR STATE] %s không lưu được trạng thái lỗi: %s", symbol, persist_error)

    if not data or data.get("close", 0) <= 0 or data.get("open", 0) <= 0:
        return None

    if data["close"] >= 1000:
        for key in ("close", "open", "high", "low"):
            data[key] /= 1000.0
    if 0 < data["volume"] < 100000:
        data["volume"] *= VOLUME_MULTIPLIER
    return data


def _fetch_history_ssi(symbol: str, start_date: str, end_date: str):
    """Fetch and normalize daily SSI candles; reject malformed/invalid rows."""
    symbol = str(symbol or "").upper().strip()
    if not re.fullmatch(r"[A-Z0-9]{2,10}", symbol):
        logger.warning("[HISTORY] Invalid symbol: %r", symbol)
        return None
    try:
        df = Market().equity(symbol).ohlcv(
            start=start_date, end=end_date, interval="1D", source="ssi"
        )
        if df is None or df.empty:
            logger.warning("[HISTORY] %s: SSI returned no candles (%s -> %s)", symbol, start_date, end_date)
            return None
        df = df.copy()
        df.columns = [str(c).lower().strip() for c in df.columns]

        # Vnstock versions may expose the timestamp as a column or DatetimeIndex.
        if "time" not in df.columns:
            date_alias = next((c for c in ("date", "datetime", "trading_date", "tradingdate") if c in df.columns), None)
            if date_alias:
                df = df.rename(columns={date_alias: "time"})
            elif isinstance(df.index, pd.DatetimeIndex) or df.index.name:
                df = df.reset_index()
                df.columns = [str(c).lower().strip() for c in df.columns]
                if "time" not in df.columns:
                    index_alias = next((c for c in ("index", "date", "datetime", "trading_date", "tradingdate") if c in df.columns), None)
                    if index_alias:
                        df = df.rename(columns={index_alias: "time"})

        required = {"time", "open", "high", "low", "close", "volume"}
        missing = required.difference(df.columns)
        if missing:
            logger.error("[HISTORY] %s: missing columns %s; got=%s", symbol, sorted(missing), list(df.columns))
            return None
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["time", "open", "high", "low", "close", "volume"])
        df = df[(df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0) & (df["volume"] >= 0)]

        # Match _fetch_one(): Mongo history prices are stored in thousand VND.
        if not df.empty and float(df["close"].median()) >= 1000:
            df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]] / 1000.0
        volume_mask = (df["volume"] > 0) & (df["volume"] < 100000)
        df.loc[volume_mask, "volume"] = df.loc[volume_mask, "volume"] * VOLUME_MULTIPLIER

        df = df[(df["high"] >= df[["open", "low", "close"]].max(axis=1)) &
                (df["low"] <= df[["open", "high", "close"]].min(axis=1))]
        df = df.drop_duplicates(subset=["time"], keep="last").sort_values("time")
        return df.reset_index(drop=True) if not df.empty else None
    except Exception:
        logger.exception("[HISTORY] SSI fetch failed for %s (%s -> %s)", symbol, start_date, end_date)
        return None

# ===================== CÀO BCTC VCI =====================
def _fetch_bctc_ssi(symbol: str):
    symbol = str(symbol).upper().strip()
    if not symbol or not re.fullmatch(r"[A-Z0-9]{2,10}", symbol):
        logger.warning("[BCTC VCI] Mã cổ phiếu không hợp lệ: %r", symbol)
        return None

    CORE_IDS = {
        "IS_NET_REVENUE", "IS_COST_OF_GOODS_SOLD", "IS_NET_PROFIT_AFTER_TAX",
        "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", "BS_TOTAL_ASSETS",
        "BS_SHORT_TERM_ASSETS", "BS_LONG_TERM_ASSETS", "BS_OWNERS_EQUITY",
        "BS_LIABILITIES"
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
            
            # ISA22 là LNST phân bổ cho cổ đông công ty mẹ, không phải
            # một bản ghi LNST hợp nhất độc lập; loại khỏi metric tổng LNST.
            if field == "ISA22" or "loi nhuan sau thue phan bo cho chu so huu" in label or "attributable to parent" in label:
                return None
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


# ===================== RATIO VNDIRECT =====================
VNDIRECT_RATIOS = {
    "pe": "PRICE_TO_EARNINGS",
    "pb": "PRICE_TO_BOOK",
    
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

# ===================== CORE CRAWL =====================
# FIX BUG #8: Preload documents 1 lần thay vì find_one 2 lần/symbol
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling, LAST_TELEGRAM_TIME, stop_crawl_requested, _history_cache
    
    if is_crawling: return
    is_crawling = True
    stop_crawl_requested = False
    realtime_job_type = "Cào bù" if is_retry else ("Cào tay" if is_manual else "Cào REALTIME")
    _realtime_progress_update(status="running", job_type=realtime_job_type,
        message="Đang khởi tạo phiên cào", current_symbol=None,
        completed=0, total=len(symbols), success=0, failed=0)

    print(f"\n[CRAWL] 🚀 [{'CÀO BÙ' if is_retry else ('CÀO TAY' if is_manual else 'REAL-TIME')}] Bắt đầu xử lý {len(symbols)} mã...", flush=True)
    try:
        vn_now = _vn_now()
        real_today = vn_now.strftime("%Y-%m-%d")
        current_hour = vn_now.hour
        current_minute = vn_now.minute

        is_closing_time = (current_hour > 15) or (current_hour == 15 and current_minute >= 5)

        alert_batch, pending_history_files = [], {}

        # Preload snapshot MongoDB trước khi gọi SSI; ghi log riêng để không còn
        # khoảng trống log khiến tưởng worker đã dừng sau dòng [CRAWL].
        preload_started = time.perf_counter()
        print(f"[CRAWL] 📥 Đang nạp snapshot giá MongoDB cho {len(symbols)} mã...", flush=True)

        def _load_symbol_docs_in_batches(col, projection, stage_name, batch_size=10):
            loaded = {}
            for batch_start in range(0, len(symbols), batch_size):
                if stop_crawl_requested:
                    break
                batch = symbols[batch_start:batch_start + batch_size]
                last_error = None
                for attempt in range(2):
                    try:
                        cursor = col.find(
                            {"symbol": {"$in": batch}},
                            projection,
                            max_time_ms=45000
                        ).batch_size(batch_size)
                        for item in cursor:
                            if item.get("symbol"):
                                loaded[item["symbol"]] = item
                        last_error = None
                        break
                    except Exception as read_error:
                        last_error = read_error
                        print(
                            f"[CRAWL] ⚠️ {stage_name}: lỗi batch "
                            f"{batch_start + 1}-{batch_start + len(batch)} "
                            f"(lần {attempt + 1}/2): {read_error}",
                            flush=True
                        )
                        if attempt == 0:
                            time.sleep(1.5)
                if last_error is not None:
                    raise RuntimeError(
                        f"{stage_name} thất bại tại batch "
                        f"{batch_start + 1}-{batch_start + len(batch)}: {last_error}"
                    ) from last_error
                done = min(batch_start + len(batch), len(symbols))
                print(
                    f"[CRAWL] 📦 {stage_name}: {done}/{len(symbols)} mã "
                    f"(đã có {len(loaded)} bản ghi)",
                    flush=True
                )
            return loaded

        preloaded_docs = _load_symbol_docs_in_batches(
            collection,
            {"symbol": 1, "close": 1, "prev_close": 1, "is_locked": 1,
             "trading_date": 1, "open": 1, "high": 1, "low": 1},
            "snapshot giá"
        )
        print(
            f"[CRAWL] ✅ Đã nạp {len(preloaded_docs)}/{len(symbols)} snapshot giá "
            f"sau {time.perf_counter() - preload_started:.1f}s; đang nạp lịch sử D1...",
            flush=True
        )

        history_started = time.perf_counter()
        # Chỉ đọc 21 nến gần nhất và chia batch nhỏ để tránh một lệnh $in lớn
        # làm MongoDB Atlas timeout khi collection đang chịu tải.
        preloaded_history = _load_symbol_docs_in_batches(
            history_col,
            {"symbol": 1, "history_d1": {"$slice": -21}},
            "lịch sử D1"
        )
        print(
            f"[CRAWL] ✅ Đã nạp lịch sử gần nhất của {len(preloaded_history)}/{len(symbols)} mã "
            f"sau {time.perf_counter() - history_started:.1f}s; bắt đầu gọi SSI...",
            flush=True
        )

        for chunk_idx in range(0, len(symbols), 5):
            if stop_crawl_requested:
                print("[CRAWL] 🛑 Đã nhận lệnh dừng khẩn cấp!", flush=True)
                break

            chunk = symbols[chunk_idx:chunk_idx+5]
            for sym in chunk:
                if stop_crawl_requested: break

                _realtime_progress_update(current_symbol=sym, message=f"Đang cào {sym}")
                doc = preloaded_docs.get(sym)
                data = _fetch_one(sym)
                progress_now = _realtime_progress_snapshot()
                _realtime_progress_update(
                    completed=progress_now["completed"] + 1,
                    success=progress_now["success"] + (1 if data else 0),
                    failed=progress_now["failed"] + (0 if data else 1),
                    message=f"Đã xử lý {sym}"
                )
                
                if data:
                    hist_doc = preloaded_history.get(sym)
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

                    # Dùng prev_close đã lưu làm dự phòng; không lấy close cũ
                    # làm giá tham chiếu vì có thể tạo phần trăm biến động sai.
                    if prev_close <= 0 and doc:
                        prev_close = float(doc.get("prev_close", 0) or 0)

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

                    today_record = doc if (doc and doc.get("trading_date") == real_today) else None
                    
                    if today_record:
                        final_open = today_record.get("open", data["open"])
                        final_high = max(today_record.get("high", data["high"]), data["high"])
                        final_low = min(today_record.get("low", data["low"]), data["low"])
                    else:
                        final_open = data["open"]
                        final_high = data["high"]
                        final_low = data["low"]

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
                    }, "$unset": {
                        "last_fetch_error": "", "last_fetch_error_code": "",
                        "last_fetch_attempts": "", "last_error_at": ""
                    }}, upsert=True)
                    
                    print(f"  ✅ [OK] {sym}: Giá {data['close']:,} (O:{final_open} H:{final_high} L:{final_low}) | KL: {data['volume']:,} | Vol%: {vol_pct}%", flush=True)
                    
                    if prev_close > 0 and abs(pct_change) >= 3.0 and vol_pct >= 120.0:
                        alert_batch.append({"sym": sym, "pct_change": pct_change, "volume_percent": vol_pct, "trend": "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG"})
                        
                    if is_closing_time:
                        # CHỐT NẾN D1: không phụ thuộc volume/is_locked.
                        # API giá có thể trả volume=0 hoặc giá không đổi nhưng OHLC vẫn hợp lệ.
                        try:
                            bar_today = {
                                "time": real_today,
                                "open": float(final_open or 0),
                                "high": float(final_high or 0),
                                "low": float(final_low or 0),
                                "close": float(data.get("close", 0) or 0),
                                "volume": int(data.get("volume", 0) or 0)
                            }
                            valid_bar = (
                                bar_today["close"] > 0
                                and bar_today["open"] > 0
                                and bar_today["high"] > 0
                                and bar_today["low"] > 0
                                and bar_today["high"] >= max(bar_today["open"], bar_today["close"], bar_today["low"])
                                and bar_today["low"] <= min(bar_today["open"], bar_today["close"], bar_today["high"])
                            )
                            if not valid_bar:
                                raise ValueError(
                                    f"OHLC không hợp lệ: O={bar_today['open']} "
                                    f"H={bar_today['high']} L={bar_today['low']} C={bar_today['close']}"
                                )

                            with _history_write_lock:
                                # Re-read under the shared lock; the preloaded snapshot may be stale
                                # if a history backfill updated this symbol during the realtime crawl.
                                current_hist_doc = history_col.find_one(
                                    {"symbol": sym}, {"_id": 0, "history_d1": 1}
                                ) or {"symbol": sym}
                                d1_bars = list(current_hist_doc.get("history_d1", []) or [])
                                d1_bars = [b for b in d1_bars if b.get("time") != real_today]
                                d1_bars.append(bar_today)
                                d1_bars.sort(key=lambda x: x.get("time", ""))

                                # Ghi Mongo trước; chỉ đánh dấu đã chốt sau khi đọc lại xác nhận.
                                write_result = history_col.update_one(
                                    {"symbol": sym},
                                    {"$set": {"history_d1": d1_bars}},
                                    upsert=True
                                )
                                saved_doc = history_col.find_one(
                                    {"symbol": sym, "history_d1.time": real_today},
                                    {"_id": 0, "symbol": 1, "history_d1": {"$elemMatch": {"time": real_today}}}
                                )
                                saved_bars = saved_doc.get("history_d1", []) if saved_doc else []
                                if not write_result.acknowledged or not saved_bars:
                                    raise RuntimeError("MongoDB chưa xác nhận lưu nến D1")

                                current_hist_doc["history_d1"] = d1_bars
                                pending_history_files[f"history/{sym}.json"] = current_hist_doc

                            # Chỉ chốt trạng thái sau khi nến đã tồn tại trong history_data.
                            collection.update_one(
                                {"symbol": sym},
                                {"$set": {"is_locked": True, "chot_so_today": real_today, "is_incomplete": False},
                                 "$unset": {"history_save_error": "", "history_save_error_at": ""}}
                            )
                            print(
                                f"  💾 [D1 SAVED] {sym} {real_today} | "
                                f"O:{bar_today['open']} H:{bar_today['high']} "
                                f"L:{bar_today['low']} C:{bar_today['close']} "
                                f"V:{bar_today['volume']}",
                                flush=True
                            )
                        except Exception as history_error:
                            # Không đánh dấu chốt: worker có thể thử lại, tránh mất nến âm thầm.
                            collection.update_one(
                                {"symbol": sym},
                                {"$set": {"is_incomplete": True, "history_save_error": str(history_error),
                                          "history_save_error_at": _now_str()}},
                                upsert=True
                            )
                            print(f"  ❌ [D1 SAVE FAILED] {sym}: {history_error}", flush=True)
                else:
                    print(f"  ❌ [LỖI] {sym}: Không lấy được dữ liệu.", flush=True)
                    collection.update_one({"symbol": sym}, {"$set": {"is_incomplete": True, "last_error_at": _now_str()}}, upsert=True)
            time.sleep(1)

        with _history_cache["lock"]:
            _history_cache["data"] = None
            _history_cache["timestamp"] = 0

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
        stopped = bool(stop_crawl_requested)
        _realtime_progress_update(
            status="stopped" if stopped else "completed",
            current_symbol=None,
            message="Đã dừng theo yêu cầu" if stopped else "Đã hoàn tất phiên cào"
        )
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

# FIX BUG #5: normalize_stock_data không ghi DB, trả update_fields cho caller
def normalize_stock_data(symbol, data):
    """
    Chuẩn hóa dữ liệu. KHÔNG ghi DB (fix bug #5).
    Trả về dict normalized. Caller tự quyết định ghi DB nếu cần.
    """
    close_price = float(data.get("close", data.get("close_price", 0)) or 0)
    pe, pb = float(data.get("pe", 0) or 0), float(data.get("pb", 0) or 0)
    roe, roa, eps = float(data.get("roe", 0) or 0), float(data.get("roa", 0) or 0), float(data.get("eps", 0) or 0)
    
    eps_need_update = False
    update_fields = {}

    if eps >= 1000:
        eps = eps / 1000.0
        eps_need_update = True
    elif close_price > 0 and eps > (close_price * 5): 
        eps = eps / 1000.0
        eps_need_update = True
    if eps_need_update: 
        update_fields["eps"] = eps

    if update_fields:
        print(f"    [CHUẨN HÓA] {symbol}: Sửa lỗi đơn vị {list(update_fields.keys())} -> {update_fields}", flush=True)

    # Growth được tính từ chuỗi BCTC theo kỳ, không đọc nhầm field không tồn tại
    # trên từng dòng income_statement.
    profit_growth_yoy = float(data.get("profit_growth_yoy", 0) or 0)
    bvps = (close_price / pb) if pb > 0 and close_price > 0 else 0
    
    if eps == 0 and pe > 0 and close_price > 0: 
        eps = close_price / pe
    
    return {
        "close_price": close_price, "pe": pe, "pb": pb, "roe": roe, "roa": roa, "eps": eps, "bvps": bvps,
        "dividend_yield": float(data.get("dividend_yield", 0)), "cfo_latest": float(data.get("cfo_latest", 0)),
        "net_profit_ttm": float(data.get("net_profit_ttm", 0)),
        "net_profit_ttm_periods": int(data.get("net_profit_ttm_periods", 0) or 0),
        "profit_growth_periods": int(data.get("profit_growth_periods", 0) or 0),
        "profit_growth_yoy_available": bool(data.get("profit_growth_yoy_available", False)),
        "cfo_ttm_periods": int(data.get("cfo_ttm_periods", 0) or 0),
        "debt_to_equity": float(data.get("debt_to_equity", 0)),
        "gross_margin": float(data.get("gross_margin", 0)), "profit_growth_yoy": profit_growth_yoy,
        "_update_fields": update_fields  # Caller dùng để ghi DB nếu cần
    }
def compute_sector_medians(normalized_db):
    """
    Trung vị đồng ngành chỉ được công nhận khi có ít nhất 3 DOANH NGHIỆP
    độc lập. P/E, P/B và ROE được tính mẫu riêng; không cộng số metric
    để biến một doanh nghiệp thành nhiều quan sát.
    """
    buckets = {}
    for symbol, nd in normalized_db.items():
        for key in (get_sub_sector(symbol), get_sector(symbol)):
            b = buckets.setdefault(key, {"pe": [], "pb": [], "roe": [], "symbols": set()})
            b["symbols"].add(symbol)
            pe = float(nd.get("pe", 0) or 0)
            pb = float(nd.get("pb", 0) or 0)
            roe = float(nd.get("roe", 0) or 0)
            # Loại multiples phi thực tế để tránh làm lệch trung vị.
            if 0 < pe <= 100: b["pe"].append(pe)
            if 0 < pb <= 20: b["pb"].append(pb)
            if -100 <= roe <= 100: b["roe"].append(roe)

    medians = {}
    for key, b in buckets.items():
        peer_count = len(b["symbols"])
        if peer_count < 3:
            continue
        medians[key] = {
            "pe": float(np.median(b["pe"])) if len(b["pe"]) >= 3 else 0.0,
            "pb": float(np.median(b["pb"])) if len(b["pb"]) >= 3 else 0.0,
            "roe": float(np.median(b["roe"])) if len(b["roe"]) >= 3 else 0.0,
            "n": peer_count,
            "pe_n": len(b["pe"]),
            "pb_n": len(b["pb"]),
            "roe_n": len(b["roe"])
        }
    return medians

def get_median_for_symbol(symbol, sector_medians):
    sub, sector = get_sub_sector(symbol), get_sector(symbol)
    if sub in sector_medians and sector_medians[sub].get("n", 0) >= 3:
        return sector_medians[sub], sub
    if sector in sector_medians and sector_medians[sector].get("n", 0) >= 3:
        return sector_medians[sector], sector
    # Không tự tạo P/E, P/B giả định khi không có đủ peer thực tế.
    return {"pe": 0.0, "pb": 0.0, "roe": 0.0, "n": 0, "pe_n": 0, "pb_n": 0, "roe_n": 0}, sector

def compute_fair_value_zone(symbol, nd, sector_median, config, group_label):
    eps, bvps, close = nd["eps"], nd["bvps"], nd["close_price"]
    cfo, npat = nd["cfo_latest"], nd["net_profit_ttm"]
    roe = nd.get("roe", 0)
    methods = {}
    med_pe = float(sector_median.get("pe", 0) or 0)
    med_pb = float(sector_median.get("pb", 0) or 0)
    med_roe = float(sector_median.get("roe", 0) or 0)
    cap_pe, cap_pb, mos = config["cap_pe"], config["cap_pb"], config["mos"]
    is_financial = group_label in ("NGAN_HANG", "CHUNG_KHOAN", "INSURANCE")
    primary_metric = str(config.get("primary_metric", "PE")).upper()
    peer_count = int(sector_median.get("n", 0) or 0)

    # Không phát hành Fair Value khi thiếu giá, thiếu mẫu peer độc lập,
    # hoặc thiếu chính chỉ tiêu định giá chủ đạo của ngành.
    primary_available = (
        (primary_metric == "PE" and eps > 0 and med_pe > 0)
        or (primary_metric == "PB" and bvps > 0 and med_pb > 0)
    )
    if close <= 0 or peer_count < 3 or not primary_available:
        missing_reason = (
            "Thiếu giá hợp lệ" if close <= 0 else
            "Chưa đủ 3 doanh nghiệp đồng ngành có dữ liệu so sánh" if peer_count < 3 else
            f"Thiếu dữ liệu {primary_metric} chủ đạo"
        )
        return {
            "fair_low": 0, "fair_high": 0, "graham": 0, "methods": {},
            "mos_pct": round(mos * 100, 1), "rationale": missing_reason,
            "discount_to_fair_pct": 0, "verdict": "KHÔNG ĐỦ DỮ LIỆU"
        }

    if eps > 0 and med_pe > 0: 
        roe_bonus = 1.15 if (roe > 0 and roe > med_roe) else 1.0
        methods["PE_method"] = round(eps * min(med_pe * roe_bonus, cap_pe), 0)
    if bvps > 0 and med_pb > 0: 
        methods["PB_method"] = round(bvps * min(med_pb, cap_pb), 0)
    if not is_financial and eps > 0 and bvps > 0: 
        methods["Graham"] = round((22.5 * eps * bvps) ** 0.5, 0)
    if not is_financial and npat > 0 and cfo > 0 and (cfo / npat) >= 0.8 and eps > 0 and med_pe > 0: 
        methods["CFO_boost"] = round(eps * min(med_pe, cap_pe) * 1.10, 0)
            
    valid_methods = {}
    for k, v in methods.items():
        if v > 0 and close > 0:
            if (v / close) >= 0.2 and (v / close) <= 3.0:
                valid_methods[k] = v
            else:
                print(f"    [CẢNH BÁO] Loại bỏ PP {k} của {symbol} do lệch quá lớn: {v} (Giá hiện tại: {close})", flush=True)

    if not valid_methods: 
        return {"fair_low": 0, "fair_high": 0, "graham": 0, "methods": {}, "mos_pct": round(mos * 100, 1), "rationale": "Không đủ DL.", "discount_to_fair_pct": 0, "verdict": "KHÔNG ĐỦ DỮ LIỆU"}
    
    weights = {"PE_method": 2, "PB_method": 1.5, "Graham": 2, "CFO_boost": 1.5}
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

def compute_fundamental_score(symbol, nd):
    """Tính điểm cơ bản 0-50 từ ratio hiện có và metric BCTC đã xác thực."""
    sector = get_sector(symbol)
    roe = float(nd.get("roe", 0) or 0)
    roa = float(nd.get("roa", 0) or 0)
    cfo_latest = float(nd.get("cfo_latest", 0) or 0)
    net_profit_ttm = float(nd.get("net_profit_ttm", 0) or 0)
    debt_to_equity = float(nd.get("debt_to_equity", 0) or 0)
    profit_growth_yoy = float(nd.get("profit_growth_yoy", 0) or 0)
    profit_growth_available = bool(nd.get("profit_growth_yoy_available", False))
    cfo_ttm_available = nd.get("cfo_ttm_periods", 0) >= 4
    dividend_yield = float(nd.get("dividend_yield", 0) or 0)
    fund_score = 0
    fund_notes = []
    
    if sector == "NGAN_HANG":
        # Chỉ dùng tăng trưởng LNST tính từ 8 kỳ BCTC đã lưu; không giả định
        # chỉ tiêu dư nợ/tín dụng khi MongoDB không có trường đó.
        if profit_growth_available:
            if profit_growth_yoy >= 20: fund_score += 18; fund_notes.append("LNST TTM tăng mạnh")
            elif profit_growth_yoy >= 10: fund_score += 10
            elif profit_growth_yoy < 0: fund_score -= 8; fund_notes.append("LNST TTM suy giảm")
        else:
            fund_notes.append("Thiếu 8 quý BCTC để xác nhận tăng trưởng LNST")
        if roe >= 18: fund_score += 17; fund_notes.append("ROE >18%")
        elif roe >= 14: fund_score += 10
    elif sector == "CHUNG_KHOAN":
        if profit_growth_available:
            if profit_growth_yoy >= 20: fund_score += 20; fund_notes.append("LN tăng mạnh")
            elif profit_growth_yoy >= 10: fund_score += 10
        if roe >= 15: fund_score += 15; fund_notes.append("ROE cao")
        elif roe >= 10: fund_score += 8
    elif sector == "BAT_DONG_SAN":
        if profit_growth_available and profit_growth_yoy >= 10: fund_score += 15
        if roe >= 12: fund_score += 20; fund_notes.append("ROE chuẩn")
        elif roe > 0: fund_score += 10
    elif sector == "CONG_NGHE_BAN_LE":
        if profit_growth_available:
            if profit_growth_yoy >= 20: fund_score += 18; fund_notes.append("Tăng trưởng lõi")
            elif profit_growth_yoy >= 10: fund_score += 10
        if roe >= 18: fund_score += 17
        elif roe >= 12: fund_score += 10
    elif sector == "DIEN_NUOC_HA_TANG":
        if dividend_yield >= 6.0: fund_score += 20; fund_notes.append("Cổ tức cao")
        elif dividend_yield > 0: fund_score += 10
        if roe >= 10: fund_score += 15
        else: fund_score += 8
    elif sector == "NGUYEN_VAT_LIEU_CHU_KY":
        if profit_growth_available:
            if profit_growth_yoy >= 15: fund_score += 18
            elif profit_growth_yoy >= 0: fund_score += 10
            else: fund_score -= 15; fund_notes.append("Suy thoái chu kỳ")
        if roe >= 12: fund_score += 17
        else: fund_score += 8
    else:
        if profit_growth_available:
            if profit_growth_yoy >= 10: fund_score += 15
            elif profit_growth_yoy >= 0: fund_score += 8
            else: fund_score -= 10
        if roe >= 14: fund_score += 20; fund_notes.append("ROE cao")
        elif roe >= 10: fund_score += 10
        
    if roa >= 5.0: fund_score += 5
    elif roa >= 2.5: fund_score += 3

    if sector not in ["NGAN_HANG", "CHUNG_KHOAN"]:
        if cfo_ttm_available:
            if cfo_latest > 0 and (net_profit_ttm <= 0 or cfo_latest >= net_profit_ttm * 0.8): fund_score += 5
            elif cfo_latest < 0: fund_score -= 5; fund_notes.append("CFO âm")
        if 0 < debt_to_equity <= 1.0: fund_score += 5
        elif debt_to_equity > 2.5: fund_score -= 5; fund_notes.append("D/E cao")
    else:
        fund_score += 5
    fund_score = int(np.clip(fund_score, 0, 50))

    return fund_score, fund_notes

def evaluate_stock_from_db(symbol, nd, sector_median, group_label, config, tech_score_raw=50, tech_status="Tích lũy", pattern="Tích lũy", existing_ai_data=None, history_bars=None):
    sector = get_sector(symbol)
    pe, pb, roe, roa = nd["pe"], nd["pb"], nd["roe"], nd["roa"]
    close_price, cfo_latest, net_profit_ttm = nd["close_price"], nd["cfo_latest"], nd["net_profit_ttm"]
    debt_to_equity, profit_growth_yoy = nd["debt_to_equity"], nd["profit_growth_yoy"]
    profit_growth_available = bool(nd.get("profit_growth_yoy_available", False))
    cfo_ttm_available = nd.get("cfo_ttm_periods", 0) >= 4
    
    data_warnings = []
    primary_metric = str(config.get("primary_metric", "PE")).upper()
    if close_price <= 0:
        data_warnings.append("Thiếu Giá")
    if primary_metric == "PE" and pe <= 0:
        data_warnings.append("Thiếu P/E chủ đạo")
    elif primary_metric == "PB" and pb <= 0:
        data_warnings.append("Thiếu P/B chủ đạo")
    if roe == 0:
        data_warnings.append("Thiếu ROE")
    if nd.get("net_profit_ttm_periods", 0) < 4:
        data_warnings.append("BCTC chưa đủ 4 quý để tính LNST TTM")
    if not profit_growth_available:
        data_warnings.append("Chưa đủ điều kiện tính tăng trưởng LNST TTM (cần 8 quý và TTM kỳ trước dương)")
    if nd.get("cfo_ttm_periods", 0) < 4:
        data_warnings.append("BCTC chưa đủ 4 quý để tính CFO TTM")
    is_data_incomplete = bool(data_warnings)

    fund_score, fund_notes = compute_fundamental_score(symbol, nd)
    
    # Điểm định giá dựa trên mức chiết khấu so với trung vị peer cùng ngành.
    # Thiếu mẫu peer/metric không được chấm điểm mặc định.
    val_score = 0
    primary_metric = str(config.get("primary_metric", "PE")).upper()
    peer_count = int(sector_median.get("n", 0) or 0)
    primary_value = pb if primary_metric == "PB" else pe
    peer_value = float(sector_median.get(primary_metric.lower(), 0) or 0)
    relative_multiple = None
    if peer_count >= 3 and primary_value > 0 and peer_value > 0:
        relative_multiple = primary_value / peer_value
        if relative_multiple <= 0.70:
            val_score = 30
        elif relative_multiple <= 0.85:
            val_score = 26
        elif relative_multiple <= 1.00:
            val_score = 22
        elif relative_multiple <= 1.15:
            val_score = 16
        elif relative_multiple <= 1.35:
            val_score = 9
        else:
            val_score = 3

        # Chỉ cộng nhẹ cho cổ tức bền vững ở nhóm không lấy P/B làm chỉ tiêu chính.
        dividend_yield = float(nd.get("dividend_yield", 0) or 0)
        if primary_metric == "PE" and dividend_yield >= 5.0:
            val_score = min(30, val_score + 2)
    else:
        data_warnings.append(
            f"Thiếu mẫu peer hoặc {primary_metric} hợp lệ; điểm định giá = 0"
        )
    val_score = int(np.clip(val_score, 0, 30))
    # Cập nhật trạng thái đầy đủ dữ liệu sau khi đã kiểm tra định giá.
    is_data_incomplete = bool(data_warnings)

    tech_score = int(np.clip(int(round((tech_score_raw / 100.0) * 20)), 0, 20))
    total_score = fund_score + val_score + tech_score
    potential_zone = "VÙNG TÍCH SẢN TỐI ƯU (A+)" if total_score >= 75 else ("VÙNG MUA AN TOÀN (B)" if total_score >= 60 else "THEO DÕI THÊM")
    
    fv = compute_fair_value_zone(symbol, nd, sector_median, config, group_label)
    decision, color, reason = compute_final_decision(fund_score, tech_score, val_score, total_score, fv["verdict"], cfo_latest, net_profit_ttm, roe, debt_to_equity, fv["discount_to_fair_pct"], close_price, fv["fair_high"], fv["fair_low"])

    sim_data = get_historical_return_rate_with_details(symbol, history_bars)
    meta = VN100_METADATA.get(symbol, {})

    ai_data = existing_ai_data or {
        "bctc": "", "catalyst": "", "risks": "", "outlook": "", "recommendation": "", 
        "price_zone": "", "macro_view": "", "ai_updated_at": "Chưa phân tích", "ai_source": ""
    }
    
    has_real_ai = bool(ai_data.get("bctc") and len(ai_data.get("bctc", "")) > 100)
    
    if not has_real_ai:
        metrics_for_template = {
            **nd, "fair_value_low": fv["fair_low"], "fair_value_high": fv["fair_high"],
            "valuation_verdict": fv["verdict"], "discount_to_fair_pct": fv["discount_to_fair_pct"],
            "margin_of_safety_pct": fv["mos_pct"]
        }
        temp_template = build_fallback_narrative(symbol, metrics_for_template, {
            "fundamental_score": fund_score, "valuation_score": val_score, "total_score": total_score,
            "profit_growth_yoy": profit_growth_yoy, "final_decision": decision
        }, sector)
        
        ai_data.update({
            "bctc": temp_template.get("bctc", ""), "catalyst": temp_template.get("catalyst", ""),
            "risks": temp_template.get("risks", ""), "outlook": temp_template.get("outlook", ""),
            "recommendation": temp_template.get("recommendation", ""), "price_zone": temp_template.get("price_zone", ""),
            "macro_view": temp_template.get("macro_view", ""), "ai_source": "TEMPLATE"
        })

    return {
        "symbol": symbol, "sector": sector, "sub_sector": group_label, "close_price": close_price, "pe": pe, "pb": pb,
        "roe": roe, "roa": roa, "eps": float(nd["eps"] or 0), "bvps": float(nd["bvps"] or 0), "dividend_yield": nd["dividend_yield"], "profit_growth_yoy": profit_growth_yoy,
        "profit_growth_periods": nd.get("profit_growth_periods", 0),
        "profit_growth_yoy_available": profit_growth_available,
        "cfo_latest": cfo_latest, "net_profit_ttm": net_profit_ttm, "debt_to_equity": debt_to_equity, "gross_margin": nd["gross_margin"],
        "fundamental_score": fund_score, "valuation_score": val_score, "technical_score": tech_score, "total_score": total_score,
        "valuation_primary_metric": primary_metric,
        "valuation_peer_count": peer_count,
        "valuation_relative_multiple": round(relative_multiple, 3) if relative_multiple is not None else None,
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

def build_bctc_quarterly_trend(bctc_doc, max_periods=8):
    """Tổng hợp xu hướng BCTC tối đa 8 quý; không tự suy diễn số liệu thiếu."""
    if not isinstance(bctc_doc, dict):
        return "Không có dữ liệu BCTC theo quý."

    income = bctc_doc.get("income_statement", []) or []
    cashflow = bctc_doc.get("cash_flow", []) or []
    periods = sorted(
        {x.get("period") for x in income if x.get("period")},
        reverse=True
    )[:max_periods]
    if not periods:
        return "Không có dữ liệu doanh thu/lợi nhuận theo quý."

    rows = {}
    for period in periods:
        revenue = _get_bctc_value(income, "IS_NET_REVENUE", period)
        cogs = _get_bctc_value(income, "IS_COST_OF_GOODS_SOLD", period)
        npat = _get_bctc_value(
            income,
            ["IS_NET_PROFIT_AFTER_TAX", "IS_PROFIT_AFTER_TAX", "IS_NET_PROFIT"],
            period
        )
        cfo = _get_bctc_value(
            cashflow, "CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", period
        )
        rows[period] = {
            "revenue": revenue, "npat": npat, "cfo": cfo,
            "gross_margin": ((revenue + cogs) / revenue * 100) if revenue > 0 else None
        }

    def growth(current, previous):
        if previous is None or previous == 0:
            return None
        return (current / previous - 1) * 100

    def fmt(value):
        return "không đủ dữ liệu" if value is None else f"{value:+.1f}%"

    latest_period = periods[0]
    expected_period = _expected_latest_bctc_period()
    lines = [
        f"XU HƯỚNG BCTC THEO QUÝ (tối đa {len(periods)} quý; đơn vị tỷ VNĐ):",
        f"Kỳ dữ liệu mới nhất: {latest_period}; kỳ công bố dự kiến hiện tại: {expected_period}."
    ]
    if latest_period < expected_period:
        lines.append(
            f"CẢNH BÁO: Dữ liệu BCTC đang cũ ({latest_period}), chưa có dữ liệu đến "
            f"kỳ dự kiến {expected_period}. Không được trình bày các số liệu này như "
            "tình hình tài chính mới nhất tại thời điểm hiện tại."
        )
    for period in reversed(periods):
        item = rows[period]
        try:
            year, quarter = period.split("-Q")
            prior_yoy = f"{int(year)-1}-Q{quarter}"
            prior_q = f"{int(year)-1}-Q4" if quarter == "1" else f"{year}-Q{int(quarter)-1}"
        except Exception:
            prior_yoy, prior_q = None, None

        yoy = rows.get(prior_yoy, {})
        prev = rows.get(prior_q, {})
        gm = "N/A" if item["gross_margin"] is None else f"{item['gross_margin']:.1f}%"
        lines.append(
            f"- {period}: Doanh thu {item['revenue']/1e9:,.2f}; "
            f"LNST {item['npat']/1e9:,.2f}; CFO {item['cfo']/1e9:,.2f}; "
            f"biên gộp {gm}; doanh thu YoY {fmt(growth(item['revenue'], yoy.get('revenue')) if yoy else None)}; "
            f"LNST YoY {fmt(growth(item['npat'], yoy.get('npat')) if yoy else None)}; "
            f"doanh thu QoQ {fmt(growth(item['revenue'], prev.get('revenue')) if prev else None)}; "
            f"LNST QoQ {fmt(growth(item['npat'], prev.get('npat')) if prev else None)}."
        )

    lines.append(
        "LƯU Ý: So sánh chỉ khi có đủ kỳ tương ứng; không suy diễn dữ liệu thiếu "
        "hoặc kết luận chắc chắn khi các kỳ báo cáo không đồng nhất."
    )
    return "\n".join(lines)


def build_verified_facts(symbol, sm, si, sector, bctc_doc=None):
    bctc_doc = bctc_doc if isinstance(bctc_doc, dict) else {}
    growth_available = bool(sm.get("profit_growth_yoy_available", False))
    growth_periods = int(sm.get("profit_growth_periods", 0) or 0)
    warnings = sm.get("data_warnings", []) or []
    if isinstance(warnings, str):
        warnings = [warnings]

    lines = [
        f"MÃ {symbol} — NGÀNH {sector}",
        f"QUYẾT ĐỊNH: {si.get('final_decision')}",
        f"GIÁ: {sm.get('close', sm.get('close_price', 0)):,.0f} | P/E {sm.get('pe',0):.2f} | P/B {sm.get('pb',0):.2f}",
        f"ĐỊNH GIÁ: Fair High {sm.get('fair_value_high',0):,.0f} | Fair Low {sm.get('fair_value_low',0):,.0f} | Verdict: {sm.get('valuation_verdict')}",
        f"SINH LỜI: ROE {sm.get('roe',0)}% | ROA {sm.get('roa',0)}% | Biên gộp {sm.get('gross_margin',0)}% | D/E {sm.get('debt_to_equity',0)}",
        f"DÒNG TIỀN/LỢI NHUẬN: CFO quý gần nhất {sm.get('cfo_latest',0):,.0f} tỷ | LNST TTM {sm.get('net_profit_ttm',0):,.0f} tỷ",
        f"TĂNG TRƯỞNG LNST TTM YoY: {'ĐỦ ĐIỀU KIỆN' if growth_available else 'CHƯA ĐỦ ĐIỀU KIỆN'} | Số kỳ BCTC: {growth_periods} | Giá trị: {sm.get('profit_growth_yoy', 'không có')}",
        f"MẪU ĐỊNH GIÁ: {sm.get('valuation_primary_metric', 'không có')} | Số doanh nghiệp peer: {sm.get('valuation_peer_count', 0)}",
        f"ĐIỂM: Cơ bản {si.get('fundamental_score')}/50 | Định giá {si.get('valuation_score')}/30 | Kỹ thuật {si.get('technical_score')}/20 | Tổng {si.get('total_score')}/100",
    ]
    if warnings:
        lines.append("CẢNH BÁO DỮ LIỆU: " + " | ".join(str(x) for x in warnings))
    lines.append(build_bctc_quarterly_trend(bctc_doc))
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

    if cfo != 0:
        bctc_parts.append(
            f"CFO quý gần nhất là {cfo:,.0f} tỷ VNĐ; không so sánh trực tiếp với LNST TTM "
            "vì khác kỳ báo cáo. Cần dùng CFO TTM cùng kỳ để đánh giá chất lượng lợi nhuận."
        )

    bctc_parts.append(f"EPS đạt {eps:,.0f} VNĐ.")
    if div_yield > 0: bctc_parts.append(f"Cổ tức {div_yield}%/năm.")
    bctc = " ".join(bctc_parts)

    growth_available = bool(
        screener_item.get("profit_growth_yoy_available",
                          stock_metrics.get("profit_growth_yoy_available", False))
    )
    if growth_available:
        catalyst = f"LNST TTM tăng trưởng {growth_yoy:.1f}% YoY theo dữ liệu BCTC; cần tiếp tục đối chiếu doanh thu và dòng tiền để xác nhận chất lượng tăng trưởng. Chưa có dữ liệu đủ để xác nhận lợi thế cạnh tranh riêng của doanh nghiệp."
    else:
        catalyst = "Chưa đủ chuỗi BCTC để xác nhận tăng trưởng LNST YoY. Dữ liệu hiện có cũng chưa đủ để kết luận doanh nghiệp có lợi thế cạnh tranh riêng."

    risk_map = {
        "NGAN_HANG": f"Rủi ro nợ xấu và áp lực thu hẹp NIM. D/E hiện ở mức {de:.2f}.",
        "CHUNG_KHOAN": f"Rủi ro thanh khoản thị trường và danh mục tự doanh.",
        "BAT_DONG_SAN": "Rủi ro pháp lý dự án và đòn bẩy nợ cao.",
        "CONG_NGHE_BAN_LE": "Rủi ro cạnh tranh và biên lợi nhuận bị thu hẹp.",
        "DIEN_NUOC_HA_TANG": "Rủi ro chính sách giá và chi phí vốn tăng.",
        "NGUYEN_VAT_LIEU_CHU_KY": f"Rủi ro giá hàng hóa đảo chiều làm biên gộp {gm:.1f}% co lại.",
        "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Rủi ro cạnh tranh từ hàng nhập khẩu và sức mua yếu.",
    }
    risks = (
        "Rủi ro ngành cần theo dõi, chưa được xác nhận là vấn đề đang xảy ra tại riêng doanh nghiệp: "
        + risk_map.get(sector, f"các biến động chung của ngành {sector_name}.")
    )

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

    macro_view = (
        f"Chưa có bộ dữ liệu vĩ mô cập nhật được xác minh trong đầu vào của mã {symbol}. "
        "Các biến số cần theo dõi tùy ngành gồm lãi suất, tỷ giá, thanh khoản thị trường, "
        "giá nguyên liệu đầu vào, nhu cầu tiêu thụ và thay đổi chính sách. "
        "Đây là danh sách yếu tố cần theo dõi, không phải khẳng định chúng đang biến động."
    )

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
        # =======================================================
        # BƯỚC 1: KIỂM TRA DỮ LIỆU ĐẦU VÀO (CHẶN GỌI AI VÔ ÍCH)
        # =======================================================
        fair_low = float(sm.get("fair_value_low", 0) or 0)
        fair_high = float(sm.get("fair_value_high", 0) or 0)
        
        # AI vẫn phải phân tích BCTC khi thiếu Fair Value; tuyệt đối không tự tạo giá trị thay thế.
        fair_value_available = fair_low > 0 and fair_high > 0
        if not fair_value_available:
            fair_low = 0.0
            fair_high = 0.0
            print(f"  ⚠️ [{symbol}] Thiếu Fair Value: tiếp tục phân tích BCTC, khóa mọi kết luận định giá.", flush=True)

        bctc_doc = bctc_col.find_one({"symbol": symbol}, {"_id": 0, "income_statement": 1, "cash_flow": 1, "balance_sheet": 1})
        verified_facts = build_verified_facts(symbol, sm, si, sector, bctc_doc)
        sector_guidance = get_sector_guidance(sector)
        
        final_decision = si.get("final_decision", "CHƯA XÁC ĐỊNH")
        rec_text, rec_weight = DECISION_TO_RECOMMENDATION.get(final_decision, ("NẮM GIỮ / THEO DÕI", "Tỷ trọng 0%"))
        
        verdict = sm.get("valuation_verdict", "CHƯA TÍNH")
        discount_pct = float(sm.get("discount_to_fair_pct", 0) or 0)
        close_price = float(sm.get("close", sm.get("close_price", 0)) or 0)
        
        # Chuẩn hóa đơn vị
        if close_price > 0 and close_price < 1000: close_price *= 1000
        if fair_low > 0 and fair_low < 1000: fair_low *= 1000
        if fair_high > 0 and fair_high < 1000: fair_high *= 1000

        prompt = f"""VAI TRÒ
Bạn là chuyên gia phân tích doanh nghiệp niêm yết tại Việt Nam, tư duy độc lập, thận trọng và dựa trên bằng chứng. Nhiệm vụ là diễn giải dữ liệu đã cung cấp, không phải quảng bá cổ phiếu hay cố tạo ra một khuyến nghị hấp dẫn.

MÃ CỔ PHIẾU: {symbol}

NGUYÊN TẮC ƯU TIÊN
- Khối DỮ KIỆN ĐÃ KIỂM CHỨNG là nguồn số liệu duy nhất. Không tự thêm số, tỷ lệ, mốc thời gian, sự kiện, giá hàng hóa, chính sách mới hoặc thông tin doanh nghiệp không có trong dữ kiện.
- Không dùng kiến thức nhớ được về doanh nghiệp để khẳng định tình hình hiện tại. Nếu dữ kiện không đủ, nói rõ "chưa có dữ liệu để kết luận".
- Phân biệt rõ: số liệu TTM (12 tháng gần nhất), số liệu của một quý, tăng trưởng YoY và tăng trưởng QoQ. Không so sánh các đại lượng khác kỳ hoặc khác cơ sở.
- Không tự tính lại ROE, ROA, CFO, LNST, biên gộp, giá trị hợp lý hay biên an toàn. Chỉ dùng phép tính đơn giản khi đủ đầu vào, nêu rõ cách tính và đơn vị.
- Không biến tương quan thành nguyên nhân. Không giải thích CFO tăng/giảm do vốn lưu động, mùa vụ, tồn kho, công nợ... nếu dữ kiện không chứng minh.
- Số 0, giá trị rỗng hoặc "không đủ dữ liệu" không được diễn giải thành kết quả kinh doanh bằng 0. Không coi dữ liệu thiếu là tín hiệu tích cực hoặc tiêu cực.
- Không dùng ngôn ngữ chắc chắn như "bền vững", "luôn phục hồi", "đầu ngành", "lợi thế độc quyền" nếu dữ kiện chưa chứng minh.
- Mọi giá cổ phiếu phải ghi đơn vị VNĐ và dùng đúng mức giá trong dữ kiện. Các số liệu BCTC ghi tỷ VNĐ nếu khối dữ kiện ghi đơn vị tỷ VNĐ.

--- DỮ KIỆN ĐÃ KIỂM CHỨNG ---
{verified_facts}

--- HƯỚNG DẪN THEO NGÀNH ---
{sector_guidance}

--- QUYẾT ĐỊNH VÀ ĐỊNH GIÁ DO HỆ THỐNG TÍNH ---
Quyết định: {final_decision}
Verdict: {verdict}
Giá hiện tại: {close_price:,.0f} VNĐ
Fair Low: {fair_low:,.0f} VNĐ
Fair High: {fair_high:,.0f} VNĐ
Discount/Margin of Safety do hệ thống cung cấp: {discount_pct:.1f}%

QUY TẮC KHÔNG ĐƯỢC VI PHẠM
1. KHÔNG được thay đổi quyết định của hệ thống. Trường recommendation bắt buộc là "{rec_text} — {rec_weight}". Không tự đề xuất tỷ trọng khác.
2. valuation_verdict phải sao chép nguyên văn "{verdict}".
3. Không tự suy ra giá mục tiêu mới, Fair Value mới, xác suất tăng giá hoặc mức lỗ tiềm năng.
4. Không gọi giá hiện tại là "rẻ" chỉ vì thấp hơn Fair High. Phải đối chiếu đồng thời với Fair Low, Fair High và Verdict.
5. Chỉ diễn giải Discount/Margin of Safety theo đúng số {discount_pct:.1f}% hệ thống cung cấp; không tự đổi dấu, đổi mẫu số hoặc gọi đó là mức chiết khấu dương nếu không có căn cứ.
6. Nếu dữ liệu BCTC mới nhất cũ hơn kỳ dự kiến hoặc có cảnh báo trong khối dữ kiện, phải nêu rõ hạn chế này và không mô tả dữ liệu cũ là hiện tại.
7. Phần vĩ mô chỉ được nêu yếu tố có căn cứ trong dữ kiện. Nếu không có dữ liệu vĩ mô cập nhật, phải ghi rõ chưa có dữ liệu xác minh; có thể nêu các biến số cần theo dõi dưới dạng rủi ro/giả thuyết, không được khẳng định chúng đang tăng, giảm hay đã xảy ra.
8. Phân biệt nhận định cấp ngành với lợi thế riêng của doanh nghiệp. Không tự gán thị phần, thương hiệu mạnh, độc quyền, năng lực cạnh tranh hoặc catalyst cụ thể khi chưa có dữ kiện.
9. Tránh lặp lại cùng một con số và cùng một kết luận ở nhiều mục. Mỗi mục phải có nhiệm vụ riêng.
10. Trả về JSON hợp lệ, không Markdown, không code fence, không lời dẫn. Giá trị của mọi trường là chuỗi tiếng Việt; escape dấu ngoặc kép nếu cần.

--- YÊU CẦU NỘI DUNG TỪNG TRƯỜNG ---
{{
  "bctc": "Khoảng 8-12 câu. Mở đầu bằng kỳ BCTC mới nhất và mức độ đầy đủ của dữ liệu. Đánh giá riêng chất lượng lợi nhuận TTM (ROE, ROA, LNST, CFO, CFO/LNST nếu đủ số liệu). Sau đó phân tích chuỗi tối đa 8 quý: doanh thu, LNST, biên gộp, YoY và QoQ khi có kỳ so sánh hợp lệ. Chỉ ra xu hướng tăng/giảm/đi ngang và quý bất thường nếu dữ kiện thể hiện rõ. Phân tích CFO theo quý riêng với CFO TTM; CFO quý không phải số dư tiền mặt. Không kết luận nguyên nhân biến động khi thiếu dữ kiện. Kết thúc bằng đánh giá cân bằng về điểm mạnh, điểm cần theo dõi và giới hạn dữ liệu.",
  "catalyst": "3-4 câu. Nêu động lực tăng trưởng có căn cứ. Chỉ nêu Economic Moat riêng của doanh nghiệp nếu có dữ liệu chứng minh; nếu chỉ có hướng dẫn ngành thì nói đó là đặc điểm chung của ngành, không khẳng định là lợi thế đã được xác nhận của doanh nghiệp. Không tự bịa kế hoạch, thị phần hoặc sự kiện.",
  "risks": "3-5 câu. Xếp 2-3 rủi ro quan trọng theo mức độ ảnh hưởng: rủi ro kinh doanh/ngành, tài chính/dòng tiền và định giá nếu có dữ kiện. Nêu cơ sở của từng rủi ro; phân biệt rủi ro đã thể hiện trong số liệu với rủi ro cần theo dõi.",
  "valuation_verdict": "Sao chép nguyên văn verdict do hệ thống cung cấp: '{verdict}'.",
  "outlook": "4-5 câu. Đối chiếu giá {close_price:,.0f} VNĐ với Fair Low {fair_low:,.0f} VNĐ và Fair High {fair_high:,.0f} VNĐ. Nêu rõ giá đang ở dưới, trong hay trên vùng định giá theo đúng dữ kiện. Diễn giải verdict và quyết định '{final_decision}'. Nhắc Discount/Margin of Safety {discount_pct:.1f}% đúng nguyên trạng, không tự suy diễn công thức. Nêu điều kiện cần có để đánh giá lại, không dự báo giá chắc chắn.",
  "recommendation": "BẮT BUỘC giữ nguyên: '{rec_text} — {rec_weight}'.",
  "macro_view": "3-5 câu. Chỉ phân tích yếu tố vĩ mô/ngành có bằng chứng trong dữ kiện. Nếu không có số liệu cập nhật, nói rõ chưa có dữ liệu xác minh; liệt kê ngắn gọn các biến số cần theo dõi và giải thích kênh tác động có thể có, dùng ngôn ngữ điều kiện như 'có thể', 'cần theo dõi'. Không bịa giá hàng hóa, tỷ giá, lãi suất, chính sách hoặc dự báo."
}}

TỰ KIỂM TRA TRƯỚC KHI TRẢ JSON
- Có số nào không xuất hiện trong dữ kiện hoặc không thể tính trực tiếp từ dữ kiện không? Nếu có, xóa.
- Có nhầm CFO quý với CFO TTM hoặc tiền mặt cuối kỳ không? Nếu có, sửa.
- Có nhầm YoY với QoQ, hoặc so sánh kỳ không tương ứng không? Nếu có, sửa.
- Có khẳng định nguyên nhân, lợi thế cạnh tranh hay diễn biến vĩ mô mà không có bằng chứng không? Nếu có, hạ thành giả thuyết cần theo dõi hoặc bỏ.
- recommendation và valuation_verdict có giữ nguyên dữ liệu hệ thống không? Nếu không, sửa.
Chỉ xuất JSON hợp lệ."""

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
                    if not isinstance(parsed, dict):
                        raise ValueError("AI response phải là JSON object")

                    # Bắt buộc đủ các mục phân tích; thiếu mục thì retry, không lưu báo cáo cụt.
                    required_fields = (
                        "bctc", "catalyst", "risks", "valuation_verdict",
                        "outlook", "recommendation", "macro_view"
                    )
                    for field in required_fields:
                        value = parsed.get(field)
                        if not isinstance(value, str) or not value.strip():
                            raise ValueError(f"AI thiếu trường bắt buộc hoặc sai kiểu: {field}")

                    # Các quyết định định lượng là bất biến: AI chỉ được diễn giải, không được ghi đè.
                    parsed["valuation_verdict"] = (
                        verdict if fair_value_available else "KHÔNG ĐỦ DỮ LIỆU"
                    )
                    parsed["recommendation"] = f"{rec_text} — {rec_weight}"
                    parsed["price_zone"] = (
                        f"Vùng mua: {fair_low:,.0f} VNĐ — {fair_high:,.0f} VNĐ | "
                        f"Giá hiện tại: {close_price:,.0f} VNĐ | "
                        f"Verdict: {parsed['valuation_verdict']}"
                    )
                    parsed["_source"] = "AI"
                    parsed["_validation"] = "SCHEMA_OK_SYSTEM_DECISIONS_LOCKED"
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

# ===================== HOLIDAY CHECKER =====================
def _check_holiday_smart_standalone(vn_now):
    top_with_volume = 0
    top_with_change = 0
    top_total = 0
    
    for sym in LIQUID_SYMBOLS_TOP:
        doc = collection.find_one({"symbol": sym})
        if doc:
            top_total += 1
            vol = doc.get("volume", 0) or 0
            close_p = doc.get("close", 0) or 0
            prev_p = doc.get("prev_close", 0) or 0
            
            if vol > 0:
                top_with_volume += 1
            if prev_p > 0 and close_p != prev_p:
                top_with_change += 1
    
    if top_with_volume >= 3:
        return (False, "99.9%", f"TOP: {top_with_volume}/{top_total} mã có volume > 0")
    
    if top_with_change >= 3:
        return (False, "99%", f"TOP: {top_with_change}/{top_total} mã có biến động giá (volume=0)")
    
    mid_with_volume = 0
    mid_with_change = 0
    mid_total = 0
    
    for sym in LIQUID_SYMBOLS_MID:
        doc = collection.find_one({"symbol": sym})
        if doc:
            mid_total += 1
            vol = doc.get("volume", 0) or 0
            close_p = doc.get("close", 0) or 0
            prev_p = doc.get("prev_close", 0) or 0
            
            if vol > 0:
                mid_with_volume += 1
            if prev_p > 0 and close_p != prev_p:
                mid_with_change += 1
    
    if mid_with_volume >= 2 or mid_with_change >= 5:
        return (False, "95%", f"MID: {mid_with_volume}/{mid_total} mã volume>0, {mid_with_change}/{mid_total} mã biến động")
    
    return (
        True,
        "99.99%",
        f"Cả TOP ({top_total} mã) + MID ({mid_total} mã) đều không giao dịch. Volume=0, giá đứng."
    )


def holiday_checker_worker():
    global holiday_checked_today, holiday_status, holiday_detected_date, is_holiday_paused
    
    print("🗓️ [HOLIDAY] Đã bật thread check ngày lễ (9h30 hàng ngày)!", flush=True)
    
    while True:
        try:
            if is_auto_crawl_paused:
                time.sleep(30)
                continue
            
            vn_now = _vn_now()
            today_str = vn_now.strftime("%Y-%m-%d")
            current_hour = vn_now.hour
            current_minute = vn_now.minute
            weekday = vn_now.weekday()
            
            if weekday >= 5:
                time.sleep(60)
                continue
            
            is_after_930 = (current_hour > 9) or (current_hour == 9 and current_minute >= 30)
            is_before_15h = current_hour < 15
            if not (is_after_930 and is_before_15h):
                time.sleep(30)
                continue
            
            with holiday_check_lock:
                already_checked = (holiday_checked_today == today_str)
            
            if already_checked:
                # FIX BUG #9: Sleep dài hơn sau khi đã check (tránh spam log)
                time.sleep(1800)
                continue
            
            print(f"\n🗓️ [HOLIDAY] Check ngày lễ lúc {vn_now.strftime('%H:%M:%S')} (thread riêng)...", flush=True)
            
            is_holiday, confidence, details = _check_holiday_smart_standalone(vn_now)
            
            with holiday_check_lock:
                holiday_checked_today = today_str
                holiday_status = is_holiday
                if is_holiday:
                    holiday_detected_date = today_str
            
            if is_holiday:
                print(f"🌙 [HOLIDAY] KẾT LUẬN: NGÀY LỄ (Độ tin cậy {confidence})", flush=True)
                print(f"   Chi tiết: {details}", flush=True)
                print(f"🌙 [HOLIDAY] Tạm dừng crawl tới 8h55 hôm sau...", flush=True)
                
                is_holiday_paused = True
                
                tomorrow = vn_now + timedelta(days=1)
                while tomorrow.weekday() >= 5:
                    tomorrow += timedelta(days=1)
                wake_up_time = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                sleep_seconds = (wake_up_time - vn_now).total_seconds()
                time.sleep(min(sleep_seconds, 3600))
                
                is_holiday_paused = False
                print(f"☀️ [HOLIDAY] Đã hết thời gian nghỉ, tự mở lại hệ thống.", flush=True)
            else:
                print(f"✅ [HOLIDAY] KẾT LUẬN: NGÀY LÀM VIỆC (Độ tin cậy {confidence})", flush=True)
                print(f"   Chi tiết: {details}", flush=True)
                # FIX BUG #9: Sleep 1800s thay vì 60s (đã check xong)
                time.sleep(1800)
                
        except Exception as e:
            print(f"❌ [HOLIDAY] Lỗi: {e}", flush=True)
            time.sleep(30)


# ===================== AUTO CRAWLER WORKER =====================
def auto_crawler_worker():
    global holiday_status, holiday_checked_today, holiday_detected_date, is_holiday_paused
    
    print("⏰ [AUTO] Đã bật chế độ cào Real-time (Đọc kết quả check lễ từ thread riêng)!", flush=True)
    
    while True:
        try:
            global is_auto_crawl_paused
            
            vn_now_check = _vn_now()
            today_check = vn_now_check.strftime("%Y-%m-%d")
            with holiday_check_lock:
                if holiday_detected_date and holiday_detected_date != today_check:
                    if is_holiday_paused:
                        print(f"🔧 [AUTO] Auto-reset is_holiday_paused (đã sang ngày {today_check})", flush=True)
                        is_holiday_paused = False
                    holiday_status = None
                    holiday_checked_today = None
                    holiday_detected_date = None
            
            if is_auto_crawl_paused or is_holiday_paused:
                time.sleep(10)
                continue

            vn_now = _vn_now()
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
            is_closing_window = (current_hour == 15 and current_minute < 5)
            # Sau 15:05 tiếp tục rà soát mã chưa có nến D1 hôm nay.
            # Giới hạn đến 16:30 để tránh gọi SSI ngoài phiên quá lâu.
            is_post_close_window = (
                (current_hour == 15 and current_minute >= 5)
                or (current_hour == 16 and current_minute < 30)
            )

            if not (is_trading_time or is_closing_window or is_post_close_window):
                if is_lunch_break:
                    wake_up_time = vn_now.replace(hour=13, minute=0, second=0, microsecond=0)
                else:
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

            with holiday_check_lock:
                current_holiday_status = holiday_status
                current_holiday_checked_date = holiday_checked_today
            
            is_after_930 = (current_hour > 9) or (current_hour == 9 and current_minute >= 30)
            
            if is_after_930 and current_holiday_checked_date != today_str:
                is_after_10h = (current_hour >= 10)
                if is_after_10h:
                    print(f"⚠️ [AUTO] Quá 10h mà chưa có kết quả check lễ → TỰ CHECK (fallback).", flush=True)
                    try:
                        is_holiday, confidence, details = _check_holiday_smart_standalone(vn_now)
                        with holiday_check_lock:
                            holiday_checked_today = today_str
                            holiday_status = is_holiday
                            if is_holiday:
                                holiday_detected_date = today_str
                        
                        if is_holiday:
                            print(f"🌙 [AUTO] Fallback: NGÀY LỄ (Độ tin cậy {confidence}): {details}", flush=True)
                            is_holiday_paused = True
                            tomorrow = vn_now + timedelta(days=1)
                            while tomorrow.weekday() >= 5:
                                tomorrow += timedelta(days=1)
                            wake_up = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                            time.sleep(min((wake_up - vn_now).total_seconds(), 3600))
                            is_holiday_paused = False
                            continue
                        else:
                            print(f"✅ [AUTO] Fallback: NGÀY LÀM VIỆC (Độ tin cậy {confidence}): {details}", flush=True)
                    except Exception as fe:
                        print(f"❌ [AUTO] Fallback check lỗi: {fe}. Mặc định ngày làm việc.", flush=True)
                        with holiday_check_lock:
                            holiday_checked_today = today_str
                            holiday_status = False
                else:
                    print(f"⏳ [AUTO] Chờ holiday_checker_worker check xong (đã qua 9h30)...", flush=True)
                    time.sleep(15)
                    continue
            
            if current_holiday_checked_date == today_str and current_holiday_status == True:
                print(f"🌙 [AUTO] Hôm nay là ngày lễ (theo holiday_checker). Tạm nghỉ...", flush=True)
                time.sleep(60)
                continue

            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]

            # Sau giờ đóng cửa: chỉ rà soát D1 khi server thực sự rảnh.
            # Không chạy song song với realtime, Screener hoặc crawler BCTC/lịch sử.
            if is_post_close_window:
                if is_crawling or is_screener_running or is_full_data_running:
                    time.sleep(30)
                    continue

                saved_today = {
                    item.get("symbol")
                    for item in history_col.find(
                        {"symbol": {"$in": all_symbols}, "history_d1.time": today_str},
                        {"_id": 0, "symbol": 1}
                    )
                    if item.get("symbol")
                }
                stock_states = {
                    item.get("symbol"): item
                    for item in collection.find(
                        {"symbol": {"$in": all_symbols}},
                        {"_id": 0, "symbol": 1, "last_error_at": 1}
                    )
                    if item.get("symbol")
                }
                missing_d1 = [
                    sym for sym in all_symbols
                    if sym not in saved_today
                ]

                # Lỗi SSI/429 được giãn tối thiểu 120 giây giữa hai lượt thử
                # cho cùng mã; mã khác vẫn có thể được rà soát trong lượt này.
                retry_symbols = []
                now_vn = _vn_now()
                for sym in missing_d1:
                    last_error_at = (stock_states.get(sym) or {}).get("last_error_at")
                    if last_error_at:
                        try:
                            last_error = datetime.strptime(
                                last_error_at, "%d/%m/%Y %H:%M:%S"
                            ).replace(tzinfo=timezone.utc)
                            if (now_vn - last_error).total_seconds() < 120:
                                continue
                        except (TypeError, ValueError):
                            pass
                    retry_symbols.append(sym)

                if not retry_symbols:
                    if missing_d1:
                        logger.info(
                            "[D1 AUDIT] Còn %d mã thiếu nến hôm nay; đang trong thời gian giãn retry.",
                            len(missing_d1)
                        )
                    time.sleep(30 if missing_d1 else 60)
                    continue

                logger.info(
                    "[D1 AUDIT] Server rảnh: phát hiện %d/%d mã thiếu nến D1 %s; "
                    "đang rà soát/lưu bù.",
                    len(retry_symbols), len(all_symbols), today_str
                )
                _execute_crawl(retry_symbols, is_retry=True, is_manual=False)
                time.sleep(30)
                continue

            # Trong phiên: giữ nguyên cơ chế cào realtime hiện tại.
            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
            crawled_count = collection.count_documents({"chot_so_today": today_str})

            if crawled_count >= len(all_symbols) and len(all_symbols) > 0:
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


# ===================== FULL DATA CRAWLER =====================
def full_data_crawler_worker():
    while True:
        try:
            vn_now = _vn_now()
            is_bctc_time = (0 <= vn_now.hour < 6 and vn_now.minute == 0 and vn_now.second < 10)
            if is_bctc_time:
                target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                expected_period = _expected_latest_bctc_period(vn_now)

                for symbol in target_symbols:
                    bctc_doc = bctc_col.find_one(
                        {"symbol": symbol},
                        {"updated_at": 1, "latest_period": 1}
                    )
                    need = False
                    retry_days = 30

                    if not bctc_doc or not bctc_doc.get("updated_at"):
                        need = True
                    else:
                        latest_period = str(bctc_doc.get("latest_period") or "")
                        period_is_stale = bool(
                            latest_period
                            and re.fullmatch(r"\d{4}-Q[1-4]", latest_period)
                            and latest_period < expected_period
                        )

                        # Dữ liệu chưa theo kịp kỳ công bố dự kiến: thử cập nhật mỗi 7 ngày.
                        # Dữ liệu đã mới: giữ chu kỳ kiểm tra định kỳ 30 ngày.
                        retry_days = 7 if period_is_stale else 30

                        try:
                            last_update = datetime.strptime(
                                bctc_doc["updated_at"], "%d/%m/%Y %H:%M:%S"
                            ).replace(tzinfo=timezone.utc)
                            age_days = (
                                datetime.now(timezone.utc) - last_update
                            ).total_seconds() / 86400
                            need = age_days >= retry_days
                        except (TypeError, ValueError):
                            need = True

                        if period_is_stale:
                            logger.info(
                                "[BCTC AUTO] %s đang cũ: latest=%s, kỳ dự kiến=%s, "
                                "lịch thử cập nhật=%d ngày",
                                symbol, latest_period, expected_period, retry_days
                            )

                    if need:
                        logger.info(
                            "[BCTC AUTO] Đang làm mới %s (kỳ hiện có=%s, kỳ dự kiến=%s)",
                            symbol,
                            (bctc_doc or {}).get("latest_period", "chưa có"),
                            expected_period
                        )
                        res = _fetch_bctc_ssi(symbol)
                        if res:
                            res.update({"symbol": symbol, "updated_at": _now_str()})
                            bctc_col.update_one(
                                {"symbol": symbol}, {"$set": res}, upsert=True
                            )

                            extracted = extract_metrics_from_bctc_data(res)
                            if extracted:
                                collection.update_one(
                                    {"symbol": symbol}, {"$set": extracted}
                                )
                            logger.info(
                                "[BCTC AUTO] %s cập nhật xong | kỳ mới nhất=%s",
                                symbol, res.get("latest_period", "không rõ")
                            )
                        else:
                            logger.warning("[BCTC AUTO] %s không lấy được dữ liệu mới", symbol)

                        time.sleep(3.0)
                time.sleep(3600)
            else:
                time.sleep(30)
        except Exception as exc:
            log_exception("[BCTC AUTO WORKER]", exc)
            time.sleep(60)


# ===================== SCREENER DATA PRELOAD =====================
def _load_screener_bctc(symbols, chunk_size=10):
    """Đọc BCTC theo từng nhóm mã, chỉ lấy 3 bảng cần cho tính điểm."""
    result = {}
    symbols = list(dict.fromkeys(symbols))
    total = len(symbols)
    for start in range(0, total, chunk_size):
        chunk = symbols[start:start + chunk_size]
        cursor = bctc_col.find(
            {"symbol": {"$in": chunk}},
            {
                "_id": 0, "symbol": 1,
                "income_statement": 1, "cash_flow": 1, "balance_sheet": 1
            }
        ).batch_size(chunk_size)
        for doc in cursor:
            if doc.get("symbol"):
                result[doc["symbol"]] = doc
        print(f"[SCREENER] BCTC preload: {min(start + len(chunk), total)}/{total} mã", flush=True)
    return result


def _load_screener_history(symbols, chunk_size=5, bars_limit=1400):
    """Đọc lịch sử theo nhóm nhỏ để tránh một truy vấn trả về payload quá lớn."""
    result = {}
    symbols = list(dict.fromkeys(symbols))
    total = len(symbols)
    for start in range(0, total, chunk_size):
        chunk = symbols[start:start + chunk_size]
        cursor = history_col.find(
            {"symbol": {"$in": chunk}},
            {
                "_id": 0, "symbol": 1,
                "history_d1": {"$slice": -bars_limit}
            }
        ).batch_size(chunk_size)
        for doc in cursor:
            if doc.get("symbol"):
                result[doc["symbol"]] = doc
        print(f"[SCREENER] History preload: {min(start + len(chunk), total)}/{total} mã", flush=True)
    return result


# ===================== SCREENER WORKER =====================
def screener_worker():
    while True:
        try:
            if is_screener_running:
                time.sleep(30)
                continue
            
            vn_now = _vn_now()
            today_str = vn_now.strftime("%Y-%m-%d")
            current_hour = vn_now.hour
            current_minute = vn_now.minute
            
            all_symbols_count = collection.count_documents({})
            crawled_count = collection.count_documents({"chot_so_today": today_str})
            
            is_1630 = (current_hour == 16 and vn_now.minute == 30 and vn_now.second < 10)
            is_after_close = (current_hour > 15) or (current_hour == 15 and current_minute >= 5)
            is_just_closed = (is_after_close and crawled_count >= all_symbols_count and all_symbols_count > 0)

            if is_1630 or is_just_closed:
                today_screener_check = screener_col.find_one({"date": today_str})
                if today_screener_check:
                    time.sleep(60)
                    continue

                print(f"\n[SCREENER] ⏰ Kích hoạt chấm điểm (Lý do: {'16h30' if is_1630 else 'Vừa chốt sổ sau 15h05'})", flush=True)
                
                # TỐI ƯU: Preload toàn bộ dữ liệu vào RAM
                print("[SCREENER] ⚡ Đang tải trước toàn bộ dữ liệu (Stocks, BCTC, History)...", flush=True)
                all_stocks = list(collection.find({}))
                screener_symbols = [doc["symbol"] for doc in all_stocks if doc.get("symbol")]
                all_bctc = _load_screener_bctc(screener_symbols, chunk_size=10)
                all_hist = _load_screener_history(screener_symbols, chunk_size=5, bars_limit=1400)
                
                stock_db = {}
                bulk_updates = []
                
                print("[SCREENER] 🧠 Đang xử lý dữ liệu trong RAM...", flush=True)
                for doc in all_stocks:
                    sym = doc["symbol"]
                    bctc = all_bctc.get(sym)
                    doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                    
                    if bctc:
                        extracted = extract_metrics_from_bctc_data(bctc)
                        if extracted:
                            doc.update(extracted)
                            bulk_updates.append(UpdateOne({"symbol": sym}, {"$set": extracted}))
                    
                    stock_db[sym] = doc

                if bulk_updates:
                    collection.bulk_write(bulk_updates)
                    print(f"[SCREENER] ✅ Đã bulk update metrics cho {len(bulk_updates)} mã.", flush=True)

                normalized_db = {}
                norm_bulk_updates = []
                for s, r in stock_db.items():
                    nd = normalize_stock_data(s, r)
                    update_fields = nd.pop("_update_fields", {})
                    if update_fields:
                        norm_bulk_updates.append(UpdateOne({"symbol": s}, {"$set": update_fields}))
                    normalized_db[s] = nd
                
                if norm_bulk_updates:
                    collection.bulk_write(norm_bulk_updates)

                sector_medians = compute_sector_medians(normalized_db)
                results = []
                
                for symbol, nd in normalized_db.items():
                    grp = get_sub_sector(symbol)
                    cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(symbol))
                    med, _ = get_median_for_symbol(symbol, sector_medians)
                    
                    hist_doc = all_hist.get(symbol)
                    history_bars = hist_doc.get("history_d1", []) if hist_doc else []
                    tech_bars = history_bars[-200:]
                    tech_score_raw, tech_status, pattern = 50, "Tích lũy", "Tích lũy"
                    if ProfessionalTechnicalAnalyzer and tech_bars:
                        try:
                            analyzer = ProfessionalTechnicalAnalyzer(tech_bars) 
                            ptkt_res = analyzer.analyze()
                            tech_score_raw = ptkt_res.get("technical_score", 50)
                            tech_status = ptkt_res.get("technical_status", "Tích lũy")
                            pattern = ptkt_res.get("pattern", "Tích lũy")
                        except Exception: pass
                        
                    stock_doc = stock_db.get(symbol, {})
                    existing_ai = {
                        "bctc": stock_doc.get("ai_bctc", ""), "catalyst": stock_doc.get("ai_catalyst", ""),
                        "risks": stock_doc.get("ai_risks", ""), "macro_view": stock_doc.get("ai_macro_view", ""),
                        "outlook": stock_doc.get("ai_outlook", ""), "recommendation": stock_doc.get("ai_recommendation", ""),
                        "price_zone": stock_doc.get("ai_price_zone", ""), "ai_updated_at": stock_doc.get("ai_updated_at", "Chưa phân tích"),
                        "ai_source": stock_doc.get("ai_source", "AI")
                    }

                    res = evaluate_stock_from_db(
                        symbol, nd, med, grp, cfg, 
                        tech_score_raw=tech_score_raw, tech_status=tech_status, pattern=pattern, 
                        existing_ai_data=existing_ai,
                        history_bars=history_bars
                    )
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
                    print(f"[SCREENER] 📝 Đang kiểm tra và tạo Template cho các mã chưa có AI...", flush=True)
                    template_count = 0
                    for r in results:
                        sym = r["symbol"]
                        existing = stock_db.get(sym, {})
                        
                        if existing and existing.get("ai_bctc") and len(existing.get("ai_bctc")) > 100:
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
                    print(f"[SCREENER] ✅ Đã tạo Template cho {template_count} mã (các mã có AI thật đã được bảo vệ).", flush=True)
                        
                if results:
                    results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                    
                    protected_count = 0
                    for r in results:
                        sym = r["symbol"]
                        existing_stock = stock_db.get(sym, {})
                        if existing_stock and existing_stock.get("ai_bctc") and len(existing_stock.get("ai_bctc")) > 100:
                            r["bctc_evaluation"] = existing_stock.get("ai_bctc", "")
                            r["outlook"] = existing_stock.get("ai_outlook", "")
                            r["ai_recommendation"] = existing_stock.get("ai_recommendation", "")
                            r["ai_price_zone"] = existing_stock.get("ai_price_zone", "")
                            r["macro_view"] = existing_stock.get("ai_macro_view", "")
                            r["ai_updated_at"] = existing_stock.get("ai_updated_at", "")
                            r["ai_source"] = existing_stock.get("ai_source", "AI")
                            protected_count += 1
                    
                    if _safe_replace_screener_results(results, tag="worker"):
                        commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Update Screener {_now_str()}")
                        print(f"[SCREENER] ✅ Đã chấm điểm xong cho {len(results)} mã (Bảo vệ AI cho {protected_count} mã).", flush=True)
                    else:
                        print(f"[SCREENER] ⚠️ Không ghi được kết quả (lock timeout), giữ dữ liệu cũ.", flush=True)

                    try:
                        up_count = 0; down_count = 0; ref_count = 0
                        strong_movers = []; value_stocks = []; danger_stocks = []
                        missing_price_count = 0; missing_pe_count = 0; missing_pb_count = 0; missing_roe_count = 0
                        missing_pe_symbols = []; missing_pb_symbols = []; missing_roe_symbols = []; missing_price_symbols = []

                        for r in results:
                            decision = r.get("final_decision", "")
                            if "TRÁNH" in decision: 
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
                        ratio_up = (up_count / total_up_down) * 100 if total_up_down > 0 else 50

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
                        msg_lines.append(f"  ❌ Có <b>{len(danger_stocks)}</b> mã bị đánh giá TRÁNH (Giá đắt, nền tảng yếu).")
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

# ===================== START WORKERS =====================
threading.Thread(target=auto_crawler_worker, daemon=True).start()
threading.Thread(target=holiday_checker_worker, daemon=True).start()
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
    return jsonify({
        "status": "success", 
        "is_crawling": is_crawling,
        "realtime_progress": _realtime_progress_snapshot(),
        "history_job": _history_job_snapshot(),
        "is_paused": is_auto_crawl_paused,
        "is_holiday_paused": is_holiday_paused,
        "holiday_status": holiday_status,
        "message": "Đang cào real-time" if is_crawling else "Sẵn sàng"
    })


@app.get("/api/pause-crawl")
def pause_crawl():
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin hoặc ADMIN_SECRET_KEY chưa cấu hình."}), 403
    global is_auto_crawl_paused, stop_crawl_requested
    is_auto_crawl_paused = True
    stop_crawl_requested = True
    print("\n[API] ⏸️ Đã TẠM DỪNG hoàn toàn tiến trình cào tự động!", flush=True)
    return jsonify({"status": "success", "message": "Đã tạm dừng cào tự động."})


@app.get("/api/resume-crawl")
def resume_crawl():
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin hoặc ADMIN_SECRET_KEY chưa cấu hình."}), 403
    global is_auto_crawl_paused, stop_crawl_requested, holiday_status, holiday_checked_today, is_holiday_paused
    is_auto_crawl_paused = False
    stop_crawl_requested = False
    
    with holiday_check_lock:
        if holiday_status == True:
            print(f"\n[API] ⚠️ User override: Reset trạng thái ngày lễ. Hệ thống sẽ crawl lại.", flush=True)
            holiday_status = None
            holiday_checked_today = None
    
    if is_holiday_paused:
        print(f"\n[API] ⚠️ Reset is_holiday_paused (đang trong chế độ nghỉ lễ tự động).", flush=True)
        is_holiday_paused = False
    
    print("\n[API] ▶️ Đã TIẾP TỤC tiến trình cào tự động!", flush=True)
    return jsonify({"status": "success", "message": "Đã tiếp tục cào tự động (đã reset trạng thái ngày lễ nếu có)."})


@app.get("/api/holiday-status")
def api_holiday_status():
    with holiday_check_lock:
        return jsonify({
            "status": "success",
            "data": {
                "holiday_checked_today": holiday_checked_today,
                "holiday_status": holiday_status,
                "holiday_detected_date": holiday_detected_date,
                "is_auto_crawl_paused": is_auto_crawl_paused,
                "is_holiday_paused": is_holiday_paused,
                "current_time_vn": _now_str()
            }
        })


@app.post("/api/set-fhsc-key")
def api_set_fhsc_key():
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin hoặc ADMIN_SECRET_KEY chưa cấu hình."}), 403
    print("[API] 🔑 Gọi API /api/set-fhsc-key", flush=True)
    data = request.json
    new_key = data.get("key", "").strip()
    if not new_key: return jsonify({"status": "error", "message": "Key không được để trống"}), 400
    settings_col.update_one({"_id": "fhsc_key"}, {"$set": {"value": new_key, "updated_at": _now_str()}}, upsert=True)
    return jsonify({"status": "success", "message": "Đã lưu API Key mới thành công!"})


@app.get("/api/data")
def api_get_data():
    print("[API] 📈 Gọi API /api/data (Lấy toàn bộ giá)", flush=True)
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
                prev_close = float(doc.get("prev_close", 0) or 0)
                close_price = float(doc.get("close", 0) or 0)
                pct = 0.0
                avail = False
                arrow, color = "—", "slate"

                # Luôn tính lại từ close/prev_close hiện có trong cùng bản ghi.
                # Không giữ price_change_percent cũ vì có thể lệch sau khi giá được cập nhật.
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        pct = round(raw_pct, 2)
                        avail = True
                        if pct > 0: arrow, color = "▲", "emerald"
                        elif pct < 0: arrow, color = "▼", "rose"

                doc["prev_close"] = prev_close
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
            "crawl_in_progress": bool(is_crawling),
            "updated_at": _now_str() + (" (Đang cào - snapshot DB)" if is_crawling else " (Lấy từ DB)"),
            "updated_data": updated_data
        })

    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    return jsonify({
        "status": "success", 
        "updated_at": _now_str() + " (Bắt đầu cào)", 
        "updated_data": {}
    })


# FIX BUG #10: /api/screener trả 503 khi đang trong quá trình ghi, thay vì [] gây frontend crash
@app.get("/api/screener")
def api_get_screener():
    print("[API] 🏆 Gọi API /api/screener (Lấy điểm số)", flush=True)
    try:
        # Nếu đang trong quá trình ghi đè → chờ tối đa 5s cho lock release
        if screener_lock.locked():
            acquired = screener_lock.acquire(timeout=5)
            if acquired:
                screener_lock.release()
        
        data = list(screener_col.find({}, {"_id": 0}))
        
        if not data:
            return jsonify({
                "status": "retry",
                "message": "Screener đang cập nhật, vui lòng thử lại sau 2s",
                "data": []
            }), 503
        
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        log_exception("[API SCREENER]", e)
        return jsonify({"status": "error", "message": str(e)}), 500


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
                if (datetime.now(timezone.utc) - last_update_date.replace(tzinfo=timezone.utc)).days > 45:
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
    if not _admin_key_valid():
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
    
    _admin_progress_start("ai_single", f"Đang chạy AI cho {symbol}")
    _admin_progress_update(total=1, current_symbol=symbol)
    def _run():
        global is_ai_running
        try:
            sector = si.get("sector", "SAN_XUAT_TIEU_DUNG_PHONG_THU")
            # Fair Value nằm trong screener_col; ghép với dữ liệu gốc trước khi gọi AI.
            sm_for_ai = {**sm, **si}
            print(
                f"[AI-DEBUG] {symbol} | "
                f"collection low={sm.get('fair_value_low')!r}, high={sm.get('fair_value_high')!r} | "
                f"screener low={si.get('fair_value_low')!r}, high={si.get('fair_value_high')!r} | "
                f"merged low={sm_for_ai.get('fair_value_low')!r}, high={sm_for_ai.get('fair_value_high')!r}",
                flush=True
            )
            ai_result = _call_ai_for_symbol(symbol, sm_for_ai, si, sector)
            
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
                
                _admin_progress_update(status="success", completed=1, success=1, current_symbol=None, message=f"AI {symbol} đã lưu thành công")
                print(f"[ADMIN-AI-SINGLE] ✅ {symbol} - Đã lưu phân tích AI thành công vào cả 2 bảng!", flush=True)
            else:
                _admin_progress_update(status="error", completed=1, failed=1, current_symbol=None, message=f"AI {symbol} thất bại")
                print(f"[ADMIN-AI-SINGLE] ❌ {symbol} - Gọi AI thất bại.", flush=True)
        except Exception as job_error:
            _admin_progress_update(status="error", completed=1, failed=1, current_symbol=None, message=f"AI {symbol}: {job_error}")
            logger.exception("[ADMIN-AI-SINGLE] Job lỗi")
        finally:
            is_ai_running = False

    threading.Thread(target=_run, daemon=True).start()
    
    return jsonify({
        "status": "success", 
        "message": f"Đã bắt đầu chạy AI cho mã {symbol}. Theo dõi log Render!"
    })


@app.get("/api/admin/force-ai")
def admin_force_ai():
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin hoặc ADMIN_SECRET_KEY chưa cấu hình."}), 403
    global is_ai_running
    if is_ai_running:
        return jsonify({"status": "error", "message": "Đang chạy AI cho toàn bộ mã, vui lòng đợi!"}), 400
    
    if not ALIBABA_API_KEY:
        return jsonify({"status": "error", "message": "Thiếu ALIBABA_API_KEY!"}), 500
    
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    
    is_ai_running = True
    _admin_progress_start("ai_full", "Đang chuẩn bị AI toàn bộ")
    _admin_progress_update(total=len(all_symbols), message=f"Đang chạy AI cho {len(all_symbols)} mã")
    print(f"\n[ADMIN-AI] 🚀 Bắt đầu chạy AI cho toàn bộ {len(all_symbols)} mã...", flush=True)
    
    def _run():
        global is_ai_running
        try:
            total = len(all_symbols)
            count = 0
            
            for i, sym in enumerate(all_symbols, 1):
                _admin_progress_update(current_symbol=sym, message=f"Đang phân tích AI {sym} ({i}/{total})")
                sm = collection.find_one({"symbol": sym}, {"_id": 0})
                si = screener_col.find_one({"symbol": sym}, {"_id": 0})
                
                if not sm or not si:
                    snap = _admin_progress_snapshot()
                    _admin_progress_update(completed=i, failed=snap["failed"] + 1, message=f"{sym}: thiếu dữ liệu, bỏ qua")
                    print(f"[ADMIN-AI] [{i}/{total}] ⚠️ {sym} - Thiếu dữ liệu, bỏ qua.", flush=True)
                    continue
                
                sector = si.get("sector", "SAN_XUAT_TIEU_DUNG_PHONG_THU")
                # Fair Value nằm trong screener_col; ghép với dữ liệu gốc trước khi gọi AI.
                sm_for_ai = {**sm, **si}
                print(
                    f"[AI-DEBUG] {sym} | "
                    f"collection low={sm.get('fair_value_low')!r}, high={sm.get('fair_value_high')!r} | "
                    f"screener low={si.get('fair_value_low')!r}, high={si.get('fair_value_high')!r} | "
                    f"merged low={sm_for_ai.get('fair_value_low')!r}, high={sm_for_ai.get('fair_value_high')!r}",
                    flush=True
                )
                ai_result = _call_ai_for_symbol(sym, sm_for_ai, si, sector)
                
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
                    snap = _admin_progress_snapshot()
                    _admin_progress_update(completed=i, success=snap["success"] + 1)
                    print(f"[ADMIN-AI] [{i}/{total}] ✅ {sym} - OK", flush=True)
                else:
                    snap = _admin_progress_snapshot()
                    _admin_progress_update(completed=i, failed=snap["failed"] + 1)
                    print(f"[ADMIN-AI] [{i}/{total}] ❌ {sym} - Lỗi", flush=True)
                
                time.sleep(3.0)
            
            _admin_progress_update(message=f"Đã phân tích AI {count}/{total} mã; đang đồng bộ GitHub")
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
            snap = _admin_progress_snapshot()
            _admin_progress_update(status="success" if snap["failed"] == 0 else "partial",
                                   current_symbol=None, completed=total, message=f"Hoàn tất AI: {count}/{total} mã thành công")
        except Exception as job_error:
            _admin_progress_update(status="error", message=f"Lỗi AI hàng loạt: {job_error}")
            logger.exception("[ADMIN-AI] Job lỗi")
        finally:
            is_ai_running = False
    
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({
        "status": "success", 
        "message": f"Đã bắt đầu chạy AI cho {len(all_symbols)} mã. Xem log Render!"
    })


def _history_job_snapshot():
    with _history_job_lock:
        data = dict(_history_job_state)
        data["queued"] = len(_history_job_queue)
        return data


def _history_worker():
    global _history_worker_running
    while True:
        with _history_job_lock:
            if not _history_job_queue:
                _history_worker_running = False
                if _history_job_state.get("status") not in ("success", "partial", "error"):
                    _history_job_state.update(status="idle", message="Hàng đợi lịch sử trống")
                return
            job = _history_job_queue.pop(0)
            _history_job_state.update(
                status="running", job_type=job["job_type"], current_symbol=None,
                completed=0, total=0, success=0, failed=0,
                message=f"Bắt đầu cào {job['days']} ngày", updated_at=_now_str()
            )
        try:
            target = sorted(set(
                str(doc.get("symbol", "")).upper().strip()
                for doc in collection.find({}, {"symbol": 1})
                if doc.get("symbol")
            ))
            today_vn = _vn_now().date()
            end_date = today_vn.strftime("%Y-%m-%d")
            start_date = (today_vn - timedelta(days=job["days"])).strftime("%Y-%m-%d")
            with _history_job_lock:
                _history_job_state.update(total=len(target), message=f"Đang cào {len(target)} mã", updated_at=_now_str())
            for index, sym in enumerate(target, 1):
                with _history_job_lock:
                    _history_job_state.update(current_symbol=sym, message=f"{sym} ({index}/{len(target)})", updated_at=_now_str())
                try:
                    df = _fetch_history_ssi(sym, start_date, end_date)
                    if df is None or df.empty:
                        with _history_job_lock:
                            _history_job_state["failed"] += 1
                    else:
                        bars = [{
                            "time": row["time"].strftime("%Y-%m-%d"),
                            "open": float(row["open"]), "high": float(row["high"]),
                            "low": float(row["low"]), "close": float(row["close"]),
                            "volume": int(row["volume"])
                        } for _, row in df.iterrows()]
                        # Serialize against realtime D1 commits to avoid stale $set overwriting a backfill.
                        with _history_write_lock:
                            history_col.update_one(
                                {"symbol": sym},
                                {"$pull": {"history_d1": {"time": {"$in": [b["time"] for b in bars]}}}},
                                upsert=True
                            )
                            history_col.update_one(
                                {"symbol": sym},
                                {"$push": {"history_d1": {"$each": bars, "$sort": {"time": 1}, "$slice": -4000}}},
                                upsert=True
                            )
                            doc_hist = history_col.find_one({"symbol": sym}, {"history_d1": 1})
                            d1 = (doc_hist or {}).get("history_d1", [])
                            if d1:
                                d1 = sorted(d1, key=lambda x: x.get("time", ""))
                                history_col.update_one(
                                    {"symbol": sym},
                                    {"$set": {
                                        "history_w1": aggregate_candles(d1, "W1"),
                                        "history_mn": aggregate_candles(d1, "MN")
                                    }}
                                )
                            with _history_cache["lock"]:
                                _history_cache["data"] = None
                                _history_cache["timestamp"] = 0
                        with _history_job_lock:
                            _history_job_state["success"] += 1
                except Exception:
                    logger.exception("[HISTORY] %s failed during backfill", sym)
                    with _history_job_lock:
                        _history_job_state["failed"] += 1
                finally:
                    with _history_job_lock:
                        _history_job_state["completed"] += 1
                        _history_job_state["updated_at"] = _now_str()
                time.sleep(0.8 if job["days"] <= 30 else 1.2)
            with _history_job_lock:
                failed = _history_job_state["failed"]
                _history_job_state.update(
                    status="partial" if failed else "success",
                    current_symbol=None,
                    message=f"Hoàn tất: {_history_job_state['success']} thành công, {failed} lỗi",
                    updated_at=_now_str()
                )
            print(f"[HISTORY] Finished {job['job_type']}: {_history_job_state['success']} ok / {_history_job_state['failed']} failed", flush=True)
        except Exception:
            logger.exception("[HISTORY] Job crashed")
            with _history_job_lock:
                _history_job_state.update(status="error", message="Tác vụ lịch sử gặp lỗi; xem log server", updated_at=_now_str())


def _enqueue_history_job(job_type, days):
    global _history_worker_running
    with _history_job_lock:
        # Avoid duplicate queued/running jobs of the same range.
        active_type = _history_job_state.get("job_type") if _history_job_state.get("status") == "running" else None
        if active_type == job_type or any(j["job_type"] == job_type for j in _history_job_queue):
            return False, "Tác vụ cùng loại đang chạy hoặc đã nằm trong hàng đợi"
        _history_job_queue.append({"job_type": job_type, "days": days})
        _history_job_state["queued"] = len(_history_job_queue)
        _history_job_state["updated_at"] = _now_str()
        if _history_worker_running:
            message = f"Đã xếp hàng {job_type}; sẽ chạy sau tác vụ hiện tại"
        else:
            message = f"Đã nhận tác vụ {job_type}"
            _history_worker_running = True
            threading.Thread(target=_history_worker, daemon=True, name="history-backfill").start()
        _history_job_state["message"] = message
        return True, message


@app.get("/api/admin/progress")
def admin_progress_status():
    return jsonify({"status": "success", "job": _admin_progress_snapshot()})


@app.get("/api/history-crawl-status")
def history_crawl_status():
    return jsonify({"status": "success", "job": _history_job_snapshot()})


@app.get("/api/force-history")
def force_history():
    print("[API] Request history backfill: 30 days", flush=True)
    ok, message = _enqueue_history_job("history_30d", 30)
    return jsonify({"status": "success" if ok else "error", "message": message, "job": _history_job_snapshot()}), (202 if ok else 409)


@app.get("/api/crawl-full-history")
def crawl_full_history():
    print("[API] Request history backfill: 10 years", flush=True)
    ok, message = _enqueue_history_job("history_10y", 3650)
    return jsonify({"status": "success" if ok else "error", "message": message, "job": _history_job_snapshot()}), (202 if ok else 409)


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
            _admin_progress_start("bctc_batch", "Bắt đầu cào BCTC")
            _admin_progress_update(total=len(target), message=f"Đang cào BCTC: {len(target)} mã")
            for i, sym in enumerate(target):
                _admin_progress_update(current_symbol=sym, message=f"Đang xử lý BCTC {sym} ({i+1}/{len(target)})")
                print(f"\n[BCTC-CRAWL] [{i+1}/{len(target)}] Đang xử lý {sym}...", flush=True)
                try:
                    res = _fetch_bctc_ssi(sym)
                    if res:
                        bctc_col.update_one({"symbol": sym}, {"$set": {**res, "symbol": sym, "updated_at": _now_str()}}, upsert=True)
                        extracted = extract_metrics_from_bctc_data(res)
                        if extracted:
                            collection.update_one({"symbol": sym}, {"$set": extracted})
                        _admin_progress_update(success=_admin_progress_snapshot()["success"] + 1)
                        print(f"  ✅ [THÀNH CÔNG] {sym} | Kỳ BCTC: {res.get('latest_period', 'Không rõ')}", flush=True)
                    else:
                        _admin_progress_update(failed=_admin_progress_snapshot()["failed"] + 1)
                        print(f"  ❌ [THẤT BẠI] {sym} | Không lấy được dữ liệu", flush=True)
                except Exception as symbol_error:
                    _admin_progress_update(failed=_admin_progress_snapshot()["failed"] + 1, message=f"{sym}: {symbol_error}")
                    logger.exception("[BCTC-CRAWL] Lỗi mã %s", sym)
                finally:
                    _admin_progress_update(completed=i + 1)
                time.sleep(3.0)
            _admin_progress_update(status="success" if _admin_progress_snapshot()["failed"] == 0 else "partial",
                                   current_symbol=None, message="Đã hoàn tất cào BCTC")
            print(f"\n[BCTC-CRAWL] 🏁 Đã cào xong toàn bộ {len(target)} mã.", flush=True)
        except Exception as job_error:
            _admin_progress_update(status="error", message=f"Lỗi tác vụ BCTC: {job_error}")
            logger.exception("[BCTC-CRAWL] Job lỗi")
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
            _admin_progress_start("bctc_full", "Đang chuẩn bị cào full BCTC")
            _admin_progress_update(total=len(target), message=f"Đang cào full BCTC: {len(target)} mã")
            print(f"[BCTC-CRAWL] 🚀 Bắt đầu cào toàn bộ BCTC cho {len(target)} mã...", flush=True)
            for i, sym in enumerate(target):
                _admin_progress_update(current_symbol=sym, message=f"Đang xử lý BCTC {sym} ({i+1}/{len(target)})")
                try:
                    res = _fetch_bctc_ssi(sym)
                    if res:
                        bctc_col.update_one({"symbol": sym}, {"$set": {**res, "symbol": sym, "updated_at": _now_str()}}, upsert=True)
                        extracted = extract_metrics_from_bctc_data(res)
                        if extracted: collection.update_one({"symbol": sym}, {"$set": extracted})
                        _admin_progress_update(success=_admin_progress_snapshot()["success"] + 1)
                    else:
                        _admin_progress_update(failed=_admin_progress_snapshot()["failed"] + 1)
                except Exception as symbol_error:
                    _admin_progress_update(failed=_admin_progress_snapshot()["failed"] + 1, message=f"{sym}: {symbol_error}")
                    logger.exception("[BCTC-FULL] Lỗi mã %s", sym)
                finally:
                    _admin_progress_update(completed=i + 1)
                time.sleep(3.0)
            _admin_progress_update(status="success" if _admin_progress_snapshot()["failed"] == 0 else "partial",
                                   current_symbol=None, message="Đã hoàn tất full BCTC")
            print(f"[BCTC-CRAWL] 🏁 Đã cào xong toàn bộ BCTC.", flush=True)
        except Exception as job_error:
            _admin_progress_update(status="error", message=f"Lỗi full BCTC: {job_error}")
            logger.exception("[BCTC-FULL] Job lỗi")
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
            _admin_progress_start("ratios", "Đang chuẩn bị cào chỉ số")
            _admin_progress_update(total=len(target), message=f"Đang cào chỉ số: {len(target)} mã")
            print(f"[RATIO-CRAWL] 🚀 Bắt đầu cào bảng Ratio cho {len(target)} mã...", flush=True)
            
            for i, sym in enumerate(target):
                _admin_progress_update(current_symbol=sym, message=f"Đang lấy P/E, ROE... {sym} ({i+1}/{len(target)})")
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
                eps_final = float(eps_raw or 0)

                collection.update_one({"symbol": sym}, {"$set": {
                    "pe": round(ratios.get("pe", 0), 2),
                    "pb": round(ratios.get("pb", 0), 2),
                    
                    "eps": eps_final, 
                    "roe": round(roe_val, 1),
                    "roa": round(roa_val, 1),
                    "dividend_yield": round(div_val, 2),
                    "ratio_updated_at": _now_str()
                }}, upsert=True)
                
                _admin_progress_update(success=_admin_progress_snapshot()["success"] + 1, completed=i + 1)
                print(f"  ✅ [RATIO] {sym} thành công (P/E: {ratios.get('pe', 0)}, EPS: {eps_final}, ROE: {round(roe_val, 1)}%)", flush=True)
                time.sleep(0.5)

            _admin_progress_update(status="success", current_symbol=None, message="Đã hoàn tất cào chỉ số")
            print(f"[RATIO-CRAWL] 🏁 Đã cào xong toàn bộ bảng Ratio.", flush=True)
        except Exception as job_error:
            snap = _admin_progress_snapshot()
            _admin_progress_update(status="error", failed=snap["failed"] + 1, message=f"Lỗi cào chỉ số: {job_error}")
            logger.exception("[RATIO-CRAWL] Job lỗi")
        finally:
            is_full_data_running = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào bảng Ratio..."})


@app.get("/api/score-single")
def api_score_single():
    """
    Chấm điểm một mã theo đúng công thức Screener hiện tại.
    Không preload BCTC/lịch sử của toàn thị trường và không ghi đè toàn bộ screener.
    """
    symbol = request.args.get("symbol", "").upper().strip()
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin!"}), 403
    if not symbol or not re.fullmatch(r"[A-Z0-9]{3,10}", symbol):
        return jsonify({"status": "error", "message": "Mã cổ phiếu không hợp lệ."}), 400
    if is_screener_running:
        return jsonify({
            "status": "busy",
            "message": "Screener toàn thị trường đang chạy. Vui lòng thử lại sau."
        }), 409

    print(f"[API] 🎯 Chấm điểm riêng mã {symbol}...", flush=True)
    try:
        # 1) Chỉ lấy dữ liệu gốc và BCTC của mã được yêu cầu.
        stock_doc = collection.find_one({"symbol": symbol}, {"_id": 0})
        if not stock_doc:
            return jsonify({"status": "error", "message": f"Không tìm thấy dữ liệu giá cho {symbol}."}), 404

        bctc_doc = bctc_col.find_one(
            {"symbol": symbol},
            {"_id": 0, "income_statement": 1, "cash_flow": 1, "balance_sheet": 1}
        ) or {}
        stock_doc["bctc_history"] = bctc_doc.get("income_statement", []) or []
        extracted = extract_metrics_from_bctc_data(bctc_doc)
        if extracted:
            stock_doc.update(extracted)
            collection.update_one({"symbol": symbol}, {"$set": extracted})

        # 2) Trung vị ngành lấy từ dữ liệu giá/định giá đang lưu sẵn.
        # Không tải BCTC hoặc lịch sử của các mã khác.
        peer_projection = {
            "_id": 0, "symbol": 1, "close": 1, "close_price": 1,
            "pe": 1, "pb": 1, "roe": 1, "roa": 1, "eps": 1,
            "dividend_yield": 1, "cfo_latest": 1, "net_profit_ttm": 1,
            "debt_to_equity": 1, "gross_margin": 1
        }
        normalized_peers = {}
        for peer in collection.find({}, peer_projection):
            peer_symbol = peer.get("symbol")
            if peer_symbol:
                normalized_peers[peer_symbol] = normalize_stock_data(peer_symbol, peer)
        sector_medians = compute_sector_medians(normalized_peers)
        group_label = get_sub_sector(symbol)
        config = SUB_SECTOR_CONFIG.get(group_label) or SUB_SECTOR_CONFIG.get(get_sector(symbol))
        if not config:
            return jsonify({"status": "error", "message": f"Chưa có cấu hình chấm điểm cho ngành của {symbol}."}), 422
        sector_median, _ = get_median_for_symbol(symbol, sector_medians)
        nd = normalize_stock_data(symbol, stock_doc)
        update_fields = nd.pop("_update_fields", {})
        if update_fields:
            collection.update_one({"symbol": symbol}, {"$set": update_fields})

        # 3) Chỉ tải lịch sử của mã được yêu cầu; phân tích kỹ thuật dùng 200 nến gần nhất.
        hist_doc = history_col.find_one(
            {"symbol": symbol},
            {"_id": 0, "history_d1": {"$slice": -1400}}
        ) or {}
        history_bars = hist_doc.get("history_d1", []) or []
        tech_bars = history_bars[-200:]
        tech_score_raw, tech_status, pattern = 50, "Tích lũy", "Tích lũy"
        if ProfessionalTechnicalAnalyzer and tech_bars:
            try:
                analyzer = ProfessionalTechnicalAnalyzer(tech_bars)
                ptkt_res = analyzer.analyze()
                tech_score_raw = ptkt_res.get("technical_score", 50)
                tech_status = ptkt_res.get("technical_status", "Tích lũy")
                pattern = ptkt_res.get("pattern", "Tích lũy")
            except Exception as tech_error:
                print(f"[SINGLE-SCREENER] {symbol} lỗi phân tích kỹ thuật: {tech_error}", flush=True)

        # 4) Giữ nguyên dữ liệu AI đã có, không gọi AI trong luồng chấm điểm.
        old_result = screener_col.find_one({"symbol": symbol}, {"_id": 0}) or {}
        existing_ai = {
            "bctc": stock_doc.get("ai_bctc") or old_result.get("bctc_evaluation", ""),
            "catalyst": stock_doc.get("ai_catalyst") or old_result.get("catalyst", ""),
            "risks": stock_doc.get("ai_risks") or old_result.get("risks", ""),
            "macro_view": stock_doc.get("ai_macro_view") or old_result.get("macro_view", ""),
            "outlook": stock_doc.get("ai_outlook") or old_result.get("outlook", ""),
            "recommendation": stock_doc.get("ai_recommendation") or old_result.get("ai_recommendation", ""),
            "price_zone": stock_doc.get("ai_price_zone") or old_result.get("ai_price_zone", ""),
            "ai_updated_at": stock_doc.get("ai_updated_at") or old_result.get("ai_updated_at", "Chưa phân tích"),
            "ai_source": stock_doc.get("ai_source") or old_result.get("ai_source", "AI")
        }

        result = evaluate_stock_from_db(
            symbol, nd, sector_median, group_label, config,
            tech_score_raw=tech_score_raw,
            tech_status=tech_status,
            pattern=pattern,
            existing_ai_data=existing_ai,
            history_bars=history_bars
        )
        if not result:
            return jsonify({"status": "error", "message": f"Không thể chấm điểm {symbol}."}), 422

        result["date"] = _vn_now().strftime("%Y-%m-%d")
        result["score_updated_at"] = _now_str()

        # Nếu screener toàn thị trường vừa bắt đầu trong lúc tính, không ghi đè dữ liệu của nó.
        if is_screener_running:
            return jsonify({
                "status": "busy",
                "message": "Screener toàn thị trường vừa bắt đầu; kết quả riêng chưa được lưu."
            }), 409

        screener_col.update_one(
            {"symbol": symbol},
            {"$set": result},
            upsert=True
        )
        print(
            f"[SINGLE-SCREENER] ✅ {symbol} | Tổng {result.get('total_score')}/100 "
            f"| {result.get('final_decision')}",
            flush=True
        )
        return jsonify({
            "status": "success",
            "message": f"Đã chấm điểm riêng {symbol}.",
            "symbol": symbol,
            "data": result
        })
    except Exception as e:
        log_exception(f"[SINGLE-SCREENER] Lỗi chấm điểm {symbol}", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/force-screener")
def force_screener():
    if not _admin_key_valid():
        return jsonify({"status": "error", "message": "Sai mật khẩu Admin!"}), 403
        
    print(f"\n[API] 📥 Gọi API /api/force-screener", flush=True)
    global is_screener_running
    if is_screener_running: 
        return jsonify({"status": "error", "message": "Screener đang chạy (worker hoặc API khác), vui lòng đợi!"}), 400
        
    is_screener_running = True
    _admin_progress_start("screener", "Đang tải dữ liệu Screener")
    print(f"[SCREENER] 🚀 Bắt đầu chấm điểm lại toàn bộ (Chế độ tối ưu)...", flush=True)
    
    def _run():
        global is_screener_running
        try:
            today_str = _vn_now().strftime("%Y-%m-%d")
            
            # TỐI ƯU: Preload dữ liệu
            print("[SCREENER] ⚡ Đang tải trước toàn bộ dữ liệu...", flush=True)
            all_stocks = list(collection.find({}))
            screener_symbols = [doc["symbol"] for doc in all_stocks if doc.get("symbol")]
            _admin_progress_update(total=len(screener_symbols), message=f"Đã tìm thấy {len(screener_symbols)} mã; đang tải BCTC và lịch sử")
            all_bctc = _load_screener_bctc(screener_symbols, chunk_size=10)
            all_hist = _load_screener_history(screener_symbols, chunk_size=5, bars_limit=1400)
            
            stock_db = {}
            bulk_updates = []
            
            for doc in all_stocks:
                sym = doc["symbol"]
                bctc = all_bctc.get(sym)
                doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                
                if bctc:
                    extracted = extract_metrics_from_bctc_data(bctc)
                    if extracted:
                        doc.update(extracted)
                        bulk_updates.append(UpdateOne({"symbol": sym}, {"$set": extracted}))
                
                stock_db[sym] = doc

            if bulk_updates:
                collection.bulk_write(bulk_updates)

            norm_db = {}
            norm_bulk_updates = []
            for s, r in stock_db.items():
                nd = normalize_stock_data(s, r)
                update_fields = nd.pop("_update_fields", {})
                if update_fields:
                    norm_bulk_updates.append(UpdateOne({"symbol": s}, {"$set": update_fields}))
                norm_db[s] = nd
            
            if norm_bulk_updates:
                collection.bulk_write(norm_bulk_updates)
            
            medians = compute_sector_medians(norm_db)
            results = []
            
            _admin_progress_update(message="Đã chuẩn hóa dữ liệu; đang tính điểm từng mã")
            for score_index, (sym, nd) in enumerate(norm_db.items(), 1):
                _admin_progress_update(current_symbol=sym, message=f"Đang chấm điểm {sym} ({score_index}/{len(norm_db)})")
                grp = get_sub_sector(sym)
                cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(sym))
                med, _ = get_median_for_symbol(sym, medians)
                
                hist_doc = all_hist.get(sym)
                history_bars = hist_doc.get("history_d1", []) if hist_doc else []
                tech_bars = history_bars[-200:]
                tech_score_raw, tech_status, pattern = 50, "Tích lũy", "Tích lũy"
                if ProfessionalTechnicalAnalyzer and tech_bars:
                    try:
                        analyzer = ProfessionalTechnicalAnalyzer(tech_bars) 
                        ptkt_res = analyzer.analyze()
                        tech_score_raw = ptkt_res.get("technical_score", 50)
                        tech_status = ptkt_res.get("technical_status", "Tích lũy")
                        pattern = ptkt_res.get("pattern", "Tích lũy")
                    except Exception: pass
                    
                stock_doc = stock_db.get(sym, {})
                existing_ai = {
                    "bctc": stock_doc.get("ai_bctc", ""), "catalyst": stock_doc.get("ai_catalyst", ""),
                    "risks": stock_doc.get("ai_risks", ""), "macro_view": stock_doc.get("ai_macro_view", ""),
                    "outlook": stock_doc.get("ai_outlook", ""), "recommendation": stock_doc.get("ai_recommendation", ""),
                    "price_zone": stock_doc.get("ai_price_zone", ""), "ai_updated_at": stock_doc.get("ai_updated_at", "Chưa phân tích"),
                    "ai_source": stock_doc.get("ai_source", "AI")
                }
                
                res = evaluate_stock_from_db(
                    sym, nd, med, grp, cfg, 
                    tech_score_raw=tech_score_raw, tech_status=tech_status, pattern=pattern, 
                    existing_ai_data=existing_ai,
                    history_bars=history_bars
                )
                if res:
                    res["date"] = today_str
                    results.append(res)
                    snap = _admin_progress_snapshot()
                    _admin_progress_update(completed=score_index, success=snap["success"] + 1)
                else:
                    snap = _admin_progress_snapshot()
                    _admin_progress_update(completed=score_index, failed=snap["failed"] + 1)
            
            if results:
                results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                
                for r in results:
                    existing_stock = stock_db.get(r["symbol"], {})
                    if existing_stock and existing_stock.get("ai_bctc") and len(existing_stock.get("ai_bctc")) > 100:
                        r["bctc_evaluation"] = existing_stock.get("ai_bctc", "")
                        r["outlook"] = existing_stock.get("ai_outlook", "")
                        r["ai_recommendation"] = existing_stock.get("ai_recommendation", "")
                        r["ai_price_zone"] = existing_stock.get("ai_price_zone", "")
                        r["macro_view"] = existing_stock.get("ai_macro_view", "")
                        r["ai_updated_at"] = existing_stock.get("ai_updated_at", "")
                        r["ai_source"] = existing_stock.get("ai_source", "AI")

                _admin_progress_update(message=f"Đã tính điểm {len(results)}/{len(norm_db)} mã; đang lưu kết quả")
                if _safe_replace_screener_results(results, tag="force-api"):
                    commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Force Screener {_now_str()}")
                    _admin_progress_update(status="success", current_symbol=None, completed=len(norm_db), message=f"Hoàn tất chấm điểm: {len(results)}/{len(norm_db)} mã")
                    print(f"[SCREENER] ✅ Đã chấm điểm xong {len(results)} mã (Đã bảo vệ bài AI cũ).", flush=True)
                else:
                    _admin_progress_update(status="error", message="Không ghi được kết quả Screener (lock timeout)")
                    print(f"[SCREENER] ⚠️ Không ghi được kết quả (lock timeout).", flush=True)
            else:
                _admin_progress_update(status="success", completed=len(norm_db), message="Không có kết quả mới để lưu")
        except Exception as e:
            _admin_progress_update(status="error", message=f"Lỗi Screener: {e}")
            print(f"[SCREENER] ❌ Lỗi: {e}", flush=True)
        finally: 
            is_screener_running = False
            
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang chấm điểm lại (tối ưu)..."})


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
    today_str = _vn_now().strftime("%Y-%m-%d")
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


# ===================== API LỊCH SỬ =====================
@app.get("/api/history-all")
def api_get_history_all():
    global _history_cache
    try:
        now = time.time()
        with _history_cache["lock"]:
            if _history_cache["data"] and (now - _history_cache["timestamp"]) < HISTORY_CACHE_TTL:
                print(f"[API] 🚀 /api/history-all: Trả cache ({len(_history_cache['data']['data'])} mã)", flush=True)
                return jsonify(_history_cache["data"])
        
        print("[API] 📊 /api/history-all: Đọc DB (50 nến/mã)...", flush=True)
        all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
        result = {}
        
        for sym in all_symbols:
            hist_doc = history_col.find_one(
                {"symbol": sym}, 
                {"_id": 0, "history_d1": {"$slice": -50}}
            )
            if hist_doc and hist_doc.get("history_d1"):
                result[sym] = {"history_d1": hist_doc["history_d1"]}
                
        response_data = {"status": "success", "count": len(result), "data": result}
        
        with _history_cache["lock"]:
            _history_cache["data"] = response_data
            _history_cache["timestamp"] = now
            
        print(f"[API] ✅ /api/history-all: Đã tải {len(result)} mã (50 nến)", flush=True)
        return jsonify(response_data)
    except Exception as e:
        log_exception("[API HISTORY ALL]", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/history-all-more")
def api_get_history_all_more():
    try:
        print("[API] 📊 /api/history-all-more: Đọc nến cũ hơn (250 nến/mã)...", flush=True)
        all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
        result = {}
        
        for sym in all_symbols:
            hist_doc = history_col.find_one({"symbol": sym}, {"_id": 0})
            if hist_doc and hist_doc.get("history_d1"):
                bars = hist_doc["history_d1"]
                older_bars = bars[:-50][-250:] if len(bars) > 50 else []
                if older_bars:
                    result[sym] = {"history_d1": older_bars}
                
        response_data = {"status": "success", "count": len(result), "data": result}
        print(f"[API] ✅ /api/history-all-more: Đã tải {len(result)} mã (250 nến cũ)", flush=True)
        return jsonify(response_data)
    except Exception as e:
        log_exception("[API HISTORY ALL MORE]", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/history-all-full")
def api_get_history_all_full():
    try:
        all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
        result = {}
        
        for sym in all_symbols:
            hist_doc = history_col.find_one({"symbol": sym}, {"_id": 0})
            if hist_doc and hist_doc.get("history_d1"):
                bars = hist_doc["history_d1"]
                older_bars = bars[:-300] if len(bars) > 300 else []
                if older_bars:
                    result[sym] = {"history_d1": older_bars}
        
        return jsonify({"status": "success", "count": len(result), "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/history")
def api_get_history():
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"status": "error", "message": "Thiếu mã"}), 400
        
    print(f"[API] 📊 Gọi API /api/history | Mã: {symbol}", flush=True)
    
    try:
        hist_doc = history_col.find_one({"symbol": symbol}, {"_id": 0})
        history_d1 = hist_doc.get("history_d1", []) if hist_doc else []
        
        live_doc = collection.find_one({"symbol": symbol}, {"_id": 0})
        
        if live_doc and live_doc.get("close", 0) > 0:
            today_live = live_doc.get("trading_date")
            
            if not today_live:
                today_live = _vn_now().strftime("%Y-%m-%d")
            
            bar_today = {
                "time": today_live,
                "open": float(live_doc.get("open", 0) or live_doc["close"]),
                "high": float(live_doc.get("high", 0) or live_doc["close"]),
                "low": float(live_doc.get("low", 0) or live_doc["close"]),
                "close": float(live_doc["close"]),
                "volume": int(live_doc.get("volume", 0) or 0)
            }
            
            existing_idx = None
            for i, b in enumerate(history_d1):
                if b.get("time") == today_live:
                    existing_idx = i
                    break
            
            if existing_idx is not None:
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
