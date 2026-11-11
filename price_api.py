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
    # Render thường chạy UTC; hiển thị log theo giờ Việt Nam (UTC+7)
    _formatter.converter = lambda ts: (
        datetime.utcfromtimestamp(ts) + timedelta(hours=7)
    ).timetuple()
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
logger.propagate = False

def log_exception(context: str, exc: Exception):
    """Ghi đầy đủ loại lỗi, nội dung lỗi và traceback."""
    logger.error(
        "%s | %s: %s",
        context,
        type(exc).__name__,
        str(exc),
        exc_info=True
    )

# ===================== VNSTOCK THẾ HỆ 6 =====================
from vnstock import Market, Fundamental

# Import thư viện PTKT của bạn
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
FHSC_API_KEY = os.environ.get("FHSC_API_KEY", "").strip()
if FHSC_API_KEY:
    os.environ["FHSC_API_KEY"] = FHSC_API_KEY
SSI_API_KEY = os.environ.get("SSI_API_KEY", "").strip()
SSI_API_SECRET = os.environ.get("SSI_API_SECRET", "").strip()
if SSI_API_KEY and SSI_API_SECRET:
    os.environ["SSI_API_KEY"] = SSI_API_KEY
    os.environ["SSI_API_SECRET"] = SSI_API_SECRET

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

SYMBOLS_JSON_FILE = "vn100_symbols.json"
SCREENER_JSON_FILE = "screener_results.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
COOLDOWN_SECONDS = 300
VOLUME_MULTIPLIER = 10

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== TELEGRAM HELPER =====================
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.warning("[TELEGRAM] HTTP %s: %s", response.status_code, response.text[:500])
    except Exception as e:
        log_exception("[TELEGRAM] Không gửi được thông báo", e)

# ===================== DATABASE & AUTO SYNC =====================
logger.info("Đang kết nối MongoDB...")
try:
    mongo_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
        appname="price_api"
    )
    ping_result = mongo_client.admin.command("ping")
    logger.info("MongoDB ping thành công: %s", ping_result.get("ok"))

    db = mongo_client["quant_db"]
    collection = db["stocks"]
    history_col = db["history_data"]
    bctc_col = db["bctc_data"]
    screener_col = db["screener_results"]
    settings_col = db["settings"]

    logger.info(
        "MongoDB đã kết nối | database=quant_db | collections=stocks, history_data, bctc_data, screener_results, settings"
    )

    if os.path.exists(SYMBOLS_JSON_FILE):
        try:
            logger.info("[MONGO INIT] Đọc danh sách mã từ %s", SYMBOLS_JSON_FILE)
            with open(SYMBOLS_JSON_FILE, "r", encoding="utf-8") as f:
                json_symbols = set(str(s).upper().strip() for s in json.load(f))

            if json_symbols:
                db_symbols = set(
                    doc["symbol"] for doc in collection.find({}, {"symbol": 1})
                    if doc.get("symbol")
                )
                symbols_to_add = json_symbols - db_symbols
                symbols_to_remove = db_symbols - json_symbols

                if symbols_to_add:
                    logger.info("[MONGO INIT] Thêm %s mã: %s", len(symbols_to_add), sorted(symbols_to_add))
                    for sym in sorted(symbols_to_add):
                        collection.insert_one({"symbol": sym})

                if symbols_to_remove:
                    logger.info("[MONGO INIT] Xóa %s mã không còn trong file: %s",
                                len(symbols_to_remove), sorted(symbols_to_remove))
                    collection.delete_many({"symbol": {"$in": list(symbols_to_remove)}})

                logger.info(
                    "[MONGO INIT] Đồng bộ danh sách hoàn tất | file=%s | database=%s",
                    len(json_symbols), len(db_symbols)
                )
            else:
                logger.warning("[MONGO INIT] File danh sách mã rỗng, bỏ qua đồng bộ.")
        except Exception as e:
            log_exception("[MONGO INIT] Lỗi đọc/đồng bộ vn100_symbols.json", e)
    else:
        logger.warning("[MONGO INIT] Không tìm thấy %s; bỏ qua đồng bộ danh sách.", SYMBOLS_JSON_FILE)

except PyMongoError as e:
    log_exception("[MONGODB STARTUP] Không thể kết nối hoặc khởi tạo MongoDB", e)
    sys.exit(1)
except Exception as e:
    log_exception("[STARTUP] Lỗi không xác định khi khởi tạo database", e)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

is_crawling = False
is_full_data_running = False
is_screener_running = False
LAST_CRAWL_TIME = {}
LAST_ALERT_BATCH = []
LAST_TELEGRAM_TIME = 0

# ===================== HELPERS =====================
def _now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M:%S")

def _get_trading_date_str() -> str:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.hour < 9: vn_now -= timedelta(days=1)
    while vn_now.weekday() >= 5: vn_now -= timedelta(days=1)
    return vn_now.strftime("%Y-%m-%d")

def _is_after_15h() -> bool:
    return (datetime.utcnow() + timedelta(hours=7)).hour >= 15

def _fetch_one(symbol: str) -> Optional[dict]:
    def _do():
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            past_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            df = Market().equity(symbol).ohlcv(
                start=past_7d, end=today, source="ssi"
            )
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    "close": float(latest["close"]),
                    "open": float(latest["open"]),
                    "high": float(latest["high"]),
                    "low": float(latest["low"]),
                    "volume": int(latest["volume"])
                }
            logger.warning("[PRICE] %s: Vnstock trả về DataFrame rỗng.", symbol)
            return None
        except Exception as e:
            log_exception(f"[PRICE] Lỗi lấy giá {symbol}", e)
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            data = fut.result(timeout=15)
        except concurrent.futures.TimeoutError as e:
            log_exception(f"[PRICE] Timeout khi lấy giá {symbol} sau 15 giây", e)
            return None
        except Exception as e:
            log_exception(f"[PRICE] Worker lấy giá lỗi với {symbol}", e)
            return None

    if not data or data["close"] <= 0 or data["open"] <= 0:
        return None

    if data["close"] >= 1000:
        for k in ["close", "open", "high", "low"]:
            if data[k] > 0:
                data[k] = data[k] / 1000.0

    if 0 < data["volume"] < 100000:
        data["volume"] = data["volume"] * VOLUME_MULTIPLIER

    return {
        "close": round(data["close"], 2),
        "open": round(data["open"], 2),
        "high": round(data["high"], 2),
        "low": round(data["low"], 2),
        "volume": int(data["volume"])
    }


# ===================== VNSTOCK TĨNH =====================
def _fetch_history_ssi(symbol: str, start_date: str, end_date: str):
    logger.info("[HISTORY FETCH] %s | %s -> %s", symbol, start_date, end_date)
    try:
        df = Market().equity(symbol).ohlcv(
            start=start_date, end=end_date, source="ssi"
        )
        if df is not None and not df.empty:
            logger.info("[HISTORY SUCCESS] %s | %s dòng | cột=%s",
                        symbol, len(df), list(df.columns))
            return df

        logger.warning("[HISTORY EMPTY] %s | Vnstock trả dữ liệu rỗng.", symbol)
    except Exception as e:
        log_exception(f"[HISTORY ERROR] Không lấy được lịch sử giá {symbol}", e)
    return None



def _fetch_bctc_ssi(symbol: str):
    """
    Lấy BCTC theo API Vnstock 6 (Nguồn FHSC).
    Tối ưu: Thêm delay, cơ chế retry khi bị RateLimit (429) và xử lý từng báo cáo độc lập.
    """
    symbol = str(symbol).upper().strip()
    started = time.time()

    logger.info("=" * 65)
    logger.info("[BCTC START] Mã=%s | thời điểm=%s", symbol, _now_str())

    try:
        equity = Fundamental().equity(symbol)
        logger.info("[BCTC INIT SUCCESS] %s", symbol)

        reports = {}
        # Danh sách 3 báo cáo tài chính chính
        report_names = ("balance_sheet", "income_statement", "cash_flow")

        def fetch_with_retry(func, period=None, source="fhsc", max_retries=3):
            """Hàm phụ trợ: Gọi API, nếu gặp lỗi 429 thì chờ và thử lại"""
            for attempt in range(max_retries):
                try:
                    if period:
                        return func(period=period, source=source)
                    else:
                        return func(source=source)
                except Exception as e:
                    err_str = str(e).lower()
                    # Bắt lỗi RateLimit (429) hoặc các lỗi liên quan đến giới hạn tần suất
                    if "429" in err_str or "rate limit" in err_str or "vượt giới hạn" in err_str:
                        wait_time = (attempt + 1) * 10  # Chờ 10s, 20s, 30s
                        logger.warning(
                            "[BCTC RATE LIMIT] %s | Bị giới hạn, đợi %ss rồi thử lại (Lần %s/%s)...",
                            symbol, wait_time, attempt + 1, max_retries
                        )
                        time.sleep(wait_time)
                    else:
                        # Nếu là lỗi khác (không phải 429) thì ném ra ngoài luôn
                        raise e
            return None

        # 1. Cào 3 báo cáo chính
        for report_name in report_names:
            report_started = time.time()
            logger.info("[BCTC REQUEST] %s | %s | period=quarter | source=fhsc", symbol, report_name)

            try:
                report_func = getattr(equity, report_name, None)
                if not callable(report_func):
                    logger.error("[BCTC UNSUPPORTED] %s | Không tìm thấy equity.%s()", symbol, report_name)
                    continue

                # Gọi hàm với cơ chế retry
                df = fetch_with_retry(report_func, period="quarter")
                
                # Nghỉ 1.5s giữa các báo cáo để tránh dồn dập
                time.sleep(1.5) 

                if df is None or getattr(df, 'empty', True):
                    logger.warning("[BCTC EMPTY] %s | %s trả về rỗng", symbol, report_name)
                    continue

                # Làm sạch dữ liệu
                clean_df = df.astype(object).where(pd.notna(df), None)
                records = json.loads(json.dumps(clean_df.to_dict(orient="records"), ensure_ascii=False, default=str))
                
                reports[report_name] = records
                logger.info("[BCTC SUCCESS] %s | %s | rows=%s | %.2fs", symbol, report_name, len(records), time.time() - report_started)

            except Exception as e:
                log_exception(f"[BCTC REPORT ERROR] {symbol} | {report_name}", e)
                continue

        # 2. Cào riêng bảng Chỉ số tài chính (ratio)
        try:
            logger.info("[BCTC REQUEST] %s | ratio | source=fhsc", symbol)
            df_ratio = fetch_with_retry(equity.ratio)
            
            if df_ratio is not None and not getattr(df_ratio, 'empty', True):
                clean_df = df_ratio.astype(object).where(pd.notna(df_ratio), None)
                records = json.loads(json.dumps(clean_df.to_dict(orient="records"), ensure_ascii=False, default=str))
                reports["ratio"] = records
                logger.info("[BCTC SUCCESS] %s | ratio | rows=%s", symbol, len(records))
        except Exception as e:
            logger.warning("[BCTC SKIP] %s | Lỗi lấy bảng ratio: %s", symbol, e)

        elapsed_total = round(time.time() - started, 2)
        if reports:
            logger.info(
                "[BCTC COMPLETE] %s | reports=%s | elapsed=%ss",
                symbol, {k: len(v) for k, v in reports.items()}, elapsed_total
            )
            return reports

        logger.error("[BCTC FAILED] %s | Không lấy được báo cáo nào", symbol)
        return None

    except Exception as e:
        log_exception(f"[BCTC FATAL ERROR] {symbol}", e)
        return None
    finally:
        logger.info("[BCTC END] %s", symbol)
# ===================== GITHUB BULK UPLOAD =====================
def get_github_file_data(file_path):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return None, None
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200:
            return json.loads(base64.b64decode(res.json()["content"]).decode("utf-8")), res.json()["sha"]
        elif res.status_code == 404:
            return {"history_d1": [], "history_w1": [], "history_mn": []}, None
        return None, None
    except Exception as e:
        log_exception(f"[GITHUB GET] Lỗi đọc file {file_path}", e)
        return None, None

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
            logger.warning("[GITHUB COMMIT] Lần thử %s/%s chưa thành công.", attempt + 1, max_retries)
            time.sleep(2)
        except Exception as e:
            log_exception(f"[GITHUB COMMIT] Lỗi lần thử {attempt + 1}/{max_retries}", e)
            time.sleep(2)
    logger.error("[GITHUB COMMIT] Thất bại sau %s lần thử.", max_retries)
    return False

# ===================== CÁC HÀM PHÂN TÍCH TÀI CHÍNH =====================
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
    pe, pb, roe, cfo, npat, ps = nd["pe"], nd["pb"], nd["roe"], nd["cfo_latest"], nd["net_profit_ttm"], nd["ps"]
    methods = {}
    med_pe, med_pb, med_roe = sector_median.get("pe") or 12.0, sector_median.get("pb") or 2.0, sector_median.get("roe") or 0.0
    cap_pe, cap_pb, mos = config["cap_pe"], config["cap_pb"], config["mos"]
    is_financial = group_label in ("NGAN_HANG", "CHUNG_KHOAN", "INSURANCE")
    if eps > 0 and med_pe > 0: methods["PE_method"] = round(eps * min(med_pe * (1.15 if (roe > 0 and roe > med_roe) else 1.0), cap_pe), 0)
    if bvps > 0 and med_pb > 0: methods["PB_method"] = round(bvps * min(med_pb, cap_pb), 0)
    if not is_financial and eps > 0 and bvps > 0: methods["Graham"] = round((22.5 * eps * bvps) ** 0.5, 0)
    if not is_financial and npat > 0 and cfo > 0 and (cfo / npat) >= 0.8 and eps > 0 and med_pe > 0: methods["CFO_boost"] = round(eps * min(med_pe, cap_pe) * 1.10, 0)
    if not methods: return {"fair_low": 0, "fair_high": 0, "graham": 0, "methods": {}, "mos_pct": round(mos * 100, 1), "rationale": "Không đủ dữ liệu.", "discount_to_fair_pct": 0, "verdict": "KHÔNG ĐỦ DỮ LIỆU"}
    weights = {"PE_method": 2, "PB_method": 1.5, "Graham": 2, "CFO_boost": 1, "PS_method": 1}
    weighted = []
    for k, v in methods.items(): weighted.extend([v] * int(weights.get(k, 1) * 2))
    weighted.sort()
    fair_high = weighted[len(weighted) // 2]
    fair_low = fair_high * (1 - mos)
    discount_to_fair = ((fair_low - close) / close * 100) if close > 0 else 0.0
    verdict = "KHÔNG CÓ GIÁ" if close <= 0 else ("RẺ - TRONG VÙNG MUA" if close <= fair_low else ("HỢP LÝ - CHỜ ĐIỀU CHỈNH" if close <= fair_high else ("HƠI ĐẮT - KHÔNG MUA" if close <= fair_high * 1.2 else "ĐẮT - TRÁNH")))
    rationale = f"[{group_label}] PE_method={methods.get('PE_method', 0):,.0f}; PB_method={methods.get('PB_method', 0):,.0f}; MoS {mos*100:.0f}%."
    return {"fair_low": round(fair_low, 0), "fair_high": round(fair_high, 0), "graham": round(methods.get("Graham", 0), 0), "methods": {k: round(v, 0) for k, v in methods.items()}, "mos_pct": round(mos * 100, 1), "rationale": rationale, "discount_to_fair_pct": round(discount_to_fair, 1), "verdict": verdict}

def compute_final_decision(fund_score, tech_score, val_score, total_score, verdict, cfo, npat, roe, debt_to_equity, discount_to_fair_pct=0.0, close_price=0.0, fair_high=0.0, fair_low=0.0):
    is_bank_or_sec = roe > 0 and debt_to_equity > 3.0
    if fund_score < 20: return ("❌ TRÁNH", "rose", f"PTCB chỉ {fund_score}/50.")
    if not is_bank_or_sec and cfo < 0 and npat > 0 and fund_score < 25: return ("⚠️ CẢNH BÁO LỢI NHUẬN ẢO", "amber", f"CFO âm.")
    if close_price > 0 and fair_high > 0 and close_price > fair_high * 1.2: return ("❌ TRÁNH", "rose", f"Giá vượt xa fair_high.")
    if close_price > 0 and fair_high > 0 and close_price > fair_high: return ("⚠️ CHỜ VỀ VÙNG MUA", "amber", f"Giá cao hơn fair_high.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 40 and tech_score >= 15 and val_score >= 24 and total_score >= 75 and discount_to_fair_pct >= 5): return ("🎯 MUA MẠNH", "emerald", "ĐỦ 4 YẾU TỐ.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 30 and tech_score >= 10 and val_score >= 18 and total_score >= 60): return ("✅ MUA TÍCH SẢN", "emerald", "ĐỦ 4 YẾU TỐ.")
    if (fund_score >= 30 and val_score >= 18 and verdict in ("HỢP LÝ - CHỜ ĐIỀU CHỈNH", "HƠI ĐẮT - KHÔNG MUA")): return ("⚠️ CHỜ VỀ VÙNG MUA", "amber", "Doanh nghiệp tốt nhưng giá chưa rẻ.")
    if (verdict == "RẺ - TRONG VÙNG MUA" and fund_score >= 30 and val_score >= 18 and tech_score < 10): return ("⏳ CHỜ XÁC NHẬN KỸ THUẬT", "amber", "Chờ tín hiệu đảo chiều.")
    return ("👀 THEO DÕI", "slate", "Chưa đủ điều kiện.")

def evaluate_stock_from_db(symbol, nd, sector_median, group_label, config):
    sector = get_sector(symbol)
    pe = nd["pe"]; pb = nd["pb"]; ps = nd["ps"]; roe = nd["roe"]; roa = nd["roa"]; dividend_yield = nd["dividend_yield"]
    close_price = nd["close_price"]; cfo_latest = nd["cfo_latest"]; net_profit_ttm = nd["net_profit_ttm"]
    debt_to_equity = nd["debt_to_equity"]; profit_growth_yoy = nd["profit_growth_yoy"]; credit_growth_yoy = nd["credit_growth_yoy"]
    
    fundamental_score = 0
    fund_notes = []
    
    if sector == "NGAN_HANG":
        if credit_growth_yoy >= 12: fundamental_score += 18; fund_notes.append("Tín dụng mạnh (>=12%)")
        elif credit_growth_yoy >= 8: fundamental_score += 10; fund_notes.append("Tín dụng khá")
        if roe >= 18: fundamental_score += 17; fund_notes.append("ROE xuất sắc (>=18%)")
        elif roe >= 14: fundamental_score += 10; fund_notes.append("ROE tốt (>=14%)")
    elif sector == "CHUNG_KHOAN":
        if profit_growth_yoy >= 20: fundamental_score += 20; fund_notes.append("LN tăng trưởng mạnh (>=20%)")
        elif profit_growth_yoy >= 10: fundamental_score += 10; fund_notes.append("LN hồi phục tốt")
        if roe >= 15: fundamental_score += 15; fund_notes.append("ROE ngành CK cao (>=15%)")
        elif roe >= 10: fundamental_score += 8; fund_notes.append("ROE ổn định")
    elif sector == "BAT_DONG_SAN":
        if profit_growth_yoy >= 10: fundamental_score += 15; fund_notes.append("LN duy trì tăng trưởng")
        if roe >= 12: fundamental_score += 20; fund_notes.append("ROE BĐS đạt chuẩn (>=12%)")
        elif roe > 0: fundamental_score += 10; fund_notes.append("Có lãi dương")
    elif sector == "CONG_NGHE_BAN_LE":
        if profit_growth_yoy >= 20: fundamental_score += 18; fund_notes.append("Tăng trưởng lõi mạnh (>=20%)")
        elif profit_growth_yoy >= 10: fundamental_score += 10; fund_notes.append("Tăng trưởng ổn định")
        if roe >= 18: fundamental_score += 17; fund_notes.append("ROE xuất sắc")
        elif roe >= 12: fundamental_score += 10; fund_notes.append("ROE tốt")
    elif sector == "DIEN_NUOC_HA_TANG":
        if dividend_yield >= 6.0: fundamental_score += 20; fund_notes.append(f"Cổ tức hấp dẫn ({dividend_yield}%)")
        elif dividend_yield > 0: fundamental_score += 10; fund_notes.append(f"Có cổ tức ({dividend_yield}%)")
        if roe >= 10: fundamental_score += 15; fund_notes.append("ROE ổn định phòng thủ")
        else: fundamental_score += 8
    elif sector == "NGUYEN_VAT_LIEU_CHU_KY":
        if profit_growth_yoy >= 15: fundamental_score += 18; fund_notes.append("Hưởng lợi chu kỳ")
        elif profit_growth_yoy >= 0: fundamental_score += 10; fund_notes.append("Duy trì nhịp cơ bản")
        else: fundamental_score -= 15; fund_notes.append("⚠️ LNST tăng trưởng âm (suy thoái chu kỳ)")
        if roe >= 12: fundamental_score += 17; fund_notes.append("ROE chu kỳ tốt")
        else: fundamental_score += 8
    else:
        if profit_growth_yoy >= 10: fundamental_score += 15; fund_notes.append("LN tăng trưởng tốt")
        elif profit_growth_yoy >= 0: fundamental_score += 8; fund_notes.append("LN ổn định")
        else: fundamental_score -= 10; fund_notes.append("⚠️ LNST tăng trưởng âm")
        if roe >= 14: fundamental_score += 20; fund_notes.append("ROE sản xuất cao (>=14%)")
        elif roe >= 10: fundamental_score += 10; fund_notes.append("ROE khá")
        
    if roa >= 5.0: fundamental_score += 5; fund_notes.append(f"ROA cao ({roa}%)")
    elif roa >= 2.5: fundamental_score += 3; fund_notes.append(f"ROA tốt ({roa}%)")

    if sector not in ["NGAN_HANG", "CHUNG_KHOAN"]:
        if cfo_latest > 0 and (net_profit_ttm <= 0 or cfo_latest >= net_profit_ttm * 0.8): fundamental_score += 5; fund_notes.append("CFO mạnh mẽ")
        elif cfo_latest < 0: fundamental_score -= 5; fund_notes.append("⚠️ CFO âm")
        if 0 < debt_to_equity <= 1.0: fundamental_score += 5; fund_notes.append(f"D/E an toàn ({debt_to_equity})")
        elif debt_to_equity > 2.5: fundamental_score -= 5; fund_notes.append(f"⚠️ D/E cao ({debt_to_equity})")
    else:
        fundamental_score += 5
        fund_notes.append("Ngân hàng tối ưu vốn")

    fundamental_score = int(np.clip(fundamental_score, 0, 50))

    valuation_score = 0
    if sector == "BAT_DONG_SAN":
        if (0 < pb < 1.5) or (0 < ps < 2.0): valuation_score = 30
        elif (0 < pb < 2.2) or (0 < ps < 3.5): valuation_score = 18
        else: valuation_score = 8
    elif sector == "NGUYEN_VAT_LIEU_CHU_KY":
        if (0 < ps < 1.2) or (0 < pe < 12): valuation_score = 30
        elif (0 < ps < 2.0) or (0 < pe < 18): valuation_score = 18
        else: valuation_score = 8
    elif sector == "CONG_NGHE_BAN_LE":
        if 0 < pe < 18 or (profit_growth_yoy > 20 and pe < 25): valuation_score = 30
        elif 0 < pe < 25: valuation_score = 18
        else: valuation_score = 8
    elif sector in ["NGAN_HANG", "CHUNG_KHOAN"]:
        if 0 < pb < 1.3 or (roe >= 16 and pb < 1.8): valuation_score = 30
        elif 0 < pb < 2.0: valuation_score = 18
        else: valuation_score = 8
    else:
        if 0 < pe < 15 or dividend_yield >= 5.0: valuation_score = 30
        elif 0 < pe < 20 or dividend_yield >= 3.0: valuation_score = 18
        else: valuation_score = 8
    valuation_score = int(np.clip(valuation_score, 0, 30))

    technical_score_raw = 50
    technical_status = "Tích lũy sideway"
    pattern = "Tích lũy"
    volume_analysis = "Chưa phân tích volume"

    if ProfessionalTechnicalAnalyzer:
        hist_data = history_col.find_one({"symbol": symbol})
        if hist_data and hist_data.get("history_d1"):
            try:
                analyzer = ProfessionalTechnicalAnalyzer(hist_data["history_d1"]) 
                ptkt_res = analyzer.analyze()
                technical_score_raw = ptkt_res.get("technical_score", 50)
                technical_status = ptkt_res.get("technical_status", "Tích lũy sideway")
                pattern = ptkt_res.get("pattern", "Tích lũy")
                volume_analysis = ptkt_res.get("volume_analysis", "")
            except Exception as e:
                print(f"⚠️ Lỗi PTKT {symbol}: {e}")

    technical_score = int(round((technical_score_raw / 100.0) * 20))
    technical_score = int(np.clip(technical_score, 0, 20))

    total_score = fundamental_score + valuation_score + technical_score

    potential_zone = "THEO DÕI THÊM"
    if total_score >= 75: potential_zone = "VÙNG TÍCH SẢN TỐI ƯU (A+)"
    elif total_score >= 60: potential_zone = "VÙNG MUA AN TOÀN (B)"

    fv = compute_fair_value_zone(symbol, nd, sector_median, config, group_label)

    final_decision, final_color, final_reason = compute_final_decision(
        fund_score=fundamental_score, tech_score=technical_score, val_score=valuation_score, total_score=total_score,
        verdict=fv["verdict"], cfo=cfo_latest, npat=net_profit_ttm, roe=roe, debt_to_equity=debt_to_equity,
        discount_to_fair_pct=fv["discount_to_fair_pct"], close_price=close_price, fair_high=fv["fair_high"], fair_low=fv["fair_low"]
    )

    return {
        "symbol": symbol, "sector": sector, "sub_sector": group_label, "close_price": close_price, "pe": pe, "pb": pb, "ps": ps,
        "roe": roe, "roa": roa, "eps": round(nd["eps"], 0), "bvps": round(nd["bvps"], 0), "dividend_yield": dividend_yield, "profit_growth_yoy": profit_growth_yoy,
        "cfo_latest": cfo_latest, "debt_to_equity": debt_to_equity, "gross_margin": nd["gross_margin"],
        "fundamental_score": fundamental_score, "valuation_score": valuation_score, "technical_score": technical_score, "total_score": total_score,
        "potential_zone": potential_zone, "pattern": pattern, "technical_status": technical_status, "volume_analysis": volume_analysis,
        "fundamental_notes": " | ".join(fund_notes), "fair_value_high": fv["fair_high"], "fair_value_low": fv["fair_low"], "graham_number": fv["graham"],
        "margin_of_safety_pct": fv["mos_pct"], "fair_value_methods": fv["methods"], "fair_value_rationale": fv["rationale"],
        "discount_to_fair_pct": fv["discount_to_fair_pct"], "valuation_verdict": fv["verdict"], "final_decision": final_decision,
        "final_decision_color": final_color, "final_decision_reason": final_reason,
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

# ===================== CORE CRAWL LOGIC =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling, LAST_ALERT_BATCH, LAST_TELEGRAM_TIME
    if is_crawling: return
    is_crawling = True
    try:
        trading_date = _get_trading_date_str()
        mode_str = "CÀO BÙ" if is_retry else ("CÀO TAY" if is_manual else "REAL-TIME")
        print(f"\n[CRAWL] 🚀 [{mode_str}] Bắt đầu xử lý {len(symbols)} mã (Ngày: {trading_date})...", flush=True)
        alert_batch = []
        all_crawled_data = {}
        pending_history_files = {}
        need_update_screener = False

        for chunk_idx in range(0, len(symbols), 5):
            chunk = symbols[chunk_idx:chunk_idx+5]
            for sym in chunk:
                doc = collection.find_one({"symbol": sym})
                if doc and doc.get("chot_so_today") == trading_date: continue
                data = _fetch_one(sym)
                if data:
                    hist_data, _ = get_github_file_data(f"history/{sym}.json")
                    prev_close, avg_volume_20 = 0.0, 0.0
                    if hist_data and hist_data.get("history_d1"):
                        bars = hist_data["history_d1"]
                        if len(bars) >= 2: prev_close = bars[-2]["close"] if bars[-1]["time"] == trading_date else bars[-1]["close"]
                        elif len(bars) == 1: prev_close = bars[0]["close"]
                        valid_vols = [b.get("volume", 0) for b in bars[-20:] if b.get("volume", 0) > 0]
                        if valid_vols: avg_volume_20 = sum(valid_vols) / len(valid_vols)
                    amplitude_percent = round((data["high"] - data["low"]) / data["open"] * 100, 2) if data["open"] > 0 else 0
                    volume_percent = round(data["volume"] / avg_volume_20 * 100, 2) if avg_volume_20 > 0 else 0
                    pct_change = round(((data["close"] - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0
                    all_crawled_data[sym] = {"close": data["close"], "pct_change": pct_change}
                    real_time_str = _now_str()
                    collection.update_one({"symbol": sym}, {"$set": {
                        "close": data["close"], "prev_close": prev_close, "open": data["open"], "high": data["high"], "low": data["low"], "volume": data["volume"],
                        "amplitude_percent": amplitude_percent, "volume_percent": volume_percent, "avg_volume_20": round(avg_volume_20, 2),
                        "trading_date": trading_date, "updated_at": real_time_str, "is_locked": _is_after_15h(), "is_incomplete": False
                    }}, upsert=True)
                    LAST_CRAWL_TIME[sym] = time.time()
                    print(f"  ✅ [OK] {sym}: Giá {data['close']:,} | KL: {data['volume']:,} | Vol%: {volume_percent}% (Lúc: {real_time_str})", flush=True)
                    if prev_close > 0 and abs(pct_change) >= 3.0 and volume_percent >= 120.0:
                        alert_batch.append({"sym": sym, "pct_change": pct_change, "volume_percent": volume_percent, "price": data["close"], "volume": data["volume"], "trend": "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG", "flow": "BÁN RÒNG" if pct_change < 0 else "MUA VÀO"})
                    if _is_after_15h() and hist_data is not None:
                        d1_bars = hist_data.get("history_d1", [])
                        d1_bar = {"time": trading_date, "open": data["open"], "high": data["high"], "low": data["low"], "close": data["close"], "volume": data["volume"]}
                        d1_idx = next((i for i, b in enumerate(d1_bars) if b["time"] == trading_date), -1)
                        if d1_idx == -1: d1_bars.append(d1_bar)
                        else: d1_bars[d1_idx] = d1_bar
                        d1_bars.sort(key=lambda x: x["time"])
                        hist_data["history_d1"] = d1_bars
                        pending_history_files[f"history/{sym}.json"] = hist_data
                        need_update_screener = True
                else:
                    print(f"  ❌ [LỖI] {sym}: Không lấy được dữ liệu.", flush=True)
                    collection.update_one({"symbol": sym}, {"$set": {"is_incomplete": True, "last_error_at": _now_str()}}, upsert=True)
            time.sleep(1)

        if _is_after_15h() and pending_history_files:
            print(f"\n[GITHUB] 📦 Đẩy {len(pending_history_files)} file...", flush=True)
            if need_update_screener and all_crawled_data:
                try:
                    res = requests.get(f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{SCREENER_JSON_FILE}?ref={GITHUB_BRANCH}", headers={"Authorization": f"token {GITHUB_TOKEN}"}, timeout=10)
                    if res.status_code == 200:
                        current_json = json.loads(base64.b64decode(res.json()["content"]).decode("utf-8"))
                        for item in current_json:
                            sym = item.get("symbol")
                            if sym in all_crawled_data:
                                cdata = all_crawled_data[sym]
                                item["close_price"] = cdata["close"] * 1000
                                item["price_change_percent"] = cdata["pct_change"]
                        pending_history_files[SCREENER_JSON_FILE] = current_json
                except Exception as e:
                    log_exception("[CRAWL] Không đọc/cập nhật được screener JSON từ GitHub", e)
            if commit_multiple_files_to_github(pending_history_files, f"Cập nhật dữ liệu {trading_date}"):
                print(f"[GITHUB] ✅ Đẩy Github thành công!", flush=True)
                for path in pending_history_files:
                    if path.startswith("history/"):
                        collection.update_one({"symbol": path.split("/")[1].replace(".json", "")}, {"$set": {"chot_so_today": trading_date, "is_locked": True}})

        if alert_batch:
            alert_batch.sort(key=lambda x: x["volume_percent"], reverse=True)
            msg_lines = [f"🚨 <b>CẢNH BÁO DÒNG TIỀN ({len(alert_batch)} MÃ)</b>"]
            for item in alert_batch: msg_lines.append(f"• <b>{item['sym']}</b>: {item['pct_change']:+.2f}% {item['trend']} | Vol: {item['volume_percent']:.0f}%")
            send_telegram("\n".join(msg_lines))
            print(f"[TELEGRAM] 🚀 Đã gửi cảnh báo {len(alert_batch)} mã.", flush=True)
    finally:
        is_crawling = False
        print("[CRAWL] 🏁 Đã kết thúc đợt cào.\n", flush=True)

# ===================== LUỒNG FULL DATA =====================
def full_data_crawler_worker():
    print("⏰ [FULL-DATA] Đã bật luồng cào dữ liệu tĩnh vào MongoDB (16h00).", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            if vn_now.hour == 16 and vn_now.minute == 0:
                print("\n[FULL-DATA] 🚀 Bắt đầu cập nhật Nến & BCTC...", flush=True)
                target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                end_date = datetime.now().strftime("%Y-%m-%d")
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                for i, symbol in enumerate(target_symbols):
                    new_bctc = _fetch_bctc_ssi(symbol)
                    if new_bctc:
                        new_bctc["symbol"] = symbol
                        new_bctc["updated_at"] = _now_str()
                        try:
                            logger.info(
                                "[MONGO BCTC WRITE] %s | keys=%s | rows=%s",
                                symbol,
                                list(new_bctc.keys()),
                                {k: len(v) for k, v in new_bctc.items() if isinstance(v, list)}
                            )
                            mongo_result = bctc_col.update_one(
                                {"symbol": symbol}, {"$set": new_bctc}, upsert=True
                            )
                            logger.info(
                                "[MONGO BCTC SUCCESS] %s | matched=%s | modified=%s | upserted_id=%s",
                                symbol, mongo_result.matched_count,
                                mongo_result.modified_count, mongo_result.upserted_id
                            )
                        except PyMongoError as e:
                            log_exception(f"[MONGO BCTC ERROR] Không lưu được BCTC {symbol}", e)
                        except Exception as e:
                            log_exception(f"[MONGO BCTC SERIALIZATION ERROR] Dữ liệu BCTC {symbol} không ghi được", e)
                    df = _fetch_history_ssi(symbol, start_date, end_date)
                    if df is not None and not df.empty:
                        new_bars = []
                        for _, row in df.iterrows():
                            time_str = row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time'])[:10]
                            new_bars.append({"time": time_str, "open": float(row['open']), "high": float(row['high']), "low": float(row['low']), "close": float(row['close']), "volume": int(row['volume'])})
                        for bar in new_bars:
                            history_col.update_one({"symbol": symbol}, {"$pull": {"history_d1": {"time": bar["time"]}}})
                        history_col.update_one({"symbol": symbol}, {"$push": {"history_d1": {"$each": new_bars, "$slice": -2500}}}, upsert=True)
                        doc_hist = history_col.find_one({"symbol": symbol})
                        if doc_hist and doc_hist.get("history_d1"):
                            d1_data = sorted(doc_hist["history_d1"], key=lambda x: x["time"])
                            history_col.update_one({"symbol": symbol}, {"$set": {"history_w1": aggregate_candles(d1_data, 'W1'), "history_mn": aggregate_candles(d1_data, 'MN')}})
                    time.sleep(1.5)
                print(f"[FULL-DATA] ✅ Đã cập nhật xong Nến & BCTC.", flush=True)
                time.sleep(120) 
            time.sleep(30)
        except Exception as e:
            log_exception("[FULL-DATA] Worker gặp lỗi", e)
            time.sleep(60)

# ===================== LUỒNG SCREENER =====================
def screener_worker():
    print("⏰ [SCREENER] Đã bật luồng chấm điểm Quant (16h30).", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            if vn_now.hour == 16 and vn_now.minute == 30:
                print("\n[SCREENER] 🚀 Bắt đầu phân tích và chấm điểm...", flush=True)
                stock_db = {}
                for doc in collection.find({}):
                    sym = doc["symbol"]
                    bctc = bctc_col.find_one({"symbol": sym}, {"_id": 0})
                    # Cấp dữ liệu kết quả kinh doanh cho bộ chuẩn hóa hiện tại.
                    doc["bctc_history"] = (
                        bctc.get("income_statement", []) if bctc else []
                    )
                    logger.info(
                        "[SCREENER BCTC] %s | có_bctc=%s | income_statement_rows=%s",
                        sym, bool(bctc), len(doc["bctc_history"])
                    )
                    stock_db[sym] = doc
                normalized_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
                sector_medians = compute_sector_medians(normalized_db)
                results = []
                for symbol, nd in normalized_db.items():
                    sector = get_sector(symbol)
                    group_label = get_sub_sector(symbol)
                    config = SUB_SECTOR_CONFIG.get(group_label) or SUB_SECTOR_CONFIG.get(sector)
                    median, _ = get_median_for_symbol(symbol, sector_medians)
                    res = evaluate_stock_from_db(symbol, nd, median, group_label, config)
                    if res: results.append(res)
                if results:
                    results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                    screener_col.delete_many({}) 
                    screener_col.insert_many(results) 
                    print(f"[SCREENER] ✅ Đã lưu {len(results)} mã vào MongoDB.", flush=True)
                    commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Update Screener {_now_str()}")
                    print(f"[SCREENER] ✅ Đã đẩy file JSON lên Github.", flush=True)
                time.sleep(120) 
            time.sleep(30)
        except Exception as e:
            log_exception("[SCREENER] Worker gặp lỗi", e)
            time.sleep(60)

# ===================== BACKGROUND WORKER =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật chế độ cào cuốn chiếu Real-time (tự ngủ sau 15h)!", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            current_hour = vn_now.hour
            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            total_symbols = len(all_symbols)
            crawled_count = collection.count_documents({"chot_so_today": today_str})
            if current_hour >= 15 and crawled_count >= total_symbols:
                tomorrow = vn_now + timedelta(days=1)
                wake_up_time = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                sleep_seconds = (wake_up_time - vn_now).total_seconds()
                if sleep_seconds > 0:
                    print(f"🌙 [AUTO] Đã chốt sổ xong {crawled_count}/{total_symbols} mã. Tạm nghỉ đến {wake_up_time.strftime('%H:%M %d/%m')}...", flush=True)
                    time.sleep(min(sleep_seconds, 3600)) 
                    continue
            if not is_crawling:
                symbols_to_crawl = []
                for sym in all_symbols:
                    doc = collection.find_one({"symbol": sym}, {"chot_so_today": 1})
                    if not doc or doc.get("chot_so_today") != today_str: symbols_to_crawl.append(sym)
                if not symbols_to_crawl: symbols_to_crawl = all_symbols 
                _execute_crawl(symbols_to_crawl, is_manual=False)
            time.sleep(15)
        except Exception as e:
            log_exception("[AUTO] Worker gặp lỗi", e)
            time.sleep(10)

try:
    import vnstock
    logger.info("[STARTUP] Python=%s | vnstock=%s | pandas=%s | numpy=%s",
                sys.version.split()[0],
                getattr(vnstock, "__version__", "không xác định"),
                pd.__version__, np.__version__)
except Exception as e:
    log_exception("[STARTUP] Không đọc được thông tin phiên bản thư viện", e)

threading.Thread(target=auto_crawler_worker, daemon=True).start()
threading.Thread(target=full_data_crawler_worker, daemon=True).start()
threading.Thread(target=screener_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/")
def home(): return "API is running!", 200

@app.get("/api/status")
def api_status():
    return jsonify({"status": "success", "is_crawling": is_crawling, "message": "Đang cào real-time" if is_crawling else "Sẵn sàng"})

@app.get("/api/screener")
def api_get_screener():
    try:
        data = list(screener_col.find({}, {"_id": 0}))
        return jsonify({"status": "success", "count": len(data), "data": data})
    except Exception as e:
        log_exception("[API SCREENER] Lỗi đọc Screener", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/bctc")
def api_get_bctc():
    """Kiểm tra trực tiếp dữ liệu BCTC đã lưu trong MongoDB."""
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"status": "error", "message": "Thiếu mã cổ phiếu"}), 400

    try:
        doc = bctc_col.find_one({"symbol": symbol}, {"_id": 0})
        if not doc:
            logger.warning("[API BCTC] Không tìm thấy dữ liệu cho %s", symbol)
            return jsonify({
                "status": "error",
                "message": f"Chưa có BCTC của {symbol}"
            }), 404

        logger.info(
            "[API BCTC] Trả dữ liệu %s | keys=%s",
            symbol, list(doc.keys())
        )
        return jsonify({"status": "success", "symbol": symbol, "data": doc})
    except PyMongoError as e:
        log_exception(f"[API BCTC] MongoDB lỗi khi đọc {symbol}", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        log_exception(f"[API BCTC] Lỗi không xác định khi đọc {symbol}", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/history")
def api_get_history():
    symbol = request.args.get("symbol", "").upper().strip()
    doc = history_col.find_one({"symbol": symbol}, {"_id": 0})
    if not doc: return jsonify({"status": "error", "message": "Chưa có dữ liệu"}), 404
    return jsonify({"status": "success", "history_d1": sorted(doc.get("history_d1", []), key=lambda x: x["time"]), "history_w1": doc.get("history_w1", []), "history_mn": doc.get("history_mn", [])})

@app.get("/api/data")
def api_get_data():
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
                close_price = doc.get("close", 0)
                prev_close = doc.get("prev_close", 0)
                doc["close"] = close_price; doc["close_price"] = close_price; doc["prev_close"] = prev_close
                pct, avail, arrow, color = 0.0, False, "—", "slate"
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        pct = round(raw_pct, 2); avail = True
                        if pct > 0: arrow, color = "▲", "emerald"
                        elif pct < 0: arrow, color = "▼", "rose"
                doc["price_change_percent"] = pct; doc["price_change_available"] = avail
                doc["price_arrow"] = arrow; doc["price_color"] = color
                doc["amplitude_percent"] = doc.get("amplitude_percent", 0.0); doc["volume_percent"] = doc.get("volume_percent", 0.0)
                data[sym] = doc
        return jsonify({"status": "success", "count": len(data), "data": data})
    except PyMongoError as e:
        log_exception("[API DATA] MongoDB lỗi khi đọc dữ liệu cổ phiếu", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        log_exception("[API DATA] Lỗi không xác định", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/sync")
def api_sync():
    raw = request.args.get("symbols", "").strip()
    if not raw: abort(400)
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if is_crawling:
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc: updated_data[sym] = doc["close"]
        return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào ngầm)", "updated_data": updated_data})
    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    return jsonify({"status": "success", "updated_at": _now_str() + " (Bắt đầu cào)", "updated_data": {}})

@app.get("/api/reset-today")
def reset_today():
    today_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")
    result = collection.update_many({"chot_so_today": today_str}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
    return jsonify({"status": "success", "message": f"Đã xóa cờ chốt sổ của {result.modified_count} mã."})

@app.get("/api/force-fix")
def force_fix_data():
    collection.update_many({}, {"$unset": {"chot_so_today": "", "is_locked": "", "trading_date": ""}})
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    return jsonify({"status": "success", "message": f"Đang ép cào lại toàn bộ {len(all_symbols)} mã."})

@app.get("/api/force-full-data")
def force_full_data():
    """Ép Server cào ngay BCTC và Nến 30 ngày (dành cho việc test)"""
    global is_full_data_running
    if is_full_data_running:
        return jsonify({"status": "error", "message": "Tiến trình cào dữ liệu tĩnh đang chạy, vui lòng đợi!"})
    
    # Đánh dấu trạng thái để tránh bấm 2 lần
    is_full_data_running = True
    
    def _run_force():
        global is_full_data_running
        try:
            print("\n[MANUAL] 🚀 Ép cào BCTC & Nến 30 ngày cho toàn bộ danh sách...", flush=True)
            target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            for i, symbol in enumerate(target_symbols):
                print(f"  [{i+1}/{len(target_symbols)}] Xử lý tĩnh: {symbol}...", flush=True)
                
                # 1. Cào BCTC
                new_bctc = _fetch_bctc_ssi(symbol)
                if new_bctc:
                    new_bctc["symbol"] = symbol
                    new_bctc["updated_at"] = _now_str()
                    try:
                        logger.info(
                            "[MONGO BCTC WRITE] %s | keys=%s | rows=%s",
                            symbol,
                            list(new_bctc.keys()),
                            {k: len(v) for k, v in new_bctc.items() if isinstance(v, list)}
                        )
                        mongo_result = bctc_col.update_one(
                            {"symbol": symbol}, {"$set": new_bctc}, upsert=True
                        )
                        logger.info(
                            "[MONGO BCTC SUCCESS] %s | matched=%s | modified=%s | upserted_id=%s",
                            symbol, mongo_result.matched_count,
                            mongo_result.modified_count, mongo_result.upserted_id
                        )
                        print(f"    ✅ [BCTC] {symbol} thành công.", flush=True)
                    except PyMongoError as e:
                        log_exception(f"[MONGO BCTC ERROR] Không lưu được BCTC {symbol}", e)
                
                # 2. Cào Nến 30 ngày
                df = _fetch_history_ssi(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    new_bars = []
                    for _, row in df.iterrows():
                        time_str = row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time'])[:10]
                        new_bars.append({"time": time_str, "open": float(row['open']), "high": float(row['high']), "low": float(row['low']), "close": float(row['close']), "volume": int(row['volume'])})
                    
                    for bar in new_bars:
                        history_col.update_one({"symbol": symbol}, {"$pull": {"history_d1": {"time": bar["time"]}}})
                    history_col.update_one({"symbol": symbol}, {"$push": {"history_d1": {"$each": new_bars, "$slice": -2500}}}, upsert=True)
                    
                    doc_hist = history_col.find_one({"symbol": symbol})
                    if doc_hist and doc_hist.get("history_d1"):
                        d1_data = sorted(doc_hist["history_d1"], key=lambda x: x["time"])
                        history_col.update_one({"symbol": symbol}, {"$set": {"history_w1": aggregate_candles(d1_data, 'W1'), "history_mn": aggregate_candles(d1_data, 'MN')}})
                    print(f"    ✅ [NẾN] {symbol} thành công.", flush=True)
                
                time.sleep(1.5) # Nghỉ 1.5s tránh SSI block
                
            print("[MANUAL] 🏁 Đã cào xong toàn bộ BCTC & Nến.", flush=True)
        except Exception as e:
            log_exception("[MANUAL FULL-DATA] Tiến trình thủ công gặp lỗi", e)
        finally:
            is_full_data_running = False

    threading.Thread(target=_run_force, daemon=True).start()
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào BCTC & Nến cho {collection.count_documents({})} mã. Xem log Render để theo dõi!"})


@app.get("/api/force-screener")
def force_screener():
    """Ép Server chấm điểm lại Screener ngay lập tức (dành cho việc test)"""
    global is_screener_running
    if is_screener_running:
        return jsonify({"status": "error", "message": "Tiến trình chấm điểm đang chạy, vui lòng đợi!"})
    
    is_screener_running = True
    
    def _run_screener():
        global is_screener_running
        try:
            print("\n[MANUAL] 🚀 Ép chấm điểm lại Screener...", flush=True)
            stock_db = {}
            for doc in collection.find({}):
                sym = doc["symbol"]
                bctc = bctc_col.find_one({"symbol": sym}, {"_id": 0})
                doc["bctc_history"] = (
                    bctc.get("income_statement", []) if bctc else []
                )
                logger.info(
                    "[SCREENER BCTC] %s | có_bctc=%s | income_statement_rows=%s",
                    sym, bool(bctc), len(doc["bctc_history"])
                )
                stock_db[sym] = doc
                
            normalized_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
            sector_medians = compute_sector_medians(normalized_db)
            
            results = []
            for symbol, nd in normalized_db.items():
                sector = get_sector(symbol)
                group_label = get_sub_sector(symbol)
                config = SUB_SECTOR_CONFIG.get(group_label) or SUB_SECTOR_CONFIG.get(sector)
                median, _ = get_median_for_symbol(symbol, sector_medians)
                res = evaluate_stock_from_db(symbol, nd, median, group_label, config)
                if res: results.append(res)
                
            if results:
                results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                screener_col.delete_many({}) 
                screener_col.insert_many(results) 
                print(f"[MANUAL] ✅ Đã lưu {len(results)} mã vào MongoDB.", flush=True)
                
                commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Force Update Screener {_now_str()}")
                print(f"[MANUAL] ✅ Đã đẩy file JSON lên Github.", flush=True)
        except Exception as e:
            log_exception("[MANUAL SCREENER] Tiến trình thủ công gặp lỗi", e)
        finally:
            is_screener_running = False

    threading.Thread(target=_run_screener, daemon=True).start()
    return jsonify({"status": "success", "message": "Đã kích hoạt chấm điểm lại Screener. Xem log Render để theo dõi!"})
@app.get("/api/crawl-full-history")
def crawl_full_history():
    """Ép Server cào TOÀN BỘ 10 NĂM lịch sử nến cho tất cả các mã (Dùng khi mới setup DB)"""
    global is_full_data_running
    if is_full_data_running:
        return jsonify({"status": "error", "message": "Tiến trình cào dữ liệu tĩnh đang chạy, vui lòng đợi!"})
    
    is_full_data_running = True
    
    def _run_history():
        global is_full_data_running
        try:
            print("\n[MANUAL] 🚀 Ép cào TOÀN BỘ 10 NĂM lịch sử giá...", flush=True)
            target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            end_date = datetime.now().strftime("%Y-%m-%d")
            # Lùi lại 10 năm (3650 ngày)
            start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
            
            for i, symbol in enumerate(target_symbols):
                print(f"  [{i+1}/{len(target_symbols)}] Cào 10 năm nến: {symbol}...", flush=True)
                
                df = _fetch_history_ssi(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    new_bars = []
                    for _, row in df.iterrows():
                        time_str = row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time'])[:10]
                        new_bars.append({
                            "time": time_str, 
                            "open": float(row['open']), 
                            "high": float(row['high']), 
                            "low": float(row['low']), 
                            "close": float(row['close']), 
                            "volume": int(row['volume'])
                        })
                    
                    # Ghi đè toàn bộ mảng history_d1 bằng dữ liệu 10 năm mới cào
                    doc_hist = history_col.find_one({"symbol": symbol})
                    if doc_hist:
                        history_col.update_one({"symbol": symbol}, {"$set": {
                            "history_d1": new_bars,
                            "history_w1": aggregate_candles(new_bars, 'W1'),
                            "history_mn": aggregate_candles(new_bars, 'MN')
                        }})
                    else:
                        history_col.insert_one({
                            "symbol": symbol,
                            "history_d1": new_bars,
                            "history_w1": aggregate_candles(new_bars, 'W1'),
                            "history_mn": aggregate_candles(new_bars, 'MN')
                        })
                    print(f"    ✅ [NẾN 10 NĂM] {symbol} thành công ({len(new_bars)} phiên).", flush=True)
                else:
                    print(f"    ❌ [NẾN 10 NĂM] {symbol} thất bại.", flush=True)
                
                time.sleep(1.5) # Nghỉ 1.5s tránh SSI block
                
            print("[MANUAL] 🏁 Đã cào xong toàn bộ lịch sử 10 năm cho tất cả các mã.", flush=True)
        except Exception as e:
            log_exception("[MANUAL HISTORY] Tiến trình lịch sử 10 năm gặp lỗi", e)
        finally:
            is_full_data_running = False

    threading.Thread(target=_run_history, daemon=True).start()
    return jsonify({"status": "success", "message": "Đã kích hoạt cào 10 năm lịch sử cho toàn bộ mã. Hãy xem Log trên Render!"})
@app.get("/api/retry-now")
def retry_now():
    if is_crawling: return jsonify({"status": "error", "message": "Hệ thống đang chạy, vui lòng đợi!"})
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    if not incomplete_symbols: return jsonify({"status": "success", "message": "Không có mã nào bị lỗi!"})
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào bù {len(incomplete_symbols)} mã!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
