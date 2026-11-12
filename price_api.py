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
from vnstock import Market, Fundamental

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

# Ép nạp biến môi trường SSI (BẮT BUỘC ĐỂ LẤY ĐƯỢC GIÁ REALTIME)
SSI_API_KEY = os.environ.get("SSI_API_KEY", "").strip()
SSI_API_SECRET = os.environ.get("SSI_API_SECRET", "").strip()
if SSI_API_KEY and SSI_API_SECRET:
    os.environ["SSI_API_KEY"] = SSI_API_KEY
    os.environ["SSI_API_SECRET"] = SSI_API_SECRET
else:
    logger.warning("⚠️ CẢNH BÁO: Thiếu SSI_API_KEY hoặc SSI_API_SECRET. Có thể không lấy được giá realtime!")

ENV_FHSC_API_KEY = os.environ.get("FHSC_API_KEY", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

SYMBOLS_JSON_FILE = "vn100_symbols.json"
SCREENER_JSON_FILE = "screener_results.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

VOLUME_MULTIPLIER = 10
_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

ALIBABA_API_KEY = os.environ.get("ALIBABA_API_KEY", "").strip()
AI_API_URL = "https://ws-8lcqaxwxdv0h9xzy.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions"
AI_MODEL_NAME = "qwen3.7-max"
AI_TEMPERATURE = 0.15

# ===================== DATABASE =====================
logger.info("Đang kết nối MongoDB...")
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000, socketTimeoutMS=20000, appname="price_api")
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

is_crawling = False
is_full_data_running = False
is_screener_running = False
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
    return (datetime.utcnow() + timedelta(hours=7)).hour >= 15

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

# ===================== CÀO GIÁ (ĐÃ KHÔI PHỤC LOGIC CHUẨN) =====================
def _fetch_one(symbol: str) -> Optional[dict]:
    print(f"    [FETCH] Đang lấy giá {symbol}...", flush=True)
    def _do():
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            past_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            df = Market().equity(symbol).ohlcv(start=past_7d, end=today, source="ssi")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {"close": float(latest["close"]), "open": float(latest["open"]), "high": float(latest["high"]), "low": float(latest["low"]), "volume": int(latest["volume"])}
            return None
        except Exception as e:
            print(f"    ❌ [FETCH ERROR] Lỗi gọi Vnstock cho {symbol}: {e}", flush=True)
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        try: data = ex.submit(_do).result(timeout=15)
        except concurrent.futures.TimeoutError:
            print(f"    ❌ [FETCH TIMEOUT] Quá 15s không lấy được giá {symbol}", flush=True)
            return None
        except Exception as e:
            print(f"    ❌ [FETCH THREAD ERROR] {symbol}: {e}", flush=True)
            return None

    if not data or data["close"] <= 0 or data["open"] <= 0: return None
    if data["close"] >= 1000:
        for k in ["close", "open", "high", "low"]: data[k] /= 1000.0
    if 0 < data["volume"] < 100000: data["volume"] *= VOLUME_MULTIPLIER
    return data

def _fetch_history_ssi(symbol: str, start_date: str, end_date: str):
    try:
        df = Market().equity(symbol).ohlcv(start=start_date, end=end_date, source="ssi")
        return df if df is not None and not df.empty else None
    except Exception: return None

# ===================== CÀO BCTC (CẬP NHẬT NGÀY THÁNG & LOG CHI TIẾT) =====================
def _fetch_bctc_ssi(symbol: str):
    symbol = str(symbol).upper().strip()
    fhsc_key = get_current_fhsc_key()
    if not fhsc_key: 
        print(f"  ⚠️ [BCTC] Bỏ qua {symbol} - Chưa có FHSC API KEY", flush=True)
        return None
        
    os.environ["FHSC_API_KEY"] = fhsc_key
    print(f"  [BCTC] 🚀 Bắt đầu lấy dữ liệu cho mã {symbol}...", flush=True)

    try:
        equity = Fundamental().equity(symbol)
        reports = {}
        report_names = ("balance_sheet", "income_statement", "cash_flow")

        def fetch_with_retry(func, period="quarter", max_retries=3, limit=8):
            for attempt in range(max_retries):
                try: return func(period=period, source="fhsc", limit=limit)
                except Exception as e:
                    err_str = str(e).lower()
                    if any(k in err_str for k in ["429", "rate limit", "vượt giới hạn"]): 
                        time.sleep((attempt + 1) * 10)
                    elif any(k in err_str for k in ["networkerror", "remoteprotocolerror", "connection", "kết nối", "timeout"]): 
                        time.sleep((attempt + 1) * 5)
                    else: 
                        print(f"    ❌ [BCTC ERROR] {symbol} | {str(e)}", flush=True)
                        raise e
            return None

        latest_period = "Chưa xác định"
        for report_name in report_names:
            try:
                report_func = getattr(equity, report_name, None)
                if not callable(report_func): continue
                
                print(f"    ⏳ Đang lấy {report_name}...", flush=True)
                df = fetch_with_retry(report_func)
                time.sleep(1.5) 
                
                if df is not None and not df.empty:
                    clean_df = df.astype(object).where(pd.notna(df), None)
                    records = clean_df.to_dict(orient="records")
                    
                    if records and latest_period == "Chưa xác định":
                        try:
                            first_record = records[0]
                            y, q = first_record.get("year"), first_record.get("quarter")
                            if y and q: latest_period = f"Q{q}/{y}"
                        except: pass
                            
                    reports[report_name] = json.loads(json.dumps(records, ensure_ascii=False, default=str))
                    print(f"    ✓ Đã lấy {report_name} ({len(records)} dòng)", flush=True)
            except Exception: pass
            
        if reports: 
            print(f"  ✅ [BCTC] Đã lấy xong {symbol} | Kỳ mới nhất: {latest_period}", flush=True)
            reports["latest_period"] = latest_period 
        return reports if reports else None
    except Exception as e:
        print(f"  ❌ [BCTC] Lỗi khi cào {symbol}: {e}", flush=True)
        return None

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

# ===================== CORE LOGIC CÀO & CẢNH BÁO =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling, LAST_TELEGRAM_TIME, stop_crawl_requested
    if is_crawling: return
    is_crawling = True
    stop_crawl_requested = False 
    
    print(f"\n[CRAWL] 🚀 [{'CÀO BÙ' if is_retry else ('CÀO TAY' if is_manual else 'REAL-TIME')}] Bắt đầu xử lý {len(symbols)} mã...", flush=True)
    try:
        vn_now = datetime.utcnow() + timedelta(hours=7)
        real_today = vn_now.strftime("%Y-%m-%d")
        current_hour = vn_now.hour

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

                    collection.update_one({"symbol": sym}, {"$set": {
                        "close": data["close"], "prev_close": prev_close, 
                        "open": data["open"], "high": data["high"], "low": data["low"], "volume": data["volume"],
                        "volume_percent": vol_pct, "avg_volume_20": round(avg_vol_20, 2),
                        "price_change_percent": pct_change,
                        "price_change_available": prev_close > 0,
                        "price_arrow": arrow, "price_color": color,
                        "trading_date": latest_history_date if latest_history_date else real_today, 
                        "updated_at": _now_str(), "is_locked": is_locked, "is_incomplete": False
                    }}, upsert=True)
                    
                    print(f"  ✅ [OK] {sym}: Giá {data['close']:,} | KL: {data['volume']:,} | Vol%: {vol_pct}%", flush=True)
                    
                    if prev_close > 0 and abs(pct_change) >= 3.0 and vol_pct >= 120.0:
                        alert_batch.append({"sym": sym, "pct_change": pct_change, "volume_percent": vol_pct, "trend": "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG"})
                        
                    if current_hour >= 15 and not is_locked:
                        d1_bars = hist_doc.get("history_d1", []) if hist_doc else []
                        d1_bars = [b for b in d1_bars if b.get("time") != real_today]
                        
                        d1_bars.append({
                            "time": real_today, 
                            "open": data["open"], 
                            "high": data["high"], 
                            "low": data["low"], 
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
                    print(f"  ❌ [LỖI] {sym}: Không lấy được dữ liệu.", flush=True)
                    collection.update_one({"symbol": sym}, {"$set": {"is_incomplete": True, "last_error_at": _now_str()}}, upsert=True)
            time.sleep(1)

        if current_hour >= 15 and pending_history_files:
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

# ===================== CHẤM ĐIỂM (FULL LOGIC) =====================
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
    "NGAN_HANG": {"cap_pe": 14, "cap_pb": 1.8, "mos": 0.20}, "CHUNG_KHOAN": {"cap_pe": 14, "cap_pb": 1.8, "mos": 0.25},
    "BAT_DONG_SAN": {"cap_pe": 12, "cap_pb": 2.0, "mos": 0.30}, "CONG_NGHE_BAN_LE": {"cap_pe": 20, "cap_pb": 3.0, "mos": 0.20},
    "DIEN_NUOC_HA_TANG": {"cap_pe": 15, "cap_pb": 2.0, "mos": 0.15}, "NGUYEN_VAT_LIEU_CHU_KY": {"cap_pe": 12, "cap_pb": 2.0, "mos": 0.30},
    "SAN_XUAT_TIEU_DUNG_PHONG_THU": {"cap_pe": 18, "cap_pb": 2.5, "mos": 0.25}, "INSURANCE": {"cap_pe": 16, "cap_pb": 2.0, "mos": 0.25},
    "PETROLEUM": {"cap_pe": 12, "cap_pb": 1.5, "mos": 0.30}, "MARITIME": {"cap_pe": 10, "cap_pb": 1.5, "mos": 0.30},
    "AVIATION": {"cap_pe": 15, "cap_pb": 3.0, "mos": 0.35}, "CONSTRUCTION": {"cap_pe": 12, "cap_pb": 1.5, "mos": 0.30},
    "AGRI": {"cap_pe": 14, "cap_pb": 2.0, "mos": 0.25},
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
    cfo, npat = nd["cfo_latest"], nd["net_profit_ttm"]
    methods = {}
    med_pe, med_pb, med_roe = sector_median.get("pe") or 12.0, sector_median.get("pb") or 2.0, sector_median.get("roe") or 0.0
    cap_pe, cap_pb, mos = config["cap_pe"], config["cap_pb"], config["mos"]
    is_financial = group_label in ("NGAN_HANG", "CHUNG_KHOAN", "INSURANCE")
    
    if eps > 0 and med_pe > 0: methods["PE_method"] = round(eps * min(med_pe * (1.15 if (nd["roe"] > med_roe) else 1.0), cap_pe), 0)
    if bvps > 0 and med_pb > 0: methods["PB_method"] = round(bvps * min(med_pb, cap_pb), 0)
    if not is_financial and eps > 0 and bvps > 0: methods["Graham"] = round((22.5 * eps * bvps) ** 0.5, 0)
    if not is_financial and npat > 0 and cfo > 0 and (cfo / npat) >= 0.8 and eps > 0 and med_pe > 0: methods["CFO_boost"] = round(eps * min(med_pe, cap_pe) * 1.10, 0)
    
    if not methods: return {"fair_low": 0, "fair_high": 0, "graham": 0, "methods": {}, "mos_pct": round(mos * 100, 1), "rationale": "Không đủ DL.", "discount_to_fair_pct": 0, "verdict": "KHÔNG ĐỦ DỮ LIỆU"}
    
    weights = {"PE_method": 2, "PB_method": 1.5, "Graham": 2, "CFO_boost": 1}
    weighted = []
    for k, v in methods.items(): weighted.extend([v] * int(weights.get(k, 1) * 2))
    weighted.sort()
    fair_high = weighted[len(weighted) // 2]
    fair_low = fair_high * (1 - mos)
    discount_to_fair = ((fair_low - close) / close * 100) if close > 0 else 0.0
    verdict = "KHÔNG CÓ GIÁ" if close <= 0 else ("RẺ - TRONG VÙNG MUA" if close <= fair_low else ("HỢP LÝ - CHỜ ĐIỀU CHỈNH" if close <= fair_high else ("HƠI ĐẮT - KHÔNG MUA" if close <= fair_high * 1.2 else "ĐẮT - TRÁNH")))
    return {"fair_low": round(fair_low, 0), "fair_high": round(fair_high, 0), "graham": round(methods.get("Graham", 0), 0), "methods": {k: round(v, 0) for k, v in methods.items()}, "mos_pct": round(mos * 100, 1), "rationale": f"[{group_label}]", "discount_to_fair_pct": round(discount_to_fair, 1), "verdict": verdict}

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
    return ("👀 THEO DÕI", "slate", "Chưa đủ ĐK.")

def evaluate_stock_from_db(symbol, nd, sector_median, group_label, config):
    sector = get_sector(symbol)
    pe, pb, ps, roe, roa = nd["pe"], nd["pb"], nd["ps"], nd["roe"], nd["roa"]
    close_price, cfo_latest, net_profit_ttm = nd["close_price"], nd["cfo_latest"], nd["net_profit_ttm"]
    debt_to_equity, profit_growth_yoy, credit_growth_yoy = nd["debt_to_equity"], nd["profit_growth_yoy"], nd["credit_growth_yoy"]
    
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

    return {
        "symbol": symbol, "sector": sector, "sub_sector": group_label, "close_price": close_price, "pe": pe, "pb": pb, "ps": ps,
        "roe": roe, "roa": roa, "eps": round(nd["eps"], 0), "bvps": round(nd["bvps"], 0), "dividend_yield": nd["dividend_yield"], "profit_growth_yoy": profit_growth_yoy,
        "cfo_latest": cfo_latest, "debt_to_equity": debt_to_equity, "gross_margin": nd["gross_margin"],
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
        "sim_end_price": sim_data["end_price_mean"] if sim_data else 0
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

# ===================== AI & CẢNH BÁO BCTC CŨ =====================
SECTOR_NAMES = {"NGAN_HANG": "Ngân hàng", "CHUNG_KHOAN": "Chứng khoán", "BAT_DONG_SAN": "Bất động sản", "CONG_NGHE_BAN_LE": "Công nghệ & Bán lẻ", "DIEN_NUOC_HA_TANG": "Điện, Nước & Hạ tầng", "NGUYEN_VAT_LIEU_CHU_KY": "Vật liệu & Chu kỳ", "SAN_XUAT_TIEU_DUNG_PHONG_THU": "Sản xuất & Tiêu dùng"}
DECISION_TO_RECOMMENDATION = {"🎯 MUA MẠNH": ("MUA MẠNH", "Tỷ trọng 20-30%"), "✅ MUA TÍCH SẢN": ("MUA TÍCH SẢN", "Tỷ trọng 10-15%"), "⚠️ CHỜ VỀ VÙNG MUA": ("CHỜ VỀ VÙNG MUA", "Tỷ trọng 0%"), "⏳ CHỜ XÁC NHẬN KỸ THUẬT": ("CHỜ XÁC NHẬN KỸ THUẬT", "Tỷ trọng 0%"), "❌ TRÁNH": ("TRÁNH", "Tỷ trọng 0%"), "👀 THEO DÕI": ("NẮM GIỮ / THEO DÕI", "Tỷ trọng 0%")}

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

# ===================== BACKGROUND WORKERS =====================
def auto_crawler_worker():
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

            # 1. KIỂM TRA CUỐI TUẦN
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

            # 2. KIỂM TRA GIỜ GIAO DỊCH
            is_lunch_break = (current_hour == 11 and current_minute >= 30) or (current_hour == 12)
            is_trading_time = (9 <= current_hour < 15) and (not is_lunch_break)

            if not is_trading_time:
                if is_lunch_break:
                    wake_up_time = vn_now.replace(hour=13, minute=0, second=0, microsecond=0)
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

            # 3. PHÁT HIỆN NGÀY LỄ (Kiểm tra toàn bộ 100 mã vào lúc 10h00)
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

            # 4. TIẾN HÀNH CÀO
            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            crawled_count = collection.count_documents({"chot_so_today": today_str})

            if crawled_count >= len(all_symbols) and len(all_symbols) > 0:
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
                        time.sleep(3.0)
                time.sleep(3600) 
            else: time.sleep(30)
        except Exception: time.sleep(60)

def screener_worker():
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            if vn_now.hour == 16 and vn_now.minute == 30 and vn_now.second < 10:
                stock_db = {}
                for doc in collection.find({}):
                    bctc = bctc_col.find_one({"symbol": doc["symbol"]}, {"_id": 0})
                    doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                    stock_db[doc["symbol"]] = doc
                normalized_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
                sector_medians = compute_sector_medians(normalized_db)
                results = []
                for symbol, nd in normalized_db.items():
                    grp = get_sub_sector(symbol)
                    cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(symbol))
                    med, _ = get_median_for_symbol(symbol, sector_medians)
                    res = evaluate_stock_from_db(symbol, nd, med, grp, cfg)
                    if res: results.append(res)
                if results:
                    results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                    screener_col.delete_many({}) 
                    screener_col.insert_many(results) 
                    commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Update Screener {_now_str()}")
                time.sleep(120) 
            time.sleep(30)
        except Exception: time.sleep(60)

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
    print("[API] 📈 Gọi API /api/data (Lấy toàn bộ giá)", flush=True)
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
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
    if not symbol: return jsonify({"status": "error", "message": "Thiếu mã"}), 400
    if not ALIBABA_API_KEY: return jsonify({"status": "error", "message": "Thiếu ALIBABA_API_KEY"}), 500
    try:
        sm = collection.find_one({"symbol": symbol}, {"_id": 0})
        si = screener_col.find_one({"symbol": symbol}, {"_id": 0})
        bctc_doc = bctc_col.find_one({"symbol": symbol}, {"_id": 0})
        
        if not sm or not si: return jsonify({"status": "error", "message": "Chưa đủ dữ liệu"}), 404
        
        bctc_warning = ""
        bctc_period = "Không rõ"
        bctc_updated_at = "Chưa cào"
        
        if bctc_doc:
            bctc_updated_at = bctc_doc.get("updated_at", "Chưa cào")
            bctc_period = bctc_doc.get("latest_period", "Không rõ")
            try:
                last_update_date = datetime.strptime(bctc_updated_at, "%d/%m/%Y %H:%M:%S")
                if (datetime.utcnow() - last_update_date).days > 45:
                    bctc_warning = f"⚠️ CẢNH BÁO: BCTC đã cũ, kỳ mới nhất là {bctc_period}, cào lúc {bctc_updated_at}. Hãy cào lại BCTC!"
            except:
                bctc_warning = "⚠️ CẢNH BÁO: Không xác định được ngày cào BCTC."

        sector = si.get("sector", "SAN_XUAT_TIEU_DUNG_PHONG_THU")
        rec_text, rec_weight = DECISION_TO_RECOMMENDATION.get(si.get("final_decision"), ("THEO DÕI", "0%"))
        warning_text = f"\nLƯU Ý DỮ LIỆU: {bctc_warning}" if bctc_warning else ""
        
        prompt = f"Phân tích cổ phiếu {symbol} dựa trên dữ kiện:\n{build_verified_facts(symbol, sm, si, sector)}\nKỳ BCTC mới nhất: {bctc_period}\nHướng dẫn: {get_sector_guidance(sector)}\n{warning_text}\nKHUYẾN NGHỊ BẮT BUỘC: '{rec_text} — {rec_weight}'.\nTrả về JSON thuần (không markdown):\n{{ \"bctc\": \"...\", \"catalyst\": \"...\", \"risks\": \"...\", \"valuation_verdict\": \"{si.get('valuation_verdict')}\", \"outlook\": \"...\", \"recommendation\": \"{rec_text} — {rec_weight}\", \"price_zone\": \"...\", \"macro_view\": \"...\" }}"
        
        res = requests.post(AI_API_URL, headers={"Authorization": f"Bearer {ALIBABA_API_KEY}"}, json={"model": AI_MODEL_NAME, "messages": [{"role": "user", "content": prompt}], "temperature": AI_TEMPERATURE}, timeout=60)
        if res.status_code == 200:
            content = re.sub(r"```json\s*|```\s*", "", res.json()["choices"][0]["message"]["content"].strip())
            parsed = json.loads(content)
            collection.update_one({"symbol": symbol}, {"$set": {"ai_bctc": parsed.get("bctc", ""), "ai_catalyst": parsed.get("catalyst", ""), "ai_risks": parsed.get("risks", ""), "ai_outlook": parsed.get("outlook", ""), "ai_recommendation": parsed.get("recommendation", ""), "ai_macro_view": parsed.get("macro_view", ""), "ai_updated_at": _now_str()}})
            
            gh_data, _ = get_github_file_data(SCREENER_JSON_FILE)
            if gh_data:
                for item in gh_data:
                    if item.get("symbol") == symbol:
                        item.update({"ai_bctc": parsed.get("bctc", ""), "ai_catalyst": parsed.get("catalyst", ""), "ai_risks": parsed.get("risks", ""), "ai_outlook": parsed.get("outlook", ""), "ai_recommendation": parsed.get("recommendation", ""), "ai_macro_view": parsed.get("macro_view", ""), "ai_updated_at": _now_str()})
                        break
                commit_multiple_files_to_github({SCREENER_JSON_FILE: gh_data}, f"Update AI {symbol}")
                
            return jsonify({
                "status": "success", 
                "data": parsed, 
                "bctc_period": bctc_period,
                "bctc_updated_at": bctc_updated_at,
                "warning": bctc_warning
            })
        return jsonify({"status": "error", "message": "AI lỗi"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

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
    print("[API] 📋 Gọi API /api/bctc-status (Lấy danh sách BCTC)", flush=True)
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
    print(f"\n[API] 📥 Gọi API /api/crawl-full-bctc (Ép cào toàn bộ BCTC)", flush=True)
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
                if res: bctc_col.update_one({"symbol": sym}, {"$set": {**res, "symbol": sym, "updated_at": _now_str()}}, upsert=True)
                time.sleep(3.0)
            print(f"[BCTC-CRAWL] 🏁 Đã cào xong toàn bộ BCTC.", flush=True)
        finally: is_full_data_running = False
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào FULL BCTC..."})

@app.get("/api/crawl-ratios")
def crawl_ratios():
    print(f"\n[API] 📥 Gọi API /api/crawl-ratios (Cào P/E, P/B, ROE từ VNDirect)", flush=True)
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

                collection.update_one({"symbol": sym}, {"$set": {
                    "pe": round(ratios.get("pe", 0), 2),
                    "pb": round(ratios.get("pb", 0), 2),
                    "ps": round(ratios.get("ps", 0), 2),
                    "eps": int(ratios.get("eps", 0)),
                    "roe": round(roe_val, 1),
                    "roa": round(roa_val, 1),
                    "dividend_yield": round(div_val, 2),
                    "ratio_updated_at": _now_str()
                }}, upsert=True)
                
                print(f"  ✅ [RATIO] {sym} thành công (P/E: {ratios.get('pe', 0)}, ROE: {round(roe_val, 1)}%)", flush=True)
                time.sleep(0.5)

            print(f"[RATIO-CRAWL] 🏁 Đã cào xong toàn bộ bảng Ratio.", flush=True)
        finally: 
            is_full_data_running = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang cào bảng Ratio (P/E, ROE...) từ VNDirect..."})

@app.get("/api/force-screener")
def force_screener():
    print(f"\n[API] 📥 Gọi API /api/force-screener (Ép chấm điểm lại)", flush=True)
    global is_screener_running
    if is_screener_running: 
        print("[SCREENER] ⚠️ Đang có tiến trình chấm điểm chạy, bỏ qua lệnh mới.", flush=True)
        return jsonify({"status": "error", "message": "Đang chạy"}), 400
        
    is_screener_running = True
    print(f"[SCREENER] 🚀 Bắt đầu chấm điểm lại toàn bộ...", flush=True)
    
    def _run():
        global is_screener_running
        try:
            stock_db = {}
            for doc in collection.find({}):
                bctc = bctc_col.find_one({"symbol": doc["symbol"]}, {"_id": 0})
                doc["bctc_history"] = bctc.get("income_statement", []) if bctc else []
                stock_db[doc["symbol"]] = doc
            
            print(f"[SCREENER] 📊 Đã tải dữ liệu của {len(stock_db)} mã từ Database.", flush=True)
            
            norm_db = {s: normalize_stock_data(s, r) for s, r in stock_db.items()}
            medians = compute_sector_medians(norm_db)
            results = []
            
            count = 0
            for sym, nd in norm_db.items():
                grp = get_sub_sector(sym)
                cfg = SUB_SECTOR_CONFIG.get(grp) or SUB_SECTOR_CONFIG.get(get_sector(sym))
                med, _ = get_median_for_symbol(sym, medians)
                res = evaluate_stock_from_db(sym, nd, med, grp, cfg)
                if res: results.append(res)
                count += 1
                if count % 20 == 0:
                    print(f"[SCREENER] ⏳ Đã xử lý {count}/{len(norm_db)} mã...", flush=True)
            
            if results:
                results = sorted(results, key=lambda x: (x.get("total_score", 0), x.get("fundamental_score", 0)), reverse=True)
                screener_col.delete_many({}) 
                screener_col.insert_many(results) 
                print(f"[SCREENER] 💾 Đã lưu {len(results)} mã vào MongoDB.", flush=True)
                
                commit_multiple_files_to_github({SCREENER_JSON_FILE: results}, f"Force Screener {_now_str()}")
                print(f"[SCREENER] ✅ Đã đẩy file JSON lên Github thành công!", flush=True)
        except Exception as e:
            print(f"[SCREENER] ❌ Lỗi trong quá trình chấm điểm: {e}", flush=True)
        finally: 
            is_screener_running = False
            print(f"[SCREENER] 🏁 Đã kết thúc tiến trình chấm điểm.\n", flush=True)
            
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "success", "message": "Đang chấm điểm lại..."})
@app.get("/api/force-realtime")
def force_realtime():
    print(f"\n[API] ⚡ Gọi API /api/force-realtime (Ép cào giá Realtime ngay lập tức)", flush=True)
    global is_crawling
    
    if is_crawling:
        return jsonify({"status": "error", "message": "Hệ thống đang trong quá trình cào, vui lòng đợi vài giây!"}), 400
        
    # Lấy toàn bộ danh sách mã
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    
    # Chạy ngầm tiến trình cào để không làm treo request HTTP
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    
    return jsonify({
        "status": "success", 
        "message": f"Đã kích hoạt ép cào realtime cho toàn bộ {len(all_symbols)} mã. Vui lòng đợi 1-2 phút rồi F5 trang web!"
    })
@app.get("/api/reset-today")
def reset_today():
    print(f"\n[API] 📥 Gọi API /api/reset-today (Hủy chốt sổ)", flush=True)
    today_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")
    result = collection.update_many({"chot_so_today": today_str}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
    return jsonify({"status": "success", "message": f"Đã xóa cờ chốt sổ của {result.modified_count} mã."})

@app.get("/api/force-fix")
def force_fix_data():
    print(f"\n[API] 📥 Gọi API /api/force-fix (Ép cào lại toàn bộ giá)", flush=True)
    collection.update_many({}, {"$unset": {"chot_so_today": "", "is_locked": "", "trading_date": ""}})
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    return jsonify({"status": "success", "message": f"Đang ép cào lại toàn bộ {len(all_symbols)} mã."})

@app.get("/api/retry-now")
def retry_now():
    print(f"\n[API] 📥 Gọi API /api/retry-now (Cào bù mã lỗi)", flush=True)
    if is_crawling:
        return jsonify({"status": "error", "message": "Hệ thống đang chạy, vui lòng đợi!"})
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    if not incomplete_symbols:
        return jsonify({"status": "success", "message": "Không có mã nào bị lỗi!"})
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào bù {len(incomplete_symbols)} mã!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
