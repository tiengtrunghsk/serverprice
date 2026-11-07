"""
Price API — Pro Quant Screener
- Auto-crawl: 15 phút/lần, cào lại toàn bộ trong giờ giao dịch (9h-15h T2-T6).
- Sau 15h & ngoài giờ: Nếu đã cào đủ tất cả mã, không gọi sàn nữa.
- Nếu có mã lỗi, lần cào sau chỉ bổ sung những mã lỗi đó.
- Timeout Lock: Tự giải phóng khóa nếu tiến trình cào treo quá 120s.
- CHỐNG GHI GITHUB TRÙNG: Chỉ 1 luồng được ghi Github tại 1 thời điểm.
"""

import os
import sys
import time
import json
import base64
import threading
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

import requests
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lotusmarket import fetchers

# ===================== CONFIG =====================
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
if not MONGO_URI:
    print("❌ LỖI NGHIÊM TRỌNG: Chưa cấu hình biến môi trường MONGO_URI!", flush=True)
    sys.exit(1)

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_FILE_PATH = os.environ.get("GITHUB_FILE_PATH", "data.json").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))
COOLDOWN_SECONDS = 300
LOCK_TIMEOUT_SECONDS = 120

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== DATABASE =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    settings_col = db['settings']
    gh_history = db['github_sync_history']
    print("✅ Kết nối MongoDB thành công!", flush=True)
except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

# ==== BIẾN QUẢN LÝ TRẠNG THÁI ====
is_auto_crawling = False
crawl_lock = threading.Lock()
crawl_start_time = 0
LAST_CRAWL_TIME = {}

# ==== CHỐNG GHI GITHUB TRÙNG ====
github_write_lock = threading.Lock()  # Khóa cho việc ghi Github
is_github_writing = False  # Cờ hiệu: Đang ghi Github

# ==== BỘ THEO DÕI MÃ ĐÃ CÀO THÀNH CÔNG ====
SUCCESS_CACHE: Set[str] = set()
SUCCESS_CACHE_DATE = ""
SUCCESS_CACHE_RESET_LOCK = threading.Lock()

# ===================== HELPERS =====================
def _now_str() -> str:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.strftime("%d/%m/%Y %H:%M:%S")

def _today_str() -> str:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.strftime("%d/%m/%Y")

def _is_trading_time() -> bool:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.weekday() >= 5: return False
    return 9 <= vn_now.hour < 15

def _is_after_15h() -> bool:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.hour >= 15

def _reset_cache_if_new_day():
    global SUCCESS_CACHE, SUCCESS_CACHE_DATE
    with SUCCESS_CACHE_RESET_LOCK:
        today = _today_str()
        if SUCCESS_CACHE_DATE != today:
            SUCCESS_CACHE.clear()
            SUCCESS_CACHE_DATE = today
            print(f"🔄 Reset SUCCESS_CACHE cho ngày mới: {today}", flush=True)

def _fetch_one(symbol: str) -> Optional[int]:
    def _do():
        try:
            stock = fetchers.vps(symbol)
            return getattr(stock, "close", 0) or 0
        except Exception:
            return 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            price = fut.result(timeout=FETCH_TIMEOUT)
        except concurrent.futures.TimeoutError:
            return None
        except Exception:
            return None

    if not price or price <= 0: return None
    if price < 1000: price = price * 1000
    return int(price)

def _can_user_crawl(symbols: list) -> bool:
    now = time.time()
    for sym in symbols:
        last_time = LAST_CRAWL_TIME.get(sym, 0)
        if now - last_time < COOLDOWN_SECONDS:
            return False
    return True

def _acquire_crawl_lock(blocking: bool = False) -> bool:
    global crawl_start_time
    if crawl_lock.locked() and (time.time() - crawl_start_time > LOCK_TIMEOUT_SECONDS):
        print(f"⚠️ Khóa cào bị treo quá {LOCK_TIMEOUT_SECONDS}s. Tự giải phóng!", flush=True)
        try: crawl_lock.release()
        except RuntimeError: pass
    
    acquired = crawl_lock.acquire(blocking=blocking)
    if acquired: crawl_start_time = time.time()
    return acquired

# ===================== GITHUB SYNC =====================
def get_github_file_data():
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return None, None
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200:
            file_info = res.json()
            return json.loads(base64.b64decode(file_info["content"]).decode("utf-8")), file_info["sha"]
        elif res.status_code == 404: return {}, None
        return None, None
    except Exception: return None, None

def update_github_file(data_dict, commit_message):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return False
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    _, sha = get_github_file_data()
    payload = {
        "message": commit_message, 
        "content": base64.b64encode(json.dumps(data_dict, ensure_ascii=False, indent=4).encode("utf-8")).decode("utf-8"), 
        "branch": GITHUB_BRANCH
    }
    if sha: payload["sha"] = sha
    try:
        res = requests.put(api_url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            if gh_history: gh_history.insert_one({"time": datetime.utcnow(), "message": commit_message})
            return True
    except Exception: pass
    return False

def can_update_github() -> bool:
    if not _is_after_15h(): return False
    if not gh_history: return True
    vn_now = datetime.utcnow() + timedelta(hours=7)
    start_of_day_utc = vn_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=7)
    return gh_history.count_documents({"time": {"$gte": start_of_day_utc}}) < 3

def _sync_to_github(updated_data, now_str):
    """Ghi dữ liệu lên Github, có cơ chế chống trùng."""
    global is_github_writing
    
    if not updated_data or not GITHUB_TOKEN or not can_update_github():
        return
    
    # Nếu đang có luồng khác ghi Github, bỏ qua (vì lần ghi hiện tại đã bao gồm dữ liệu mới nhất)
    if is_github_writing:
        print("[GITHUB] ⏳ Đang có luồng khác ghi Github. Bỏ qua lần này.", flush=True)
        return
    
    try:
        if not github_write_lock.acquire(blocking=False):
            print("[GITHUB] ⏳ Không lấy được khóa ghi Github. Bỏ qua.", flush=True)
            return
        
        is_github_writing = True
        print("[GITHUB] 🚀 Bắt đầu ghi Github...", flush=True)
        
        old_gh_data, _ = get_github_file_data()
        if old_gh_data is not None:
            for sym, price in updated_data.items():
                old_gh_data[sym] = old_gh_data.get(sym, {})
                old_gh_data[sym]["close"] = price
                old_gh_data[sym]["updated_at"] = now_str
            update_github_file(old_gh_data, f"Auto-update prices via API ({now_str})")
        
        print("[GITHUB] ✅ Ghi Github hoàn tất.", flush=True)
        
    finally:
        is_github_writing = False
        github_write_lock.release()

# ===================== CORE CRAWL LOGIC =====================
def _execute_crawl(symbols: list, source: str = "user", only_failed: bool = False):
    now_str = _now_str()
    updated_data = {}
    
    _reset_cache_if_new_day()
    
    if only_failed:
        symbols = [s for s in symbols if s not in SUCCESS_CACHE]
        if not symbols:
            print(f"[{source.upper()}] ✅ Tất cả mã đã cào thành công. Không cần gọi sàn.", flush=True)
            return
    
    print(f"[{source.upper()}] 🚀 Bắt đầu cào {len(symbols)} mã...", flush=True)
    
    def process_sym(sym):
        price = _fetch_one(sym)
        if price:
            collection.update_one(
                {"symbol": sym},
                {"$set": {"symbol": sym, "close": price, "updated_at": now_str}},
                upsert=True
            )
            return sym, price
        return sym, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_sym, symbols))

    for sym, price in results:
        if price:
            updated_data[sym] = price
            LAST_CRAWL_TIME[sym] = time.time()
            with SUCCESS_CACHE_RESET_LOCK:
                SUCCESS_CACHE.add(sym)

    if len(symbols) >= 5:
        settings_col.update_one({"_id": "global_last_update"}, {"$set": {"time": now_str}}, upsert=True)

    # Ghi Github (có cơ chế chống trùng)
    _sync_to_github(updated_data, now_str)

    print(f"[{source.upper()}] ✅ Hoàn tất cào {len(updated_data)}/{len(symbols)} mã.", flush=True)

# ===================== BACKGROUND WORKER =====================
def auto_crawler_worker():
    global is_auto_crawling
    print("⏰ [AUTO] Đã bật luồng tự động cập nhật giá (15 phút/lần).", flush=True)
    
    while True:
        try:
            if _is_trading_time():
                is_auto_crawling = True
                _reset_cache_if_new_day()
                
                symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                if symbols:
                    if _acquire_crawl_lock(blocking=True):
                        try:
                            _execute_crawl(symbols, source="auto", only_failed=False)
                        finally:
                            crawl_lock.release()
                
                is_auto_crawling = False
                
            time.sleep(900)
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            is_auto_crawling = False
            time.sleep(60)

threading.Thread(target=auto_crawler_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _now_str()})

@app.get("/api/last-update")
def api_last_update():
    setting = settings_col.find_one({"_id": "global_last_update"})
    if setting and "time" in setting: return jsonify({"last_update": setting["time"]})
    return jsonify({"last_update": "Chưa có dữ liệu"})

@app.get("/api/data")
def api_get_data():
    cursor = collection.find({}, {"_id": 0})
    data = {doc["symbol"]: doc for doc in cursor if "symbol" in doc}
    return jsonify({"status": "success", "count": len(data), "data": data})

@app.get("/api/sync")
def api_sync():
    global is_auto_crawling
    
    raw = request.args.get("symbols", "").strip()
    if not raw: abort(400)
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols or len(symbols) > MAX_SYMBOLS_PER_REQUEST: abort(400)

    now_str = _now_str()
    _reset_cache_if_new_day()

    all_done = all(sym in SUCCESS_CACHE for sym in symbols)
    
    if is_auto_crawling:
        print(f"[SYNC] ⏳ Auto-crawl đang chạy. Trả về MongoDB.", flush=True)
        updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) 
                        for sym in symbols if collection.find_one({"symbol": sym})}
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Auto đang chạy)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })
    
    if not _is_trading_time() and all_done:
        print(f"[SYNC] ✅ Ngoài giờ GD và đã đủ dữ liệu. Không gọi sàn.", flush=True)
        updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) 
                        for sym in symbols if collection.find_one({"symbol": sym})}
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Ngoài giờ, đã đủ)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })

    if not _can_user_crawl(symbols):
        print(f"[SYNC] ⏳ Chưa đủ 5 phút. Trả về MongoDB.", flush=True)
        updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) 
                        for sym in symbols if collection.find_one({"symbol": sym})}
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Cache 5 phút)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })

    print(f"[SYNC] 🚀 Đủ điều kiện. Cào {len(symbols)} mã...", flush=True)
    
    if not _acquire_crawl_lock(blocking=False):
        print(f"[SYNC] ⏳ Có tiến trình khác đang cào. Trả về MongoDB.", flush=True)
        updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) 
                        for sym in symbols if collection.find_one({"symbol": sym})}
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Đang cào)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })
        
    try:
        only_failed = not _is_trading_time()
        _execute_crawl(symbols, source="user", only_failed=only_failed)
    finally:
        crawl_lock.release()
    
    updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) 
                    for sym in symbols if collection.find_one({"symbol": sym})}

    return jsonify({
        "status": "success",
        "updated_at": now_str,
        "updated_count": len(updated_data),
        "updated_data": updated_data,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
