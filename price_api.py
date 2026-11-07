"""
Price API — Pro Quant Screener
Cơ chế Tối giản & Ổn định (Chống lỗi 500).
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
    print("❌ LỖI: Thiếu MONGO_URI!", flush=True)
    sys.exit(1)

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_FILE_PATH = os.environ.get("GITHUB_FILE_PATH", "data.json").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))
COOLDOWN_SECONDS = 300

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

# Biến toàn cục quản lý trạng thái
is_crawling = False
crawl_lock = threading.Lock()
LAST_CRAWL_TIME = {}

# ===================== HELPERS =====================
def _now_str() -> str:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.strftime("%d/%m/%Y %H:%M:%S")

def _is_trading_time() -> bool:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.weekday() >= 5: return False
    return 9 <= vn_now.hour < 15

def _is_after_15h() -> bool:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.hour >= 15

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
        elif res.status_code == 404:
            return {}, None
        return None, None
    except Exception:
        return None, None

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
    except Exception:
        pass
    return False

def can_update_github() -> bool:
    if not _is_after_15h(): return False
    if not gh_history: return True
    vn_now = datetime.utcnow() + timedelta(hours=7)
    start_of_day_utc = vn_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=7)
    return gh_history.count_documents({"time": {"$gte": start_of_day_utc}}) < 3

# ===================== CORE CRAWL LOGIC =====================
def _execute_crawl(symbols: list):
    global is_crawling
    
    if is_crawling:
        print("[CRAWL] ⏳ Đang có luồng cào khác chạy. Bỏ qua.", flush=True)
        return
        
    is_crawling = True
    try:
        now_str = _now_str()
        updated_data = {}
        
        print(f"[CRAWL] 🚀 Bắt đầu cào tuần tự {len(symbols)} mã...", flush=True)
        
        # CÀO TUẦN TỰ TỪNG MÃ (An toàn tuyệt đối cho Render Free)
        for sym in symbols:
            price = _fetch_one(sym)
            if price:
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {"symbol": sym, "close": price, "updated_at": now_str}},
                    upsert=True
                )
                updated_data[sym] = price
                LAST_CRAWL_TIME[sym] = time.time()
                print(f"[CRAWL] ✅ {sym}: {price:,}", flush=True)
            else:
                print(f"[CRAWL] ❌ {sym}: Lỗi/Timeout", flush=True)

        # Cập nhật giờ toàn cục
        if len(symbols) >= 5:
            settings_col.update_one({"_id": "global_last_update"}, {"$set": {"time": now_str}}, upsert=True)

        # Ghi Github (Chỉ ghi khi đã qua 15h và đủ điều kiện)
        if updated_data and GITHUB_TOKEN and can_update_github():
            print("[GITHUB] 🚀 Bắt đầu ghi Github...", flush=True)
            old_gh_data, _ = get_github_file_data()
            if old_gh_data is not None:
                for sym, price in updated_data.items():
                    old_gh_data[sym] = old_gh_data.get(sym, {})
                    old_gh_data[sym]["close"] = price
                    old_gh_data[sym]["updated_at"] = now_str
                update_github_file(old_gh_data, f"Auto-update prices via API ({now_str})")

        print(f"[CRAWL] ✅ Hoàn tất cào {len(updated_data)}/{len(symbols)} mã.", flush=True)
        
    finally:
        is_crawling = False

# ===================== BACKGROUND WORKER (AUTO 15m) =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật luồng tự động cập nhật giá (15 phút/lần).", flush=True)
    while True:
        try:
            if _is_trading_time():
                symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                if symbols:
                    _execute_crawl(symbols)
            time.sleep(900) # 15 phút
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
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
    """Trả toàn bộ dữ liệu, đồng bộ cả trường 'close' và 'close_price' để Frontend dễ dùng."""
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
                # Đảm bảo Frontend luôn nhận được 'close_price' và 'updated_at' đúng định dạng
                doc["close_price"] = doc.get("close", 0)
                data[sym] = doc
        return jsonify({"status": "success", "count": len(data), "data": data})
    except PyMongoError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.get("/api/sync")
def api_sync():
    raw = request.args.get("symbols", "").strip()
    if not raw: abort(400)
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols or len(symbols) > MAX_SYMBOLS_PER_REQUEST: abort(400)

    now_str = _now_str()
    
    # Nếu đang cào -> Trả về MongoDB ngay lập tức (Chống spam/sập)
    if is_crawling:
        print(f"[SYNC] ⏳ Hệ thống đang cào. Trả về MongoDB.", flush=True)
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc:
                updated_data[sym] = doc["close"]
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Đang cào)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })

    # Kiểm tra giãn cách 5 phút
    can_crawl = True
    for sym in symbols:
        if time.time() - LAST_CRAWL_TIME.get(sym, 0) < COOLDOWN_SECONDS:
            can_crawl = False
            break
            
    if not can_crawl:
        print(f"[SYNC] ⏳ Chưa đủ 5 phút. Trả về MongoDB.", flush=True)
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc:
                updated_data[sym] = doc["close"]
        return jsonify({
            "status": "success",
            "updated_at": now_str + " (Cache 5 phút)",
            "updated_count": len(updated_data),
            "updated_data": updated_data,
        })

    # Đủ điều kiện -> Cào ngầm và trả kết quả hiện tại (để người dùng không phải chờ)
    print(f"[SYNC] 🚀 Kích hoạt cào ngầm {len(symbols)} mã...", flush=True)
    threading.Thread(target=_execute_crawl, args=(symbols,)).start()
    
    updated_data = {}
    for sym in symbols:
        doc = collection.find_one({"symbol": sym})
        if doc and "close" in doc:
            updated_data[sym] = doc["close"]

    return jsonify({
        "status": "success",
        "updated_at": now_str + " (Đang cào ngầm)",
        "updated_count": len(updated_data),
        "updated_data": updated_data,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
