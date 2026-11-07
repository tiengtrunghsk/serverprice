"""
Price API — Pro Quant Screener
Cơ chế tối ưu: Khóa theo từng Mã (Symbol-based Locking) + Cào song song (Multi-threading).
"""

import os
import sys
import time
import json
import base64
import threading
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, Optional

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

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "15"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "45"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== DATABASE =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    settings_col = db['settings'] # Lưu giờ cập nhật toàn cục
    gh_history = db['github_sync_history'] # Lưu lịch sử ghi Github
    print("✅ Kết nối MongoDB thành công!", flush=True)
except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    db = None
    collection = None
    settings_col = None
    gh_history = None

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()
github_sync_lock = threading.Lock() 

# Cơ chế Khóa theo từng Mã (Symbol Locks)
symbol_locks = {}
locks_guard = threading.Lock()

def get_symbol_lock(symbol):
    with locks_guard:
        if symbol not in symbol_locks:
            symbol_locks[symbol] = threading.Lock()
        return symbol_locks[symbol]

# ===================== HELPERS =====================
def _now_str() -> str:
    """Trả về giờ Việt Nam (GMT+7) dạng chuỗi."""
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.strftime("%d/%m/%Y %H:%M:%S")

def _is_after_15h() -> bool:
    """Kiểm tra xem giờ hiện tại (GMT+7) đã qua 15h chưa."""
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.hour >= 15

def _fetch_one(symbol: str) -> Optional[int]:
    """Lấy giá close từ lotusmarket với timeout cứng."""
    def _do():
        try:
            stock = fetchers.vps(symbol)
            price = getattr(stock, "close", 0) or 0
            return price
        except Exception as e:
            print(f"[FETCH] ❌ Exception cho {symbol}: {type(e).__name__} - {e}", flush=True)
            return 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            price = fut.result(timeout=FETCH_TIMEOUT)
        except concurrent.futures.TimeoutError:
            print(f"[FETCH] ❌ Timeout {FETCH_TIMEOUT}s cho {symbol}", flush=True)
            return None
        except Exception as e:
            print(f"[FETCH] ❌ Lỗi luồng cho {symbol}: {e}", flush=True)
            return None

    if not price or price <= 0:
        return None

    if price < 1000:
        price = price * 1000

    return int(price)


def _get_cached(symbol: str) -> Optional[dict]:
    with _cache_lock:
        entry = _cache.get(symbol)
        if not entry:
            return None
        if time.time() - entry["ts"] > CACHE_TTL:
            return None
        return entry


def _set_cache(symbol: str, close: int):
    with _cache_lock:
        _cache[symbol] = {"close": close, "ts": time.time()}


def _get_price(symbol: str, use_cache: bool = True) -> dict:
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {"symbol": symbol, "close": 0, "updated_at": _now_str(), "source": "error"}

    if use_cache:
        cached = _get_cached(symbol)
        if cached:
            return {
                "symbol": symbol,
                "close": cached["close"],
                "updated_at": _now_str(),
                "source": "cache",
            }

    price = _fetch_one(symbol)
    if price is None:
        return {"symbol": symbol, "close": 0, "updated_at": _now_str(), "source": "error"}

    _set_cache(symbol, price)
    return {
        "symbol": symbol,
        "close": price,
        "updated_at": _now_str(),
        "source": "live",
    }


# ===================== GITHUB SYNC LOGIC =====================
def get_github_file_data():
    """Lấy nội dung data.json hiện tại từ GitHub."""
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]):
        print("⚠️ Thiếu cấu hình GitHub, không thể lấy dữ liệu.", flush=True)
        return None, None

    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200:
            file_info = res.json()
            content_decoded = base64.b64decode(file_info["content"]).decode("utf-8")
            return json.loads(content_decoded), file_info["sha"]
        elif res.status_code == 404:
            return {}, None
        else:
            print(f"❌ Lỗi lấy file GitHub: {res.status_code}", flush=True)
            return None, None
    except Exception as e:
        print(f"❌ Exception khi lấy GitHub: {e}", flush=True)
        return None, None


def update_github_file(data_dict, commit_message):
    """Ghi đè nội dung data_dict lên data.json trên GitHub."""
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]):
        return False

    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

    _, sha = get_github_file_data()
    
    content_str = json.dumps(data_dict, ensure_ascii=False, indent=4)
    content_b64 = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": content_b64,
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    try:
        res = requests.put(api_url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            print(f"✅ Đã ghi đè {GITHUB_FILE_PATH} lên GitHub!", flush=True)
            if gh_history:
                gh_history.insert_one({"time": datetime.utcnow(), "message": commit_message})
            return True
        else:
            print(f"❌ Lỗi commit GitHub: {res.status_code} - {res.text}", flush=True)
            return False
    except Exception as e:
        print(f"❌ Exception khi commit GitHub: {e}", flush=True)
        return False

def can_update_github() -> bool:
    """
    Kiểm tra điều kiện được phép ghi Github:
    1. Phải sau 15h (giờ VN).
    2. Trong ngày hôm nay chưa ghi quá 3 lần.
    """
    if not _is_after_15h():
        print("[GITHUB] ⏳ Chưa tới 15h, không ghi Github.", flush=True)
        return False

    if not gh_history:
        return True

    vn_now = datetime.utcnow() + timedelta(hours=7)
    start_of_day_vn = vn_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_day_utc = start_of_day_vn - timedelta(hours=7)
    
    count = gh_history.count_documents({"time": {"$gte": start_of_day_utc}})
    
    if count >= 3:
        print(f"[GITHUB] 🛑 Đã ghi đủ 3 lần hôm nay. Không ghi nữa.", flush=True)
        return False
        
    return True


# ===================== ROUTES =====================
@app.get("/")
def root():
    return jsonify({
        "service": "quant-price-api",
        "status": "ok",
        "db_connected": collection is not None,
        "github_configured": bool(GITHUB_TOKEN and GITHUB_USERNAME and GITHUB_REPO),
        "endpoints": ["/healthz", "/api/data", "/api/price/<symbol>", "/api/sync?symbols=VCB,ACB", "/api/backup-github", "/api/last-update"],
    })


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _now_str(), "db": collection is not None})


@app.get("/api/last-update")
def api_last_update():
    """Lấy mốc thời gian cập nhật toàn cục từ MongoDB."""
    if settings_col is None:
        return jsonify({"last_update": "Chưa kết nối DB"})
    
    try:
        setting = settings_col.find_one({"_id": "global_last_update"})
        if setting and "time" in setting:
            return jsonify({"last_update": setting["time"]})
        return jsonify({"last_update": "Chưa có dữ liệu"})
    except Exception as e:
        print(f"[TIME] Lỗi: {e}", flush=True)
        return jsonify({"last_update": "Lỗi DB"})


@app.get("/api/data")
def api_get_data():
    """Trả toàn bộ dữ liệu trong collection stocks (dùng thay cho data.json tĩnh)."""
    if collection is None:
        return jsonify({"status": "error", "message": "MongoDB chưa kết nối"}), 500

    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
                data[sym] = doc
        print(f"[DATA] Trả về {len(data)} mã từ MongoDB.", flush=True)
        return jsonify({"status": "success", "count": len(data), "data": data})
    except PyMongoError as e:
        print(f"[DATA] ❌ Lỗi Mongo: {e}", flush=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/price/<symbol>")
def api_price(symbol: str):
    symbol = symbol.strip().upper()
    if not symbol.isalnum() or len(symbol) > 10:
        abort(400, description="Invalid symbol")

    # Sử dụng lock riêng cho mã này để chống spam cục bộ
    lock = get_symbol_lock(symbol)
    if not lock.acquire(blocking=False):
        # Nếu đang cào mã này, trả giá cũ trong DB ngay
        cached_price = _get_cached(symbol)
        return jsonify({"symbol": symbol, "close": cached_price["close"] if cached_price else 0, "source": "busy"}), 200
    
    try:
        price = _fetch_one(symbol)
        if price:
            collection.update_one({"symbol": symbol}, {"$set": {"close": price, "updated_at": _now_str()}}, upsert=True)
            return jsonify({"symbol": symbol, "close": price, "source": "live"}), 200
        return jsonify({"symbol": symbol, "close": 0, "source": "error"}), 502
    finally:
        lock.release()


@app.get("/api/sync")
def api_sync():
    """Lấy giá realtime cho danh sách mã, ghi đè vào MongoDB VÀ Github."""
    if collection is None:
        return jsonify({"status": "error", "message": "MongoDB chưa kết nối"}), 500

    raw = request.args.get("symbols", "").strip()
    if not raw:
        abort(400, description="Missing ?symbols=")

    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols:
        abort(400, description="Empty symbols list")
    if len(symbols) > MAX_SYMBOLS_PER_REQUEST:
        abort(400, description=f"Too many symbols (max {MAX_SYMBOLS_PER_REQUEST})")

    now_str = _now_str()
    updated_data = {}
    success = 0
    fail = 0

    # Hàm xử lý 1 mã (có lock riêng)
    def process_symbol(sym):
        lock = get_symbol_lock(sym)
        if not lock.acquire(blocking=False):
            return sym, 0, "busy" # Trả về 0 nếu đang bận
            
        try:
            price = _fetch_one(sym)
            if price and price > 0:
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {"symbol": sym, "close": price, "updated_at": now_str}},
                    upsert=True
                )
                return sym, price, "success"
            else:
                return sym, 0, "error"
        finally:
            lock.release()

    # Cào song song tối đa 10 luồng cùng lúc (tăng tốc độ)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_symbol, symbols))

    # Tổng hợp kết quả
    for sym, price, status in results:
        if status == "success":
            updated_data[sym] = price
            success += 1
            print(f"[SYNC] ✅ {sym}: {price:,}", flush=True)
        else:
            fail += 1
            # Nếu bận, lấy giá cũ từ DB để trả về cho người dùng
            if status == "busy":
                doc = collection.find_one({"symbol": sym})
                if doc and "close" in doc:
                    updated_data[sym] = doc["close"]
            print(f"[SYNC] ❌ {sym}: {status}", flush=True)

    # Cập nhật giờ toàn cục nếu là lệnh cập nhật hàng loạt
    if len(symbols) >= 5 and success > 0:
        settings_col.update_one({"_id": "global_last_update"}, {"$set": {"time": now_str}}, upsert=True)
        print(f"[SYNC] Đã cập nhật mốc thời gian toàn cục: {now_str}", flush=True)

    # Đồng bộ Github (chỉ đồng bộ những mã vừa cào thành công)
    if updated_data and GITHUB_TOKEN:
        if can_update_github():
            with github_sync_lock:
                old_gh_data, _ = get_github_file_data()
                if old_gh_data is not None:
                    has_changes = False
                    for sym, price, status in results:
                        if status == "success":
                            old_gh_data[sym] = old_gh_data.get(sym, {})
                            old_gh_data[sym]["close"] = price
                            old_gh_data[sym]["updated_at"] = now_str
                            has_changes = True
                    if has_changes:
                        update_github_file(old_gh_data, f"Auto-update prices via API ({now_str})")

    print(f"[SYNC] Hoàn tất: {success} OK | {fail} FAIL", flush=True)
    return jsonify({
        "status": "success",
        "updated_at": now_str,
        "updated_count": len(updated_data),
        "failed_count": fail,
        "updated_data": updated_data,
    })


@app.get("/api/backup-github")
def api_backup_github():
    """Đối chiếu MongoDB và GitHub."""
    if collection is None:
        return jsonify({"status": "error", "message": "MongoDB chưa kết nối"}), 500

    if not can_update_github():
        return jsonify({"status": "skipped", "message": "Chưa tới 15h hoặc đã ghi đủ 3 lần hôm nay."}), 200

    print("[BACKUP] Đang đối chiếu MongoDB và GitHub...", flush=True)
    
    cursor = collection.find({}, {"_id": 0})
    mongo_data = {doc["symbol"]: doc for doc in cursor if "symbol" in doc}
    if not mongo_data:
        return jsonify({"status": "error", "message": "MongoDB trống, không có gì để backup"}), 400

    with github_sync_lock:
        gh_data, _ = get_github_file_data()
        if gh_data is None:
            return jsonify({"status": "error", "message": "Không lấy được data.json từ GitHub"}), 500

        diff_count = 0
        now_str = _now_str()
        for sym, m_doc in mongo_data.items():
            m_price = m_doc.get("close")
            g_price = gh_data.get(sym, {}).get("close")
            
            if m_price != g_price:
                if sym not in gh_data: gh_data[sym] = {}
                gh_data[sym]["close"] = m_price
                gh_data[sym]["updated_at"] = m_doc.get("updated_at", now_str)
                diff_count += 1

        if diff_count > 0:
            update_github_file(gh_data, f"Auto-backup {diff_count} prices from MongoDB ({now_str})")
    
    return jsonify({
        "status": "success", 
        "message": f"Đã đối chiếu xong. Cập nhật {diff_count} mã lên GitHub.",
        "mongo_count": len(mongo_data),
        "diff_count": diff_count
    })


# ===================== ENTRYPOINT =====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
