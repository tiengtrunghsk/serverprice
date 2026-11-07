"""
Price API — Pro Quant Screener
Phục vụ tại Render, lưu trữ dữ liệu trên MongoDB Atlas.

Biến môi trường bắt buộc:
  - MONGO_URI : chuỗi kết nối MongoDB Atlas

Endpoint:
  GET /healthz
  GET /api/data                       -> Lấy toàn bộ dữ liệu cổ phiếu từ MongoDB
  GET /api/price/<symbol>             -> Lấy giá realtime 1 mã (không ghi DB)
  GET /api/sync?symbols=VCB,ACB,FPT   -> Lấy giá realtime + ghi đè vào MongoDB
"""

import os
import sys
import time
import threading
import concurrent.futures
import traceback
from datetime import datetime
from typing import Dict, Optional

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

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "45"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== DATABASE =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Ping thử để chắc chắn kết nối OK
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    print("✅ Kết nối MongoDB thành công!", flush=True)
except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    # Không sys.exit để app vẫn boot, chỉ log lỗi (nếu IP chưa mở)
    db = None
    collection = None

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()

# ===================== HELPERS =====================
def _now_str() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


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

    # lotusmarket trả VND. Nếu < 1000 thì × 1000 (heuristic cũ)
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


# ===================== ROUTES =====================
@app.get("/")
def root():
    return jsonify({
        "service": "quant-price-api",
        "status": "ok",
        "db_connected": collection is not None,
        "endpoints": ["/healthz", "/api/data", "/api/price/<symbol>", "/api/sync?symbols=VCB,ACB"],
    })


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _now_str(), "db": collection is not None})


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

    no_cache = request.args.get("nocache") == "1"
    result = _get_price(symbol, use_cache=not no_cache)
    return jsonify(result), (200 if result["close"] > 0 else 502)


@app.get("/api/sync")
def api_sync():
    """Lấy giá realtime cho danh sách mã, ghi đè vào MongoDB."""
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

    print(f"[SYNC] Bắt đầu đồng bộ {len(symbols)} mã: {symbols}", flush=True)
    now_str = _now_str()
    updated_data = {}
    success = 0
    fail = 0

    for sym in symbols:
        res = _get_price(sym, use_cache=False)
        if res["close"] > 0:
            try:
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {
                        "symbol": sym,
                        "close": res["close"],
                        "updated_at": now_str,
                    }},
                    upsert=True,
                )
                updated_data[sym] = res["close"]
                success += 1
                print(f"[SYNC] ✅ {sym}: {res['close']:,}", flush=True)
            except PyMongoError as e:
                print(f"[SYNC] ❌ Lỗi ghi Mongo cho {sym}: {e}", flush=True)
                fail += 1
        else:
            print(f"[SYNC] ❌ Không lấy được giá {sym}", flush=True)
            fail += 1

    print(f"[SYNC] Hoàn tất: {success} OK | {fail} FAIL", flush=True)
    return jsonify({
        "status": "success",
        "updated_at": now_str,
        "updated_count": len(updated_data),
        "failed_count": fail,
        "updated_data": updated_data,
    })


# ===================== ENTRYPOINT =====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
