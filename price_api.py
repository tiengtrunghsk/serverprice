"""
Price API — cập nhật giá realtime cho Pro Quant Screener (Bản có Log chi tiết).
"""

import os
import sys
import json
import time
import threading
import concurrent.futures
import traceback
from datetime import datetime
from typing import Dict, List, Optional

from flask import Flask, jsonify, request, abort
from flask_cors import CORS

from lotusmarket import fetchers

# ===================== CONFIG =====================
DATA_FILE = "data.json"
SYMBOLS_FILE = "vn100_symbols.json"
LAST_UPDATE_FILE = "last_update.json"

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "10")) # Tăng lên 10s cho an toàn
CACHE_TTL = int(os.environ.get("CACHE_TTL", "45"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
if _raw_origins == "*" or not _raw_origins:
    ALLOWED_ORIGINS = "*"
else:
    ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()

# ===================== HELPERS =====================
def _now_str() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def _fetch_one(symbol: str) -> Optional[int]:
    """Trả về giá close (int, VND) hoặc None nếu fail/timeout."""
    def _do():
        try:
            print(f"[FETCH] Bắt đầu lấy dữ liệu cho {symbol} từ lotusmarket...", flush=True)
            stock = fetchers.vps(symbol)
            print(f"[FETCH] Dữ liệu thô nhận được từ {symbol}: {stock}", flush=True)
            
            if stock is None:
                print(f"[FETCH] ❌ {symbol}: lotusmarket trả về None", flush=True)
                return 0
                
            price = getattr(stock, "close", 0) or 0
            print(f"[FETCH] ✅ {symbol}: Lấy được close = {price}", flush=True)
            return price
        except Exception as e:
            print(f"[FETCH] ❌ LỖI EXCEPTION khi lấy {symbol}: {type(e).__name__} - {e}", flush=True)
            print(traceback.format_exc(), flush=True) # In toàn bộ traceback
            return 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            price = fut.result(timeout=FETCH_TIMEOUT)
        except concurrent.futures.TimeoutError:
            print(f"[FETCH] ❌ TIMEOUT {FETCH_TIMEOUT}s khi lấy {symbol}", flush=True)
            return None
        except Exception as e:
            print(f"[FETCH] ❌ LỖI LUỒNG khi lấy {symbol}: {e}", flush=True)
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
        _cache[symbol] = {
            "close": close,
            "updated_at": _now_str(),
            "ts": time.time(),
        }

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
                "updated_at": cached["updated_at"],
                "source": "cache",
            }

    print(f"[PRICE] Không có cache, gọi _fetch_one cho {symbol}", flush=True)
    price = _fetch_one(symbol)
    if price is None:
        print(f"[PRICE] ❌ {symbol}: Trả về lỗi (None) cho Frontend", flush=True)
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
    return jsonify({"service": "quant-price-api", "status": "ok"})

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _now_str()})

@app.get("/api/price/<symbol>")
def api_price(symbol: str):
    symbol = symbol.strip().upper()
    if not symbol.isalnum() or len(symbol) > 10:
        abort(400, description="Invalid symbol")

    no_cache = request.args.get("nocache") == "1"
    print(f"[API] Có request lấy giá {symbol} (nocache={no_cache})", flush=True)
    result = _get_price(symbol, use_cache=not no_cache)
    
    if result["close"] > 0:
        return jsonify(result), 200
    else:
        return jsonify(result), 502

@app.get("/api/prices")
def api_prices():
    raw = request.args.get("symbols", "").strip()
    if not raw:
        abort(400, description="Missing ?symbols=")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    no_cache = request.args.get("nocache") == "1"
    
    print(f"[API] Có request lấy list mã: {symbols}", flush=True)
    data = {sym: _get_price(sym, use_cache=not no_cache) for sym in symbols}
    return jsonify({"updated_at": _now_str(), "data": data})

# ===================== BATCH MODE (Giữ nguyên) =====================
def _load_json(path: str, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception as e:
        print(f"⚠️ Lỗi đọc {path}: {e}", flush=True)
        return default

def _get_symbols_batch() -> List[str]:
    data = _load_json(SYMBOLS_FILE, None)
    if isinstance(data, list) and data:
        if isinstance(data[0], dict): return [item["symbol"] for item in data if "symbol" in item]
        return data
    stock_db = _load_json(DATA_FILE, {})
    if stock_db: return list(stock_db.keys())
    return []

def run_batch():
    symbols = _get_symbols_batch()
    if not symbols: return
    stock_db = _load_json(DATA_FILE, {})
    now_str = _now_str()
    print("=" * 60, flush=True)
    print(f"🚀 BATCH CẬP NHẬT GIÁ — {len(symbols)} mã", flush=True)
    print("=" * 60, flush=True)
    ok = fail = 0
    for i, sym in enumerate(symbols, 1):
        print(f"[{i:>3}/{len(symbols)}] {sym:<6} ...", end="", flush=True)
        price = _fetch_one(sym)
        if price and price > 0:
            stock_db.setdefault(sym, {})
            stock_db[sym]["close"] = price
            stock_db[sym]["updated_at"] = now_str
            print(f" ✅ {price:>10,}", flush=True)
            ok += 1
        else:
            print(f" ❌", flush=True)
            fail += 1
        time.sleep(1.0)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(stock_db, f, ensure_ascii=False, indent=4)
    with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_update": now_str}, f, ensure_ascii=False, indent=4)
    print(f"\n📊 TỔNG KẾT: {ok} OK | {fail} FAIL", flush=True)

# ===================== ENTRYPOINT =====================
if __name__ == "__main__":
    if "--batch" in sys.argv:
        run_batch()
    else:
        port = int(os.environ.get("PORT", "8000"))
        app.run(host="0.0.0.0", port=port, debug=False)
