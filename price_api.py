"""
Price API — cập nhật giá realtime cho Pro Quant Screener.

Hai chế độ:
  1) Server mode (mặc định): chạy HTTP API trên Render.
       python price_api.py
  2) Batch mode: cập nhật toàn bộ data.json (giữ tương thích pipeline cũ).
       python price_api.py --batch

Endpoints:
  GET /healthz
  GET /api/price/<symbol>                -> { symbol, close, updated_at, source }
  GET /api/prices?symbols=VCB,ACB,FPT   -> { updated_at, data: {SYM: {...}} }
"""

import os
import sys
import json
import time
import threading
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Optional

from flask import Flask, jsonify, request, abort
from flask_cors import CORS

from lotusmarket import fetchers

# ===================== CONFIG =====================
DATA_FILE = "data.json"
SYMBOLS_FILE = "vn100_symbols.json"
LAST_UPDATE_FILE = "last_update.json"

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "8"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "45"))       # giây
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

# In-memory cache: { "VCB": {"close": 92000, "updated_at": "...", "ts": 1699...} }
_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()

# ===================== HELPERS =====================
def _now_str() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def _fetch_one(symbol: str) -> Optional[int]:
    """
    Trả về giá close (int, VND) hoặc None nếu fail.
    Dùng ThreadPoolExecutor để ép timeout cứng — tránh treo request HTTP.
    """
    def _do():
        try:
            stock = fetchers.vps(symbol)
            price = getattr(stock, "close", 0) or 0
            return price
        except Exception:
            return 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            price = fut.result(timeout=FETCH_TIMEOUT)
        except (concurrent.futures.TimeoutError, Exception):
            return None

    if not price or price <= 0:
        return None

    # lotusmarket trả VND (VD 20350). Heuristic cũ: nếu < 1000 thì × 1000.
    # Giữ nguyên hành vi cũ để không phá pipeline, nhưng log rõ để dễ debug.
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
    """Trả về dict {symbol, close, updated_at, source} — source ∈ {cache, live, error}."""
    symbol = symbol.strip().upper()
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
@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _now_str()})


@app.get("/api/price/<symbol>")
def api_price(symbol: str):
    symbol = symbol.strip().upper()
    if not symbol.isalnum() or len(symbol) > 10:
        abort(400, description="Invalid symbol")

    no_cache = request.args.get("nocache") == "1"
    result = _get_price(symbol, use_cache=not no_cache)
    status = 200 if result["close"] > 0 else 502
    return jsonify(result), status


@app.get("/api/prices")
def api_prices():
    raw = request.args.get("symbols", "").strip()
    if not raw:
        abort(400, description="Missing ?symbols=")

    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols:
        abort(400, description="Empty symbols list")
    if len(symbols) > MAX_SYMBOLS_PER_REQUEST:
        abort(400, description=f"Too many symbols (max {MAX_SYMBOLS_PER_REQUEST})")

    no_cache = request.args.get("nocache") == "1"
    data = {}
    for sym in symbols:
        data[sym] = _get_price(sym, use_cache=not no_cache)

    return jsonify({"updated_at": _now_str(), "data": data})


# ===================== BATCH MODE (giữ tương thích) =====================
def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Lỗi đọc {path}: {e}", flush=True)
        return default


def _get_symbols_batch() -> List[str]:
    data = _load_json(SYMBOLS_FILE, None)
    if isinstance(data, list) and data:
        if isinstance(data[0], dict):
            return [item["symbol"] for item in data if "symbol" in item]
        return data

    stock_db = _load_json(DATA_FILE, {})
    if stock_db:
        return list(stock_db.keys())

    print(f"❌ Không tìm thấy {SYMBOLS_FILE} hoặc {DATA_FILE}.", flush=True)
    return []


def run_batch():
    symbols = _get_symbols_batch()
    if not symbols:
        return

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
            old = stock_db.get(sym, {}).get("close", 0)
            print(f" ❌ (giữ cũ: {old:,})", flush=True)
            fail += 1
        time.sleep(1.0)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(stock_db, f, ensure_ascii=False, indent=4)
    with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_update": now_str}, f, ensure_ascii=False, indent=4)

    print("\n" + "=" * 60, flush=True)
    print(f"📊 TỔNG KẾT: {ok} OK | {fail} FAIL — {now_str}", flush=True)
    print("=" * 60, flush=True)


# ===================== ENTRYPOINT =====================
if __name__ == "__main__":
    if "--batch" in sys.argv:
        run_batch()
    else:
        port = int(os.environ.get("PORT", "8000"))
        # Render sẽ inject $PORT. debug=False để production an toàn.
        app.run(host="0.0.0.0", port=port, debug=False)
