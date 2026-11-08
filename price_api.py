"""
Price API — Pro Quant Screener
Cơ chế Tối giản & Ổn định (Chống lỗi 500) + Lấy đầy đủ OHLC & Volume.
ĐỒNG BỘ TỰ ĐỘNG: Đọc danh sách mã từ vn100_symbols.json, so sánh với MongoDB (Thiếu thêm - Thừa xóa).
CÀO BÙ THÔNG MINH: Mã lỗi được đánh dấu is_incomplete. Chỉ cào bù các mã bị đánh dấu.
CHỐT SỔ LINH HOẠT: Sau 15h, tiếp tục cào bù cho đến khi tất cả các mã được chốt sổ (10 lần ghi/ngày).
CHỐNG MẤT DỮ LIỆU: Mã chưa đủ dữ liệu sẽ không được chốt sổ, giữ nguyên dữ liệu cũ.
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
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

SYMBOLS_JSON_FILE = "vn100_symbols.json"

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
MAX_SYMBOLS_PER_REQUEST = int(os.environ.get("MAX_SYMBOLS_PER_REQUEST", "20"))
COOLDOWN_SECONDS = 300

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== DATABASE & AUTO SYNC =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    settings_col = db['settings']
    gh_history = db['github_sync_history']
    print("✅ Kết nối MongoDB thành công!", flush=True)
    
    # TỰ ĐỘNG ĐỒNG BỘ DANH SÁCH MÃ TỪ FILE JSON SANG MONGODB
    if os.path.exists(SYMBOLS_JSON_FILE):
        try:
            with open(SYMBOLS_JSON_FILE, "r", encoding="utf-8") as f:
                json_symbols = set([str(s).upper().strip() for s in json.load(f)])
            
            if len(json_symbols) > 0:
                print(f"🔄 Đang đồng bộ danh sách mã từ {SYMBOLS_JSON_FILE} sang MongoDB...", flush=True)
                
                db_symbols = set([doc["symbol"] for doc in collection.find({}, {"symbol": 1})])
                
                symbols_to_add = json_symbols - db_symbols
                if symbols_to_add:
                    print(f"➕ Thêm {len(symbols_to_add)} mã mới vào DB: {', '.join(symbols_to_add)}", flush=True)
                    for sym in symbols_to_add:
                        collection.insert_one({"symbol": sym})

                symbols_to_remove = db_symbols - json_symbols
                if symbols_to_remove:
                    print(f"➖ Xóa {len(symbols_to_remove)} mã không còn trong JSON khỏi DB: {', '.join(symbols_to_remove)}", flush=True)
                    collection.delete_many({"symbol": {"$in": list(symbols_to_remove)}})
                
                if not symbols_to_add and not symbols_to_remove:
                    print("ℹ️ Danh sách mã trên MongoDB đã đồng bộ hoàn toàn với file JSON.", flush=True)
            else:
                print(f"⚠️ File {SYMBOLS_JSON_FILE} rỗng. Bỏ qua đồng bộ để bảo vệ dữ liệu.", flush=True)
        except Exception as e:
            print(f"⚠️ Lỗi khi đọc/đồng bộ file {SYMBOLS_JSON_FILE}: {e}", flush=True)
    else:
        print(f"⚠️ Không tìm thấy file {SYMBOLS_JSON_FILE}. Server sẽ dùng danh sách hiện có trong MongoDB.", flush=True)

except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

is_crawling = False
crawl_lock = threading.Lock()
LAST_CRAWL_TIME = {}
LAST_GITHUB_WRITE_TIME = 0

# ===================== HELPERS =====================
def _now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M:%S")

def _get_trading_date_str() -> str:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    while vn_now.weekday() >= 5:
        vn_now -= timedelta(days=1)
    return vn_now.strftime("%Y-%m-%d")

def _is_after_15h() -> bool:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.hour >= 15

def _fetch_one(symbol: str) -> Optional[dict]:
    def _do():
        try:
            stock = fetchers.vps(symbol)
            if not stock: return None
            return {
                "close": getattr(stock, "close", 0) or 0,
                "open": getattr(stock, "open", 0) or 0,
                "high": getattr(stock, "high", 0) or 0,
                "low": getattr(stock, "low", 0) or 0,
                "volume": getattr(stock, "volume", 0) or getattr(stock, "nmVolume", 0) or getattr(stock, "vol", 0)
            }
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            data = fut.result(timeout=FETCH_TIMEOUT)
        except Exception:
            return None

    # CHỐT CHẶN: Nếu OHLC = 0 là ngày nghỉ -> Bỏ qua
    if not data or data["close"] <= 0 or data["open"] <= 0 or data["high"] <= 0 or data["low"] <= 0:
        return None
    
    if data["close"] < 1000:
        for k in ["close", "open", "high", "low"]:
            if data[k] > 0: data[k] = data[k] * 1000
            
    return {
        "close": int(data["close"]), "open": int(data["open"]),
        "high": int(data["high"]), "low": int(data["low"]), "volume": int(data["volume"])
    }

# ===================== GITHUB SYNC =====================
def get_github_file_data(file_path):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return None, None
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200:
            file_info = res.json()
            return json.loads(base64.b64decode(file_info["content"]).decode("utf-8")), file_info["sha"]
        elif res.status_code == 404:
            return {"history_d1": [], "history_w1": [], "history_mn": []}, None
        return None, None
    except Exception:
        return None, None

def update_github_file(file_path, data_dict, commit_message):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return False
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    _, sha = get_github_file_data(file_path)
    payload = {
        "message": commit_message, 
        "content": base64.b64encode(json.dumps(data_dict, ensure_ascii=False, indent=4).encode("utf-8")).decode("utf-8"), 
        "branch": GITHUB_BRANCH
    }
    if sha: payload["sha"] = sha
    try:
        res = requests.put(api_url, headers=headers, json=payload, timeout=15)
        if res.status_code in [200, 201]:
            if gh_history: gh_history.insert_one({"time": datetime.utcnow(), "file": file_path})
            return True
    except Exception:
        pass
    return False

def can_update_github_today() -> bool:
    global LAST_GITHUB_WRITE_TIME
    # Lớp 4: Chống spam (Phải cách lần ghi trước ít nhất 10 phút)
    if time.time() - LAST_GITHUB_WRITE_TIME < 600: return False
    if not gh_history: 
        LAST_GITHUB_WRITE_TIME = time.time()
        return True
    vn_now = datetime.utcnow() + timedelta(hours=7)
    start_of_day_utc = vn_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=7)
    count_today = gh_history.count_documents({"time": {"$gte": start_of_day_utc}})
    # Tăng giới hạn lên 10 lần/ngày để đảm bảo chốt sổ đầy đủ
    if count_today >= 10: return False
    LAST_GITHUB_WRITE_TIME = time.time()
    return True

# ===================== CORE CRAWL LOGIC =====================
def _execute_crawl(symbols: list, is_retry: bool = False):
    global is_crawling
    if is_crawling: return
    is_crawling = True
    try:
        now_str = _now_str()
        trading_date = _get_trading_date_str()
        updated_data = {}
        
        mode_str = "CÀO BÙ" if is_retry else "CÀO MỚI"
        print(f"[CRAWL] 🚀 [{mode_str}] Bắt đầu cào {len(symbols)} mã (Ngày giao dịch: {trading_date})...", flush=True)
        
        for sym in symbols:
            # KHÓA SỔ TỪNG MÃ: Nếu đã chốt sổ hôm nay thì bỏ qua cào API
            doc = collection.find_one({"symbol": sym})
            if doc and doc.get("chot_so_today") == trading_date:
                print(f"[CRAWL] 🔒 {sym}: Đã chốt sổ ngày {trading_date}. Bỏ qua cào API.", flush=True)
                if "close" in doc:
                    updated_data[sym] = doc["close"]
                continue

            data = _fetch_one(sym)
            
            if data:
                # 1. LƯU VÀO MONGODB (Luôn cập nhật giá realtime)
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {
                        "symbol": sym, "close": data["close"], 
                        "open": data["open"], "high": data["high"], 
                        "low": data["low"], "volume": data["volume"],
                        "trading_date": trading_date,
                        "updated_at": now_str,
                        "is_locked": False,
                        "is_incomplete": False # Đánh dấu đã đầy đủ
                    }},
                    upsert=True
                )
                updated_data[sym] = data["close"]
                LAST_CRAWL_TIME[sym] = time.time()
                
                # 2. CHỐT SỔ SAU 15H VÀ CHƯA ĐƯỢC CHỐT
                if _is_after_15h() and can_update_github_today():
                    history_path = f"history/{sym}.json"
                    hist_data, _ = get_github_file_data(history_path)
                    
                    if hist_data is not None:
                        d1_bars = hist_data.get("history_d1", [])
                        w1_bars = hist_data.get("history_w1", [])
                        mn_bars = hist_data.get("history_mn", [])
                        
                        # --- XỬ LÝ D1 ---
                        d1_bar = {"time": trading_date, "open": data["open"], "high": data["high"], "low": data["low"], "close": data["close"], "volume": data["volume"]}
                        d1_idx = next((i for i, b in enumerate(d1_bars) if b["time"] == trading_date), -1)
                        if d1_idx == -1: d1_bars.append(d1_bar)
                        else: d1_bars[d1_idx] = d1_bar
                        d1_bars.sort(key=lambda x: x["time"])
                        
                        # --- XỬ LÝ W1 (Tuần) ---
                        vn_now = datetime.utcnow() + timedelta(hours=7)
                        start_of_week = vn_now - timedelta(days=vn_now.weekday())
                        week_str = start_of_week.strftime("%Y-%m-%d")
                        
                        week_bars_in_d1 = [b for b in d1_bars if datetime.strptime(b["time"], "%Y-%m-%d") >= start_of_week.replace(hour=0, minute=0, second=0)]
                        if week_bars_in_d1:
                            w1_bar = {
                                "time": week_str,
                                "open": week_bars_in_d1[0]["open"],
                                "high": max(b["high"] for b in week_bars_in_d1),
                                "low": min(b["low"] for b in week_bars_in_d1),
                                "close": week_bars_in_d1[-1]["close"],
                                "volume": sum(b["volume"] for b in week_bars_in_d1)
                            }
                            w1_idx = next((i for i, b in enumerate(w1_bars) if b["time"] == week_str), -1)
                            if w1_idx == -1: w1_bars.append(w1_bar)
                            else: w1_bars[w1_idx] = w1_bar
                            w1_bars.sort(key=lambda x: x["time"])

                        # --- XỬ LÝ MN (Tháng) ---
                        month_str = vn_now.strftime("%Y-%m-01")
                        month_bars_in_d1 = [b for b in d1_bars if b["time"].startswith(vn_now.strftime("%Y-%m"))]
                        if month_bars_in_d1:
                            mn_bar = {
                                "time": month_str,
                                "open": month_bars_in_d1[0]["open"],
                                "high": max(b["high"] for b in month_bars_in_d1),
                                "low": min(b["low"] for b in month_bars_in_d1),
                                "close": month_bars_in_d1[-1]["close"],
                                "volume": sum(b["volume"] for b in month_bars_in_d1)
                            }
                            mn_idx = next((i for i, b in enumerate(mn_bars) if b["time"] == month_str), -1)
                            if mn_idx == -1: mn_bars.append(mn_bar)
                            else: mn_bars[mn_idx] = mn_bar
                            mn_bars.sort(key=lambda x: x["time"])

                        # Đẩy lên GitHub
                        hist_data["history_d1"] = d1_bars
                        hist_data["history_w1"] = w1_bars
                        hist_data["history_mn"] = mn_bars
                        
                        if update_github_file(history_path, hist_data, f"Chốt sổ D1/W1/MN {sym} ngày {trading_date}"):
                            # ĐÁNH DẤU ĐÃ CHỐT SỔ VÀ KHÓA
                            collection.update_one({"symbol": sym}, {"$set": {"chot_so_today": trading_date, "is_locked": True}})
                            print(f"[GITHUB] 📤 Đã chốt sổ {sym} (D1/W1/MN) ngày {trading_date}", flush=True)

                print(f"[CRAWL] ✅ {sym}: {data['close']:,} | KL: {data['volume']:,}", flush=True)
            else:
                # ĐÁNH DẤU LỖI (KHÔNG ghi đè giá trị cũ của ngày khác)
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {
                        "is_incomplete": True,
                        "last_error_at": now_str
                    }},
                    upsert=True
                )
                print(f"[CRAWL] ⚠️ {sym}: Lỗi API. Đã đánh dấu để cào bù.", flush=True)

        if len(symbols) >= 5:
            settings_col.update_one({"_id": "global_last_update"}, {"$set": {"time": now_str}}, upsert=True)

    finally:
        is_crawling = False

# ===================== BACKGROUND WORKER (AUTO 15m) =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật luồng tự động cập nhật giá (15 phút/lần).", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            is_after_15h = vn_now.hour >= 15

            # 1. Reset cờ khi sang ngày mới
            collection.update_many(
                {"chot_so_today": {"$ne": today_str}},
                {"$unset": {"chot_so_today": "", "is_locked": ""}}
            )

            # 2. Lấy danh sách tất cả các mã
            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            
            # 3. Phân loại mã cần cào
            incomplete_symbols = []  # Mã bị lỗi API
            unchot_symbols = []      # Mã chưa được chốt sổ hôm nay (đủ dữ liệu nhưng chưa push)
            
            for sym in all_symbols:
                doc = collection.find_one({"symbol": sym})
                if not doc: 
                    incomplete_symbols.append(sym)
                    continue
                
                # Mã bị lỗi dữ liệu
                if doc.get("is_incomplete"):
                    incomplete_symbols.append(sym)
                    continue
                
                # Mã chưa chốt sổ hôm nay (dù đã có dữ liệu)
                if doc.get("trading_date") == today_str and doc.get("chot_so_today") != today_str:
                    if is_after_15h:
                        unchot_symbols.append(sym)
            
            # 4. Ưu tiên 1: Cào bù các mã bị lỗi
            if incomplete_symbols:
                print(f"[AUTO] 🔄 Phát hiện {len(incomplete_symbols)} mã bị lỗi. Đang cào bù...", flush=True)
                _execute_crawl(incomplete_symbols, is_retry=True)

            # 5. Ưu tiên 2: Chốt sổ các mã đã có dữ liệu nhưng chưa được push
            if is_after_15h and unchot_symbols:
                print(f"[AUTO] 📤 Phát hiện {len(unchot_symbols)} mã đủ dữ liệu nhưng chưa chốt sổ. Đang xử lý...", flush=True)
                _execute_crawl(unchot_symbols)
            
            # 6. Ưu tiên 3: Cào mới các mã chưa có dữ liệu hôm nay (trong phiên)
            if not is_after_15h:
                symbols_to_crawl = []
                for sym in all_symbols:
                    doc = collection.find_one({"symbol": sym})
                    if not doc or doc.get("trading_date") != today_str:
                        symbols_to_crawl.append(sym)
                
                if symbols_to_crawl:
                    _execute_crawl(symbols_to_crawl)

            # 7. Báo cáo tình trạng
            if is_after_15h:
                total = len(all_symbols)
                completed = total - len(incomplete_symbols) - len(unchot_symbols)
                if incomplete_symbols or unchot_symbols:
                    print(f"[AUTO] ⏳ Tiến độ chốt sổ: {completed}/{total} mã. Còn {len(incomplete_symbols)} lỗi, {len(unchot_symbols)} chưa chốt.", flush=True)
                else:
                    print(f"[AUTO] ✅ HOÀN TẤT 100%: Đã chốt sổ toàn bộ {total} mã hôm nay!", flush=True)

            time.sleep(900)
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            time.sleep(60)

threading.Thread(target=auto_crawler_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/api/data")
def api_get_data():
    try:
        cursor = collection.find({}, {"_id": 0})
        data = {}
        for doc in cursor:
            sym = doc.get("symbol")
            if sym:
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
    
    if is_crawling:
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc: updated_data[sym] = doc["close"]
        return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào)", "updated_data": updated_data})

    can_crawl = True
    for sym in symbols:
        if time.time() - LAST_CRAWL_TIME.get(sym, 0) < COOLDOWN_SECONDS:
            can_crawl = False
            break
            
    if not can_crawl:
        updated_data = {}
        for sym in symbols:
            doc = collection.find_one({"symbol": sym})
            if doc and "close" in doc: updated_data[sym] = doc["close"]
        return jsonify({"status": "success", "updated_at": _now_str() + " (Cache 5 phút)", "updated_data": updated_data})

    threading.Thread(target=_execute_crawl, args=(symbols,)).start()
    
    updated_data = {}
    for sym in symbols:
        doc = collection.find_one({"symbol": sym})
        if doc and "close" in doc: updated_data[sym] = doc["close"]

    return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào ngầm)", "updated_data": updated_data})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
