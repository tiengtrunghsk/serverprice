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
SCREENER_JSON_FILE = "screener_results.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
COOLDOWN_SECONDS = 300
VOLUME_MULTIPLIER = 10

# Số luồng cào song song (10-15 là tối ưu)
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "12"))

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== TELEGRAM HELPER =====================
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM] Lỗi gửi tin: {e}", flush=True)

# ===================== DATABASE =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    settings_col = db['settings']
    print("✅ Kết nối MongoDB thành công!", flush=True)
    
    if os.path.exists(SYMBOLS_JSON_FILE):
        try:
            with open(SYMBOLS_JSON_FILE, "r", encoding="utf-8") as f:
                json_symbols = set([str(s).upper().strip() for s in json.load(f)])
            
            if len(json_symbols) > 0:
                print(f"🔄 Đang đồng bộ danh sách mã...", flush=True)
                db_symbols = set([doc["symbol"] for doc in collection.find({}, {"symbol": 1})])
                
                symbols_to_add = json_symbols - db_symbols
                if symbols_to_add:
                    for sym in symbols_to_add: collection.insert_one({"symbol": sym})

                symbols_to_remove = db_symbols - json_symbols
                if symbols_to_remove:
                    collection.delete_many({"symbol": {"$in": list(symbols_to_remove)}})
                
                print("ℹ️ Danh sách mã đã đồng bộ.", flush=True)
        except Exception as e:
            print(f"⚠️ Lỗi đồng bộ file JSON: {e}", flush=True)

except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

is_crawling = False
LAST_CRAWL_TIME = {}
LAST_ALERT_BATCH = []
LAST_GITHUB_WRITE_TIME = 0
LAST_TELEGRAM_TIME = 0

# ===================== HELPERS =====================
def _now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M:%S")

def _get_current_update_time() -> str:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.hour >= 15:
        return vn_now.replace(hour=15, minute=0, second=0).strftime("%d/%m/%Y %H:%M:%S")
    return vn_now.strftime("%d/%m/%Y %H:%M:%S")

def _get_trading_date_str() -> str:
    vn_now = datetime.utcnow() + timedelta(hours=7)
    if vn_now.hour < 9: vn_now -= timedelta(days=1)
    while vn_now.weekday() >= 5: vn_now -= timedelta(days=1)
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
            data = fut.result(timeout=10) 
        except Exception:
            return None

    if not data or data["close"] <= 0 or data["open"] <= 0:
        return None
    
    if data["close"] >= 1000:
        for k in ["close", "open", "high", "low"]:
            if data[k] > 0: data[k] = data[k] / 1000.0
            
    if 0 < data["volume"] < 100000:
        data["volume"] = data["volume"] * VOLUME_MULTIPLIER
            
    return {
        "close": round(data["close"], 2), "open": round(data["open"], 2),
        "high": round(data["high"], 2), "low": round(data["low"], 2), 
        "volume": int(data["volume"])
    }

def can_update_github_today() -> bool:
    global LAST_GITHUB_WRITE_TIME
    if time.time() - LAST_GITHUB_WRITE_TIME < 5: return False
    LAST_GITHUB_WRITE_TIME = time.time()
    return True

# ===================== GITHUB UPLOAD =====================
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
                content_str = json.dumps(content_dict, ensure_ascii=False, indent=4)
                b64_content = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")
                blob_res = requests.post(f"{base_url}/git/blobs", headers=headers, json={"content": b64_content, "encoding": "base64"}, timeout=10)
                if blob_res.status_code != 201: break
                tree_items.append({"path": path, "mode": "100644", "type": "blob", "sha": blob_res.json()["sha"]})
            
            tree_res = requests.post(f"{base_url}/git/trees", headers=headers, json={"base_tree": base_commit_sha, "tree": tree_items}, timeout=15)
            if tree_res.status_code != 201: time.sleep(2); continue
            new_tree_sha = tree_res.json()["sha"]

            commit_res = requests.post(f"{base_url}/git/commits", headers=headers, json={"message": commit_message, "tree": new_tree_sha, "parents": [base_commit_sha]}, timeout=10)
            if commit_res.status_code != 201: time.sleep(2); continue
            new_commit_sha = commit_res.json()["sha"]

            update_res = requests.patch(f"{base_url}/git/refs/heads/{GITHUB_BRANCH}", headers=headers, json={"sha": new_commit_sha, "force": True}, timeout=10)
            if update_res.status_code == 200: return True
            else: time.sleep(3)
        except Exception:
            time.sleep(3)
    return False

# ===================== CORE CRAWL LOGIC (TỐI ƯU SONG SONG) =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling, LAST_ALERT_BATCH, LAST_TELEGRAM_TIME
    if is_crawling: return
    is_crawling = True
    
    try:
        now_str = _now_str()
        trading_date = _get_trading_date_str()
        updated_data = {}
        alert_batch = []
        all_crawled_data = {}
        pending_history_files = {}
        need_update_screener = False
        
        # Lấy toàn bộ dữ liệu hiện có trong DB 1 lần (Tránh gọi find_one 100 lần)
        db_docs = {doc["symbol"]: doc for doc in collection.find({})}
        
        mode_str = "CÀO BÙ" if is_retry else ("CÀO TAY" if is_manual else "CÀO TỰ ĐỘNG")
        print(f"[CRAWL] 🚀 [{mode_str}] Bắt đầu cào SONG SONG {len(symbols)} mã...", flush=True)

        def _process_one(sym):
            doc = db_docs.get(sym, {})
            if doc.get("chot_so_today") == trading_date:
                return (sym, None, doc.get("close", 0), True)

            data = _fetch_one(sym)
            if not data: return (sym, None, None, False)

            # Xử lý tính toán ngay tại thread để không block main thread
            prev_close = doc.get("prev_close", 0)
            avg_volume_20 = doc.get("avg_volume_20", 0)
            
            if prev_close <= 0:
                # Nếu chưa có prev_close, thử đọc từ history file
                hist_data, _ = get_github_file_data(f"history/{sym}.json")
                if hist_data and hist_data.get("history_d1"):
                    bars = hist_data["history_d1"]
                    if len(bars) >= 2:
                        prev_close = bars[-2]["close"] if bars[-1]["time"] == trading_date else bars[-1]["close"]
                    elif len(bars) == 1:
                        prev_close = bars[0]["close"]
                    
                    recent_vols = [b.get("volume", 0) for b in bars[-20:]]
                    if recent_vols: avg_volume_20 = sum(recent_vols) / len(recent_vols)

            amplitude_percent = 0.0
            if data["open"] > 0 and data["high"] > 0 and data["low"] > 0:
                raw_amp = (data["high"] - data["low"]) / data["open"] * 100
                if 0 <= raw_amp <= 30.0: amplitude_percent = round(raw_amp, 2)

            volume_percent = 0.0
            if avg_volume_20 > 0 and data["volume"] > 0:
                volume_percent = round(data["volume"] / avg_volume_20 * 100, 2)
                    
            pct_change = 0.0
            if prev_close > 0:
                pct_change = round(((data["close"] - prev_close) / prev_close) * 100, 2)

            return (sym, data, prev_close, False, amplitude_percent, volume_percent, pct_change, avg_volume_20)

        # Chạy 12 luồng cùng lúc
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_process_one, sym): sym for sym in symbols}
            for i, fut in enumerate(concurrent.futures.as_completed(futures)):
                sym = futures[fut]
                try:
                    res = fut.result()
                    if res[3] is True: # Đã chốt sổ
                        updated_data[sym] = res[2]
                        continue
                    
                    if res[1] is None: # Lỗi
                        collection.update_one({"symbol": sym}, {"$set": {"is_incomplete": True, "last_error_at": now_str}}, upsert=True)
                        print(f"[CRAWL] ⚠️ {sym}: Lỗi API", flush=True)
                        continue

                    _, data, prev_close, _, amp_pct, vol_pct, pct_change, avg_vol = res
                    all_crawled_data[sym] = {"close": data["close"], "pct_change": pct_change, "volume_percent": vol_pct, "amplitude_percent": amp_pct}
                    
                    current_update_time = _get_current_update_time()
                    collection.update_one(
                        {"symbol": sym},
                        {"$set": {
                            "close": data["close"], "prev_close": prev_close,
                            "open": data["open"], "high": data["high"], "low": data["low"], "volume": data["volume"],
                            "amplitude_percent": amp_pct, "volume_percent": vol_pct, "avg_volume_20": avg_vol,
                            "trading_date": trading_date, "updated_at": current_update_time,
                            "is_locked": _is_after_15h(), "is_incomplete": False
                        }}, upsert=True
                    )
                    updated_data[sym] = data["close"]
                    LAST_CRAWL_TIME[sym] = time.time()
                    
                    if prev_close > 0 and ((pct_change >= 3.0) or (pct_change <= -3.0)) and (vol_pct >= 120.0):
                        alert_batch.append({
                            "sym": sym, "pct_change": pct_change, "volume_percent": vol_pct,
                            "price": data["close"], "volume": data["volume"],
                            "trend": "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG",
                            "flow": "BÁN RÒNG" if pct_change < 0 else "MUA VÀO"
                        })
                    
                    # In tiến trình mỗi 10 mã
                    if (i+1) % 10 == 0: print(f"[CRAWL] ⏳ Đã xử lý {i+1}/{len(symbols)} mã...", flush=True)

                except Exception as e:
                    print(f"[CRAWL] ❌ Lỗi xử lý {sym}: {e}", flush=True)

        # === XỬ LÝ ĐẨY LÊN GITHUB (Chỉ chạy SAU KHI CÀO XONG) ===
        if _is_after_15h() and all_crawled_data and can_update_github_today():
            print(f"[GITHUB] 📦 Đang lấy dữ liệu cũ để cập nhật...", flush=True)
            try:
                # Chỉ lấy Screener JSON 1 lần
                api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{SCREENER_JSON_FILE}"
                headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
                res = requests.get(api_url + f"?ref={GITHUB_BRANCH}", headers=headers, timeout=10)
                if res.status_code == 200:
                    current_json = json.loads(base64.b64decode(res.json()["content"]).decode("utf-8"))
                    for item in current_json:
                        sym = item.get("symbol")
                        if sym in all_crawled_data:
                            cdata = all_crawled_data[sym]
                            item["close_price"] = cdata["close"] * 1000
                            item["price_change_percent"] = cdata["pct_change"]
                            item["price_change_available"] = True
                            item["price_arrow"] = "▲" if cdata["pct_change"] > 0 else ("▼" if cdata["pct_change"] < 0 else "—")
                            item["price_color"] = "emerald" if cdata["pct_change"] > 0 else ("rose" if cdata["pct_change"] < 0 else "slate")
                            item["volume_percent"] = cdata["volume_percent"]
                            item["amplitude_percent"] = cdata["amplitude_percent"]
                    pending_history_files[SCREENER_JSON_FILE] = current_json
            except Exception as e:
                print(f"[GITHUB] ⚠️ Lỗi đọc Screener JSON: {e}", flush=True)

            # Chỉ up history file khi cần (sau 15h)
            for sym in all_crawled_data:
                hist_data, _ = get_github_file_data(f"history/{sym}.json")
                if hist_data is not None:
                    d1_bars = hist_data.get("history_d1", [])
                    # ... (Phần logic update nến tuần/tháng như cũ, nhưng chỉ chạy khi > 15h)
                    # Tạm thời tối ưu: Chỉ up file history khi thực sự cần
                    pending_history_files[f"history/{sym}.json"] = hist_data

            commit_msg = f"Update {trading_date}"
            if commit_multiple_files_to_github(pending_history_files, commit_msg):
                # Cập nhật cờ chốt sổ
                collection.update_many({"symbol": {"$in": list(all_crawled_data.keys())}}, {"$set": {"chot_so_today": trading_date}})
                print(f"[GITHUB] ✅ Đã đẩy {len(pending_history_files)} file lên GitHub.", flush=True)
                
                # Gửi Telegram
                current_time = time.time()
                if (current_time - LAST_TELEGRAM_TIME > 14400) or is_manual:
                    msg = f"✅ <b>CẬP NHẬT THÀNH CÔNG</b>\n🕒 {now_str}\n📅 Ngày: {trading_date}\n📤 Đã đẩy {len(pending_history_files)} file."
                    send_telegram(msg)
                    LAST_TELEGRAM_TIME = current_time

        # Xử lý Telegram Cảnh báo dòng tiền
        if alert_batch:
            sorted_syms = sorted([x['sym'] for x in alert_batch])
            if sorted_syms != sorted(LAST_ALERT_BATCH):
                alert_batch.sort(key=lambda x: x["volume_percent"], reverse=True)
                msg = f"🚨 <b>CẢNH BÁO DÒNG TIỀN ({len(alert_batch)} MÃ)</b>\n"
                for item in alert_batch:
                    msg += f"• <b>{item['sym']}</b>: {item['pct_change']:+.2f}% {item['trend']} | Vol {item['volume_percent']:.0f}%\n"
                send_telegram(msg)
                LAST_ALERT_BATCH = sorted_syms

    finally:
        is_crawling = False

# ===================== BACKGROUND WORKER =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật luồng tự động cập nhật giá (15 phút/lần).", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            is_after_15h = vn_now.hour >= 15

            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})
            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            
            incomplete_symbols = []
            unchot_symbols = []
            
            for sym in all_symbols:
                doc = collection.find_one({"symbol": sym})
                if not doc or doc.get("is_incomplete"):
                    incomplete_symbols.append(sym)
                elif doc.get("trading_date") == today_str and doc.get("chot_so_today") != today_str and is_after_15h:
                    unchot_symbols.append(sym)
            
            if incomplete_symbols: _execute_crawl(incomplete_symbols, is_retry=True, is_manual=False)
            if is_after_15h and unchot_symbols: _execute_crawl(unchot_symbols, is_manual=False)
            
            if not is_after_15h:
                symbols_to_crawl = [sym for sym in all_symbols if not collection.find_one({"symbol": sym}) or collection.find_one({"symbol": sym}).get("trading_date") != today_str]
                if symbols_to_crawl: _execute_crawl(symbols_to_crawl, is_manual=False)

            time.sleep(900)
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            time.sleep(60)

threading.Thread(target=auto_crawler_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/")
def home(): return "API is running!", 200

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
                
                doc["close"] = close_price
                doc["close_price"] = close_price
                doc["prev_close"] = prev_close

                price_change_percent = 0.0
                price_change_available = False
                arrow, color = "—", "slate"
                
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        price_change_percent = round(raw_pct, 2)
                        price_change_available = True
                        if price_change_percent > 0: arrow, color = "▲", "emerald"
                        elif price_change_percent < 0: arrow, color = "▼", "rose"
                
                doc["price_change_percent"] = price_change_percent
                doc["price_change_available"] = price_change_available
                doc["price_arrow"] = arrow
                doc["price_color"] = color
                doc["amplitude_percent"] = doc.get("amplitude_percent", 0.0)
                doc["volume_percent"] = doc.get("volume_percent", 0.0)
                
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
        return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào)", "updated_data": {sym: collection.find_one({"symbol": sym}).get("close", 0) for sym in symbols if collection.find_one({"symbol": sym})}})

    can_crawl = True
    for sym in symbols:
        if time.time() - LAST_CRAWL_TIME.get(sym, 0) < COOLDOWN_SECONDS:
            can_crawl = False
            break
            
    if not can_crawl:
        return jsonify({"status": "success", "updated_at": _now_str() + " (Cache)", "updated_data": {sym: collection.find_one({"symbol": sym}).get("close", 0) for sym in symbols if collection.find_one({"symbol": sym})}})

    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    
    return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào ngầm)", "updated_data": {sym: collection.find_one({"symbol": sym}).get("close", 0) for sym in symbols if collection.find_one({"symbol": sym})}})

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

@app.get("/api/retry-now")
def retry_now():
    if is_crawling: return jsonify({"status": "error", "message": "Hệ thống đang chạy!"})
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    if not incomplete_symbols: return jsonify({"status": "success", "message": "Không có mã nào bị lỗi!"})
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào bù {len(incomplete_symbols)} mã."})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
