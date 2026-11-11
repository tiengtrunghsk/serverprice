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

# ===================== VNSTOCK SDK =====================
import vnstock
from vnstock import Market

# ===================== CONFIG =====================
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
if not MONGO_URI:
    print("❌ LỖI: Thiếu MONGO_URI!", flush=True)
    sys.exit(1)

# Đăng nhập Vnstock bằng API Key (nếu có)
VNSTOCK_API_KEY = os.environ.get("VNSTOCK_API_KEY", "").strip()
if VNSTOCK_API_KEY:
    try:
        vnstock.login(VNSTOCK_API_KEY)
        print("✅ Đã đăng nhập Vnstock bằng API Key.", flush=True)
        # In ra thông tin hạn mức để kiểm tra
        try:
            print(vnstock.status(), flush=True)
            print(vnstock.usage(), flush=True)
        except:
            pass
    except Exception as e:
        print(f"⚠️ Lỗi đăng nhập Vnstock: {e}. Sẽ chạy ở cấp Khách.", flush=True)
else:
    print("⚠️ Không có VNSTOCK_API_KEY. Chạy ở cấp Khách (20 req/phút).", flush=True)

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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM] Lỗi gửi tin: {e}", flush=True)

# ===================== DATABASE & AUTO SYNC =====================
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
                    print("ℹ️ Danh sách mã trên MongoDB đã đồng bộ hoàn toàn.", flush=True)
            else:
                print(f"⚠️ File {SYMBOLS_JSON_FILE} rỗng. Bỏ qua đồng bộ.", flush=True)
        except Exception as e:
            print(f"⚠️ Lỗi khi đọc/đồng bộ file {SYMBOLS_JSON_FILE}: {e}", flush=True)
    else:
        print(f"⚠️ Không tìm thấy file {SYMBOLS_JSON_FILE}. Dùng danh sách hiện có.", flush=True)

except PyMongoError as e:
    print(f"❌ LỖI KẾT NỐI MONGODB: {e}", flush=True)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

is_crawling = False
crawl_lock = threading.Lock()
LAST_CRAWL_START_TIME = 0
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
    if vn_now.hour < 9:
        vn_now -= timedelta(days=1)
    while vn_now.weekday() >= 5:
        vn_now -= timedelta(days=1)
    return vn_now.strftime("%Y-%m-%d")

def _is_after_15h() -> bool:
    vn_time = datetime.utcnow() + timedelta(hours=7)
    return vn_time.hour >= 15

# ===================== FETCH DATA TỪ VNSTOCK 5 =====================
def _fetch_bulk_prices(symbols: list, retries: int = 3) -> dict:
    """Lấy giá realtime của toàn bộ mã trong 1 request duy nhất bằng Vnstock 5.x"""
    result = {}
    for attempt in range(retries):
        try:
            market = Market()
            board = market.quote(symbols)
            
            if board is None or board.empty:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return result

            for _, row in board.iterrows():
                sym = row.get('symbol')
                if not sym or sym not in symbols:
                    continue
                    
                close = float(row.get('close_price', 0) or 0)
                open_p = float(row.get('open_price', 0) or 0)
                high = float(row.get('high_price', 0) or 0)
                low = float(row.get('low_price', 0) or 0)
                vol = int(row.get('volume_accumulated', 0) or 0)
                
                if close <= 0 or open_p <= 0:
                    continue

                if close >= 1000:
                    close /= 1000.0
                    open_p /= 1000.0
                    high /= 1000.0
                    low /= 1000.0
                
                if 0 < vol < 100000:
                    vol = vol * VOLUME_MULTIPLIER

                result[sym] = {
                    "close": round(close, 2),
                    "open": round(open_p, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "volume": int(vol)
                }
            return result
        except Exception as e:
            print(f"[VNSTOCK] Lỗi fetch bulk (Lần {attempt+1}/{retries}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(3)
    
    return result

def can_update_github_today() -> bool:
    global LAST_GITHUB_WRITE_TIME
    if time.time() - LAST_GITHUB_WRITE_TIME < 5: return False
    LAST_GITHUB_WRITE_TIME = time.time()
    return True

# ===================== GITHUB BULK UPLOAD (TREES API) =====================
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

def commit_multiple_files_to_github(files_to_update: dict, commit_message: str, max_retries: int = 5):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]) or not files_to_update: 
        return False

    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    base_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}"

    for attempt in range(max_retries):
        try:
            print(f"[GITHUB] 🚀 Đang đẩy {len(files_to_update)} file (Lần thử {attempt + 1}/{max_retries})...", flush=True)
            
            ref_res = requests.get(f"{base_url}/git/ref/heads/{GITHUB_BRANCH}", headers=headers, timeout=10)
            if ref_res.status_code != 200:
                time.sleep(2); continue
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
            
            if update_res.status_code == 200:
                print(f"[GITHUB] ✅ Đã gom {len(files_to_update)} file vào 1 commit thành công!", flush=True)
                return True
            else:
                time.sleep(3)
        except Exception:
            time.sleep(3)

    print(f"[GITHUB] 🛑 Đã thử {max_retries} lần nhưng thất bại.", flush=True)
    return False

# ===================== CORE CRAWL LOGIC (BULK) =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling, LAST_ALERT_BATCH, LAST_TELEGRAM_TIME, LAST_CRAWL_START_TIME
    
    if is_crawling:
        if time.time() - LAST_CRAWL_START_TIME > 300:
            print("[CRAWL] ⚠️ Phát hiện kẹt tiến trình, tự động Reset!", flush=True)
            is_crawling = False
        else:
            return
        
    is_crawling = True
    LAST_CRAWL_START_TIME = time.time()
    
    try:
        now_str = _now_str()
        trading_date = _get_trading_date_str()
        updated_data = {}
        alert_batch = []
        all_crawled_data = {}
        
        pending_history_files = {}
        need_update_screener = False
        
        mode_str = "CÀO BÙ" if is_retry else ("CÀO TAY" if is_manual else "CÀO TỰ ĐỘNG")
        print(f"[CRAWL] 🚀 [{mode_str}] Bắt đầu lấy BULK giá cho {len(symbols)} mã...", flush=True)
        
        bulk_data = _fetch_bulk_prices(set(symbols))
        
        for i, sym in enumerate(symbols):
            doc = collection.find_one({"symbol": sym})
            
            if doc and doc.get("chot_so_today") == trading_date:
                if "close" in doc: updated_data[sym] = doc["close"]
                continue

            data = bulk_data.get(sym)
            
            if data:
                hist_data, _ = get_github_file_data(f"history/{sym}.json")
                prev_close = 0.0
                avg_volume_20 = 0.0
                
                if hist_data and hist_data.get("history_d1"):
                    bars = hist_data["history_d1"]
                    if len(bars) >= 2:
                        vn_now = datetime.utcnow() + timedelta(hours=7)
                        if vn_now.hour < 9:
                            prev_close = bars[-2]["close"]
                        else:
                            if bars[-1]["time"] == trading_date:
                                prev_close = bars[-2]["close"]
                            else:
                                prev_close = bars[-1]["close"]
                    elif len(bars) == 1:
                        prev_close = bars[0]["close"]
                        
                    recent_volumes = [b.get("volume", 0) for b in bars[-20:]]
                    if recent_volumes:
                        avg_volume_20 = sum(recent_volumes) / len(recent_volumes)

                amplitude_percent = 0.0
                if data["open"] > 0 and data["high"] > 0 and data["low"] > 0:
                    raw_amp = (data["high"] - data["low"]) / data["open"] * 100
                    if 0 <= raw_amp <= 30.0:
                        amplitude_percent = round(raw_amp, 2)

                volume_percent = 0.0
                if avg_volume_20 > 0 and data["volume"] > 0:
                    raw_vol = data["volume"] / avg_volume_20 * 100
                    if raw_vol >= 0:
                        volume_percent = round(raw_vol, 2)
                        
                pct_change = 0.0
                if prev_close > 0:
                    pct_change = round(((data["close"] - prev_close) / prev_close) * 100, 2)

                all_crawled_data[sym] = {
                    "close": data["close"],
                    "pct_change": pct_change,
                    "volume_percent": volume_percent,
                    "amplitude_percent": amplitude_percent
                }

                current_update_time = _get_current_update_time()

                collection.update_one(
                    {"symbol": sym},
                    {"$set": {
                        "symbol": sym, "close": data["close"], 
                        "prev_close": prev_close,
                        "open": data["open"], "high": data["high"], 
                        "low": data["low"], "volume": data["volume"],
                        "amplitude_percent": amplitude_percent,
                        "volume_percent": volume_percent,
                        "trading_date": trading_date,
                        "updated_at": current_update_time,
                        "is_locked": _is_after_15h(),
                        "is_incomplete": False
                    }},
                    upsert=True
                )
                updated_data[sym] = data["close"]
                LAST_CRAWL_TIME[sym] = time.time()
                
                if prev_close > 0:
                    is_price_shock = (pct_change >= 3.0) or (pct_change <= -3.0)
                    is_volume_shock = (volume_percent >= 120.0)
                    if is_price_shock and is_volume_shock:
                        trend_icon = "🔴 GIẢM" if pct_change < 0 else "🟢 TĂNG"
                        cash_flow = "BÁN RÒNG" if pct_change < 0 else "MUA VÀO"
                        alert_batch.append({
                            "sym": sym, "pct_change": pct_change, "volume_percent": volume_percent,
                            "price": data["close"], "volume": data["volume"], "trend": trend_icon, "flow": cash_flow
                        })

                if _is_after_15h() and can_update_github_today():
                    if hist_data is not None:
                        d1_bars = hist_data.get("history_d1", [])
                        w1_bars = hist_data.get("history_w1", [])
                        mn_bars = hist_data.get("history_mn", [])
                        
                        d1_bar = {"time": trading_date, "open": data["open"], "high": data["high"], "low": data["low"], "close": data["close"], "volume": data["volume"]}
                        d1_idx = next((i for i, b in enumerate(d1_bars) if b["time"] == trading_date), -1)
                        if d1_idx == -1: d1_bars.append(d1_bar)
                        else: d1_bars[d1_idx] = d1_bar
                        d1_bars.sort(key=lambda x: x["time"])
                        
                        vn_now = datetime.utcnow() + timedelta(hours=7)
                        start_of_week = vn_now - timedelta(days=vn_now.weekday())
                        week_str = start_of_week.strftime("%Y-%m-%d")
                        
                        week_bars_in_d1 = [b for b in d1_bars if datetime.strptime(b["time"], "%Y-%m-%d") >= start_of_week.replace(hour=0, minute=0, second=0)]
                        if week_bars_in_d1:
                            w1_bar = {
                                "time": week_str, "open": week_bars_in_d1[0]["open"],
                                "high": max(b["high"] for b in week_bars_in_d1),
                                "low": min(b["low"] for b in week_bars_in_d1),
                                "close": week_bars_in_d1[-1]["close"],
                                "volume": sum(b["volume"] for b in week_bars_in_d1)
                            }
                            w1_idx = next((i for i, b in enumerate(w1_bars) if b["time"] == week_str), -1)
                            if w1_idx == -1: w1_bars.append(w1_bar)
                            else: w1_bars[w1_idx] = w1_bar
                            w1_bars.sort(key=lambda x: x["time"])

                        month_str = vn_now.strftime("%Y-%m-01")
                        month_bars_in_d1 = [b for b in d1_bars if b["time"].startswith(vn_now.strftime("%Y-%m"))]
                        if month_bars_in_d1:
                            mn_bar = {
                                "time": month_str, "open": month_bars_in_d1[0]["open"],
                                "high": max(b["high"] for b in month_bars_in_d1),
                                "low": min(b["low"] for b in month_bars_in_d1),
                                "close": month_bars_in_d1[-1]["close"],
                                "volume": sum(b["volume"] for b in month_bars_in_d1)
                            }
                            mn_idx = next((i for i, b in enumerate(mn_bars) if b["time"] == month_str), -1)
                            if mn_idx == -1: mn_bars.append(mn_bar)
                            else: mn_bars[mn_idx] = mn_bar
                            mn_bars.sort(key=lambda x: x["time"])

                        hist_data["history_d1"] = d1_bars
                        hist_data["history_w1"] = w1_bars
                        hist_data["history_mn"] = mn_bars
                        
                        pending_history_files[f"history/{sym}.json"] = hist_data
                    
                    need_update_screener = True

                print(f"[CRAWL] ✅ {sym}: {data['close']:,} | Prev: {prev_close:,} | Vol: {volume_percent}%", flush=True)
            else:
                collection.update_one(
                    {"symbol": sym},
                    {"$set": {"is_incomplete": True, "last_error_at": now_str}},
                    upsert=True
                )
                print(f"[CRAWL] ⚠️ {sym}: Không có dữ liệu từ API bulk.", flush=True)

        if len(symbols) >= 5:
            settings_col.update_one({"_id": "global_last_update"}, {"$set": {"time": now_str}}, upsert=True)

        if alert_batch:
            current_symbols_sorted = sorted([item['sym'] for item in alert_batch])
            last_symbols_sorted = sorted(LAST_ALERT_BATCH)
            
            if current_symbols_sorted != last_symbols_sorted:
                alert_batch.sort(key=lambda x: x["volume_percent"], reverse=True)
                msg_lines = [f"🚨 <b>CẢNH BÁO DÒNG TIỀN ({len(alert_batch)} MÃ)</b>", "----------------------------------"]
                for item in alert_batch:
                    line = (f"• <b>{item['sym']}</b>: {item['pct_change']:+.2f}% {item['trend']} | "
                            f"Vol: {item['volume_percent']:.0f}% ({item['flow']})\n"
                            f"  Giá: {item['price']:,} | KL: {item['volume']:,}")
                    msg_lines.append(line)
                msg_lines.append("----------------------------------")
                msg_lines.append(f"🕒 Lúc: {now_str}")
                
                send_telegram("\n".join(msg_lines))
                print(f"[TELEGRAM] 🚀 Đã gửi tin cảnh báo {len(alert_batch)} mã.", flush=True)
                
                LAST_ALERT_BATCH.clear()
                LAST_ALERT_BATCH.extend(current_symbols_sorted)

        if pending_history_files or need_update_screener:
            print(f"[GITHUB] 📦 Đang gom file để đẩy lên Github...", flush=True)
            
            if need_update_screener and all_crawled_data:
                try:
                    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{SCREENER_JSON_FILE}"
                    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
                    res = requests.get(api_url + f"?ref={GITHUB_BRANCH}", headers=headers, timeout=10)
                    if res.status_code == 200:
                        current_json = json.loads(base64.b64decode(res.json()["content"]).decode("utf-8"))
                        updated_count = 0
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
                                updated_count += 1
                        
                        if updated_count > 0:
                            pending_history_files[SCREENER_JSON_FILE] = current_json
                except Exception as e:
                    print(f"[GITHUB] ⚠️ Lỗi khi đọc {SCREENER_JSON_FILE}: {e}", flush=True)

            commit_msg = f"Cập nhật dữ liệu ngày {trading_date}"
            if commit_multiple_files_to_github(pending_history_files, commit_msg):
                success_symbols = []
                for path in pending_history_files:
                    if path.startswith("history/"):
                        sym = path.split("/")[1].replace(".json", "")
                        collection.update_one({"symbol": sym}, {"$set": {"chot_so_today": trading_date, "is_locked": True}})
                        success_symbols.append(sym)
                
                current_time = time.time()
                if (current_time - LAST_TELEGRAM_TIME > 14400) or is_manual:
                    error_docs = collection.find({"is_incomplete": True}, {"symbol": 1})
                    error_symbols = [d["symbol"] for d in error_docs]
                    
                    msg = f"✅ <b>CẬP NHẬT DỮ LIỆU THÀNH CÔNG</b>\n"
                    msg += f"🕒 Thời gian: {now_str}\n"
                    msg += f"📤 Đã đẩy lên GitHub: <b>{len(success_symbols)} files</b>\n"
                    msg += f"📅 Ngày giao dịch: {trading_date}\n"
                    
                    if len(error_symbols) > 0:
                        msg += f"\n⚠️ <b>{len(error_symbols)} MÃ BỊ LỖI:</b>\n"
                        msg += f"<code>{', '.join(error_symbols[:15])}</code>"
                        if len(error_symbols) > 15:
                            msg += f"... và {len(error_symbols) - 15} mã khác."
                    else:
                        msg += "\n🎉 <b>Hoàn hảo! Không có mã nào bị lỗi.</b>"
                        
                    send_telegram(msg)
                    LAST_TELEGRAM_TIME = current_time

    finally:
        is_crawling = False

# ===================== BACKGROUND WORKER (ROLLING CRAWL) =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật chế độ cào cuốn chiếu (Rolling Crawl) - Cập nhật liên tục!", flush=True)
    
    time.sleep(5)
    
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            
            is_market_open = (9 <= vn_now.hour < 15) or (vn_now.hour == 15 and vn_now.minute == 0)
            if not is_market_open:
                time.sleep(60)
                continue

            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            
            symbols_to_crawl = []
            for sym in all_symbols:
                doc = collection.find_one({"symbol": sym}, {"chot_so_today": 1})
                if doc and doc.get("chot_so_today") == today_str:
                    continue 
                symbols_to_crawl.append(sym)
            
            if not symbols_to_crawl:
                time.sleep(30)
                continue

            if not is_crawling:
                threading.Thread(target=_execute_crawl, args=(symbols_to_crawl, False, False), daemon=True).start()
            
            time.sleep(5)

        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            time.sleep(10)

threading.Thread(target=auto_crawler_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/")
def home():
    return "API is running!", 200

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
                arrow = "—"
                color = "slate"
                
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        price_change_percent = round(raw_pct, 2)
                        price_change_available = True
                        
                        if price_change_percent > 0:
                            arrow = "▲"
                            color = "emerald"
                        elif price_change_percent < 0:
                            arrow = "▼"
                            color = "rose"
                
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

    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    
    updated_data = {}
    for sym in symbols:
        doc = collection.find_one({"symbol": sym})
        if doc and "close" in doc: updated_data[sym] = doc["close"]

    return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào ngầm)", "updated_data": updated_data})

@app.get("/api/reset-today")
def reset_today():
    today_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")
    result = collection.update_many(
        {"chot_so_today": today_str},
        {"$unset": {"chot_so_today": "", "is_locked": ""}}
    )
    return jsonify({"status": "success", "message": f"Đã xóa cờ chốt sổ của {result.modified_count} mã trong ngày hôm nay."})

@app.get("/api/force-fix")
def force_fix_data():
    collection.update_many({}, {"$unset": {"chot_so_today": "", "is_locked": "", "trading_date": ""}})
    all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
    threading.Thread(target=_execute_crawl, args=(all_symbols, False, True)).start()
    return jsonify({"status": "success", "message": f"Đang ép cào lại toàn bộ {len(all_symbols)} mã. Vui lòng đợi 1-2 phút!"})

@app.get("/api/retry-now")
def retry_now():
    if is_crawling:
        return jsonify({"status": "error", "message": "Hệ thống đang chạy, vui lòng đợi!"})
        
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    
    if not incomplete_symbols:
        return jsonify({"status": "success", "message": "Không có mã nào bị lỗi cần cào bù!"})
        
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    
    return jsonify({"status": "success", "message": f"Đã kích hoạt cào bù ngay lập tức cho {len(incomplete_symbols)} mã bị lỗi!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
