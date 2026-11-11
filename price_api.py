import os
import sys
import time
import json
import base64
import threading
import concurrent.futures
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

import requests
import pandas as pd
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from vnstock import Market, Quote, Finance

# ===================== CONFIG =====================
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
if not MONGO_URI:
    print("❌ LỖI: Thiếu MONGO_URI!", flush=True)
    sys.exit(1)

SSI_API_KEY = os.environ.get("SSI_API_KEY", "").strip()
SSI_API_SECRET = os.environ.get("SSI_API_SECRET", "").strip()
if SSI_API_KEY and SSI_API_SECRET:
    os.environ["SSI_API_KEY"] = SSI_API_KEY
    os.environ["SSI_API_SECRET"] = SSI_API_SECRET

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()

SYMBOLS_JSON_FILE = "vn100_symbols.json"
SCREENER_JSON_FILE = "screener_results.json"
DATA_FILE = "data.json"
LAST_UPDATE_FILE = "last_update.json"
CHEAP_STOCKS_FILE = "cheap_stocks.json"  
HISTORY_DIR = "history"
BCTC_DIR = "bctc_data"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

COOLDOWN_SECONDS = 300
VOLUME_MULTIPLIER = 10

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = "*" if (_raw_origins == "*" or not _raw_origins) else [o.strip() for o in _raw_origins.split(",") if o.strip()]

# ===================== TELEGRAM HELPER =====================
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: requests.post(url, json=payload, timeout=10)
    except Exception: pass

# ===================== DATABASE & AUTO SYNC =====================
print(f"🔌 Đang kết nối MongoDB...", flush=True)
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    db = mongo_client['quant_db']
    collection = db['stocks']
    print("✅ Kết nối MongoDB thành công!", flush=True)
    
    if os.path.exists(SYMBOLS_JSON_FILE):
        try:
            with open(SYMBOLS_JSON_FILE, "r", encoding="utf-8") as f:
                json_symbols = set([str(s).upper().strip() for s in json.load(f)])
            if len(json_symbols) > 0:
                db_symbols = set([doc["symbol"] for doc in collection.find({}, {"symbol": 1})])
                symbols_to_add = json_symbols - db_symbols
                if symbols_to_add:
                    for sym in symbols_to_add: collection.insert_one({"symbol": sym})
                symbols_to_remove = db_symbols - json_symbols
                if symbols_to_remove:
                    collection.delete_many({"symbol": {"$in": list(symbols_to_remove)}})
                print("ℹ️ Đã đồng bộ danh sách mã.", flush=True)
        except Exception as e:
            print(f"⚠️ Lỗi đồng bộ JSON: {e}", flush=True)
except PyMongoError as e:
    print(f"❌ LỖI MONGODB: {e}", flush=True)
    sys.exit(1)

# ===================== APP =====================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

is_crawling = False
LAST_CRAWL_TIME = {}

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

# ===================== VNSTOCK SSI FUNCTIONS =====================
def _fetch_price_ssi(symbol: str) -> Optional[dict]:
    def _do():
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            past_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            market = Market()
            df = market.equity(symbol).ohlcv(start=past_7d, end=today, source="ssi")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {"close": float(latest['close']), "open": float(latest['open']), "high": float(latest['high']), "low": float(latest['low']), "volume": int(latest['volume'])}
            return None
        except Exception: return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try: data = fut.result(timeout=15) 
        except Exception: return None

    if not data or data["close"] <= 0 or data["open"] <= 0: return None
    if data["close"] >= 1000:
        for k in ["close", "open", "high", "low"]:
            if data[k] > 0: data[k] = data[k] / 1000.0
    if 0 < data["volume"] < 100000: data["volume"] = data["volume"] * VOLUME_MULTIPLIER
    return {"close": round(data["close"], 2), "open": round(data["open"], 2), "high": round(data["high"], 2), "low": round(data["low"], 2), "volume": int(data["volume"])}

def _fetch_history_ssi(symbol: str, start_date: str, end_date: str):
    try:
        quote = Quote(source="ssi", symbol=symbol)
        df = quote.history(start=start_date, end=end_date)
        if df is not None and not df.empty: return df
    except Exception: pass
    return None

def _fetch_bctc_ssi(symbol: str):
    try:
        stock_obj = Finance(symbol=symbol, source='SSI')
        df_balance = stock_obj.balance_sheet(period='quarter')
        df_income = stock_obj.income_statement(period='quarter')
        try: df_cashflow = stock_obj.cash_flow(period='quarter')
        except: df_cashflow = None
        try: df_ratio = stock_obj.ratio(period='quarter')
        except: df_ratio = None
        
        reports = {}
        if df_balance is not None and not df_balance.empty: reports['balance_sheet'] = df_balance.to_dict(orient="records")
        if df_income is not None and not df_income.empty: reports['income_statement'] = df_income.to_dict(orient="records")
        if df_cashflow is not None and not df_cashflow.empty: reports['cash_flow'] = df_cashflow.to_dict(orient="records")
        if df_ratio is not None and not df_ratio.empty: reports['ratios'] = df_ratio.to_dict(orient="records")
        if not reports: return None
        return json.loads(json.dumps(reports, default=str))
    except Exception: return None

# ===================== GITHUB BULK UPLOAD =====================
def get_github_file_data(file_path):
    if not all([GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO]): return None, None
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}?ref={GITHUB_BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code == 200: return json.loads(base64.b64decode(res.json()["content"]).decode("utf-8")), res.json()["sha"]
        return None, None
    except Exception: return None, None

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

# ===================== DATA PROCESSING HELPERS =====================
def translate_sector(sector_name):
    mapping = {
        "BANKS": "NGAN_HANG", "FINANCIALS": "CHUNG_KHOAN", "SECURITIES": "CHUNG_KHOAN", "REAL ESTATE": "BAT_DONG_SAN", "REAL_ESTATE": "BAT_DONG_SAN",
        "TECHNOLOGY": "CONG_NGHE_BAN_LE", "RETAIL": "CONG_NGHE_BAN_LE", "UTILITIES": "DIEN_NUOC_HA_TANG", "ENERGY": "DIEN_NUOC_HA_TANG",
        "MATERIALS": "NGUYEN_VAT_LIEU_CHU_KY", "INDUSTRIALS": "NGUYEN_VAT_LIEU_CHU_KY", "CONSUMER STAPLES": "SAN_XUAT_TIEU_DUNG_PHONG_THU",
        "CONSUMER DISCRETIONARY": "SAN_XUAT_TIEU_DUNG_PHONG_THU", "HEALTHCARE": "SAN_XUAT_TIEU_DUNG_PHONG_THU"
    }
    upper_name = str(sector_name).upper()
    for eng, vn in mapping.items():
        if eng in upper_name: return vn
    return "SAN_XUAT_TIEU_DUNG_PHONG_THU"

def aggregate_candles(data, interval):
    if not data or interval == 'D1': return data
    groups = {}
    for bar in data:
        date = datetime.strptime(bar['time'], '%Y-%m-%d')
        if interval == 'W1':
            start_of_week = date - timedelta(days=date.weekday())
            key = start_of_week.strftime('%Y-%m-%d')
        elif interval == 'MN':
            key = date.strftime('%Y-%m-01')
        if key not in groups:
            groups[key] = {"time": key, "open": bar['open'], "high": bar['high'], "low": bar['low'], "close": bar['close'], "volume": bar['volume']}
        else:
            groups[key]['high'] = max(groups[key]['high'], bar['high'])
            groups[key]['low'] = min(groups[key]['low'], bar['low'])
            groups[key]['close'] = bar['close']
            groups[key]['volume'] += bar['volume']
    return sorted(list(groups.values()), key=lambda x: x['time'])

def extract_advanced_metrics_from_bctc(bctc_data):
    if not bctc_data: return {"cfo_latest": 0, "debt_to_equity": 0, "gross_margin": 0, "net_profit_ttm": 0}
    balance_sheet = bctc_data.get("balance_sheet", [])
    cash_flow = bctc_data.get("cash_flow", [])
    income_statement = bctc_data.get("income_statement", [])
    latest_period = balance_sheet[0].get("period") if balance_sheet else (income_statement[0].get("period") if income_statement else None)
    
    latest_cfo = 0
    cfo_ids = ["CF_NET_CASH_FLOWS_FROM_OPERATING_ACTIVITIES", "CF_OPERATE_CASH_FLOW", "CFO", "CF_NET_CASH"]
    if cash_flow:
        for item in cash_flow:
            if latest_period and item.get("period") == latest_period and item.get("id") in cfo_ids:
                latest_cfo = item.get("value", 0); break
        if latest_cfo == 0:
            for item in cash_flow:
                if item.get("id") in cfo_ids: latest_cfo = item.get("value", 0); break

    total_liabilities, equity = 0, 0
    if balance_sheet:
        for item in balance_sheet:
            if latest_period and item.get("period") == latest_period:
                item_id = item.get("id", ""); val = item.get("value", 0)
                if item_id in ["BS_TOTAL_LIABILITIES", "TOTAL_LIABILITIES"]: total_liabilities = val
                elif item_id in ["BS_EQUITY", "EQUITY"]: equity = val
    debt_to_equity = round(total_liabilities / equity, 2) if equity > 0 else 0

    gross_profit, revenue, is_bank_or_sec = 0, 0, False
    if income_statement:
        for item in income_statement:
            if latest_period and item.get("period") == latest_period:
                item_id = item.get("id", ""); val = item.get("value", 0)
                if item_id in ["IS_GROSS_PROFIT", "GROSS_PROFIT"]: gross_profit = val
                elif item_id in ["IS_NET_REVENUE", "REVENUE", "NET_REVENUE", "IS_TOTAL_REVENUE"]: revenue = val
                elif item_id in ["IS_NET_INTEREST_INCOME"]: is_bank_or_sec = True

    gross_margin = 0.0 if is_bank_or_sec else (round((gross_profit / revenue) * 100, 1) if revenue > 0 else 0)
    net_profit_ttm = 0
    pat_ids = ["IS_NET_PROFIT_AFTER_TAX", "NET_PROFIT_AFTER_TAX", "IS_PROFIT_AFTER_TAX"]
    pat_items = [item for item in income_statement if item.get("id") in pat_ids]
    if pat_items:
        pat_items = sorted(pat_items, key=lambda x: x.get("period", ""), reverse=True)
        net_profit_ttm = sum([q.get("value", 0) for q in pat_items[:4]])
    return {"cfo_latest": round(latest_cfo / 1e9, 2), "debt_to_equity": debt_to_equity, "gross_margin": gross_margin, "net_profit_ttm": round(net_profit_ttm / 1e9, 2)}

def parse_bctc_history(bctc_data):
    if not bctc_data: return []
    balance_sheet = bctc_data.get("balance_sheet", [])
    income_statement = bctc_data.get("income_statement", [])
    if not balance_sheet or not income_statement: return []

    bs_dict = {item.get("period"): {} for item in balance_sheet}
    for item in balance_sheet: bs_dict[item.get("period")][item.get("id")] = item.get("value")
    is_dict = {item.get("period"): {} for item in income_statement}
    for item in income_statement: is_dict[item.get("period")][item.get("id")] = item.get("value")

    periods = sorted(list(set(bs_dict.keys()).intersection(set(is_dict.keys()))), reverse=True)
    bctc_records = []
    for idx, curr_p in enumerate(periods):
        if not curr_p or "-" not in curr_p: continue
        prev_p = periods[idx + 1] if idx + 1 < len(periods) else None
        year, q_str = curr_p.split("-")
        yoy_p = f"{int(year) - 1}-{q_str}"

        total_assets = bs_dict[curr_p].get("BS_TOTAL_ASSETS", 0)
        loans = bs_dict[curr_p].get("BS_LOANS_TO_CUSTOMERS", 0)
        equity = bs_dict[curr_p].get("BS_EQUITY", 0)
        nii = is_dict[curr_p].get("IS_NET_INTEREST_INCOME", 0)
        pat = is_dict[curr_p].get("IS_NET_PROFIT_AFTER_TAX", 0)

        loans_prev = bs_dict.get(prev_p, {}).get("BS_LOANS_TO_CUSTOMERS", 0) if prev_p else 0
        loans_yoy = bs_dict.get(yoy_p, {}).get("BS_LOANS_TO_CUSTOMERS", 0)
        credit_growth_qoq = ((loans - loans_prev) / loans_prev * 100) if loans_prev else 0
        credit_growth_yoy = ((loans - loans_yoy) / loans_yoy * 100) if loans_yoy else 0

        pat_prev = is_dict.get(prev_p, {}).get("IS_NET_PROFIT_AFTER_TAX", 0) if prev_p else 0
        pat_yoy = is_dict.get(yoy_p, {}).get("IS_NET_PROFIT_AFTER_TAX", 0)
        pat_growth_qoq = ((pat - pat_prev) / pat_prev * 100) if pat_prev else 0
        pat_growth_yoy = ((pat - pat_yoy) / pat_yoy * 100) if pat_yoy else 0
        roe_q = (pat / equity * 100) if equity else 0

        bctc_records.append({
            "period": curr_p, "total_assets": round(total_assets / 1e9, 2), "loans": round(loans / 1e9, 2),
            "equity": round(equity / 1e9, 2), "net_interest_income": round(nii / 1e9, 2), "net_profit": round(pat / 1e9, 2),
            "credit_growth_qoq": round(credit_growth_qoq, 2), "credit_growth_yoy": round(credit_growth_yoy, 2),
            "profit_growth_qoq": round(pat_growth_qoq, 2), "profit_growth_yoy": round(pat_growth_yoy, 2), "roe_quarter": round(roe_q, 2)
        })
    return bctc_records

def filter_and_save_cheap_stocks(stock_db):
    sector_cheap_groups = {}
    for symbol, info in stock_db.items():
        pe, pb, roe, net_profit = info.get("pe", 0), info.get("pb", 0), info.get("roe", 0), info.get("net_profit_ttm", 0)
        if 0 < pe <= 12 and 0 < pb <= 1.5 and roe >= 8.0 and net_profit > 0:
            sector = translate_sector(info.get("sector", ""))
            info['sector'] = sector
            sector_cheap_groups.setdefault(sector, []).append(info)
    for sector in sector_cheap_groups: sector_cheap_groups[sector] = sorted(sector_cheap_groups[sector], key=lambda x: x['pe'])
    if sector_cheap_groups: commit_multiple_files_to_github({CHEAP_STOCKS_FILE: sector_cheap_groups}, f"Update Cheap Stocks {_get_trading_date_str()}")

# ===================== REAL-TIME CRAWL =====================
def _execute_crawl(symbols: list, is_retry: bool = False, is_manual: bool = False):
    global is_crawling
    if is_crawling: return
    is_crawling = True
    try:
        trading_date = _get_trading_date_str()
        print(f"\n[CRAWL] 🚀 Cào Real-time {len(symbols)} mã...", flush=True)
        for chunk_idx in range(0, len(symbols), 5):
            chunk = symbols[chunk_idx:chunk_idx+5]
            for sym in chunk:
                doc = collection.find_one({"symbol": sym})
                if doc and doc.get("chot_so_today") == trading_date: continue

                data = _fetch_price_ssi(sym)
                if data:
                    hist_data, _ = get_github_file_data(f"history/{sym}.json")
                    prev_close, avg_volume_20 = 0.0, 0.0
                    
                    if hist_data and hist_data.get("history_d1"):
                        bars = hist_data["history_d1"]
                        if len(bars) >= 2: prev_close = bars[-2]["close"] if bars[-1]["time"] == trading_date else bars[-1]["close"]
                        elif len(bars) == 1: prev_close = bars[0]["close"]
                        valid_vols = [b.get("volume", 0) for b in bars[-20:] if b.get("volume", 0) > 0]
                        if valid_vols: avg_volume_20 = sum(valid_vols) / len(valid_vols)

                    amp_pct = round((data["high"] - data["low"]) / data["open"] * 100, 2) if data["open"] > 0 else 0
                    vol_pct = round(data["volume"] / avg_volume_20 * 100, 2) if avg_volume_20 > 0 else 0
                    pct_change = round(((data["close"] - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0

                    collection.update_one({"symbol": sym}, {"$set": {
                        "close": data["close"], "prev_close": prev_close, "open": data["open"], "high": data["high"], "low": data["low"], "volume": data["volume"],
                        "amplitude_percent": amp_pct, "volume_percent": vol_pct, "avg_volume_20": round(avg_volume_20, 2),
                        "trading_date": trading_date, "updated_at": _now_str(), "is_locked": _is_after_15h(), "is_incomplete": False
                    }}, upsert=True)
                    
                    print(f"  ✅ [OK] {sym}: Giá {data['close']:,} | Vol: {vol_pct}% (Lúc: {_now_str()})", flush=True)
                    
                    if _is_after_15h() and hist_data is not None:
                        d1_bars = hist_data.get("history_d1", [])
                        d1_bar = {"time": trading_date, "open": data["open"], "high": data["high"], "low": data["low"], "close": data["close"], "volume": data["volume"]}
                        d1_idx = next((i for i, b in enumerate(d1_bars) if b["time"] == trading_date), -1)
                        if d1_idx == -1: d1_bars.append(d1_bar)
                        else: d1_bars[d1_idx] = d1_bar
                        hist_data["history_d1"] = d1_bars
                        commit_multiple_files_to_github({f"history/{sym}.json": hist_data}, f"Update {sym}")
                else:
                    print(f"  ❌ [LỖI] {sym}", flush=True)
            time.sleep(1)
    finally:
        is_crawling = False

# ===================== STATIC DATA CRAWL (16H00) - TỐI ƯU INCREMENTAL =====================
def full_data_crawler_worker():
    print("⏰ [FULL-DATA] Đã bật luồng cào dữ liệu tĩnh (16h00).", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            if vn_now.hour == 16 and vn_now.minute == 0:
                print("\n[FULL-DATA] 🚀 Bắt đầu cập nhật dữ liệu tĩnh (Incremental Update)...", flush=True)
                target_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
                
                stock_db = {}
                if os.path.exists(DATA_FILE):
                    try:
                        with open(DATA_FILE, "r", encoding="utf-8") as f: stock_db = json.load(f)
                    except: pass

                end_date = datetime.now().strftime("%Y-%m-%d")
                # Chỉ lấy 30 ngày gần nhất để cập nhật
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                
                for i, symbol in enumerate(target_symbols):
                    print(f"  [{i+1}/{len(target_symbols)}] Xử lý tĩnh: {symbol}...", flush=True)
                    
                    # 1. CÀO BCTC & MERGE (Chỉ thêm quý mới)
                    new_bctc = _fetch_bctc_ssi(symbol)
                    old_bctc, _ = get_github_file_data(f"bctc_data/{symbol}.json")
                    
                    if new_bctc:
                        if old_bctc and "balance_sheet" in old_bctc:
                            # Gộp bảng cân đối kế toán
                            old_bs = {item['period']: item for item in old_bctc.get('balance_sheet', []) if 'period' in item}
                            for item in new_bctc.get('balance_sheet', []):
                                if 'period' in item: old_bs[item['period']] = item
                            new_bctc['balance_sheet'] = list(old_bs.values())
                            
                            # Gộp báo cáo kết quả kinh doanh
                            old_is = {item['period']: item for item in old_bctc.get('income_statement', []) if 'period' in item}
                            for item in new_bctc.get('income_statement', []):
                                if 'period' in item: old_is[item['period']] = item
                            new_bctc['income_statement'] = list(old_is.values())
                            
                            # Gộp lưu chuyển tiền tệ
                            if 'cash_flow' in new_bctc:
                                old_cf = {item['period']: item for item in old_bctc.get('cash_flow', []) if 'period' in item}
                                for item in new_bctc.get('cash_flow', []):
                                    if 'period' in item: old_cf[item['period']] = item
                                new_bctc['cash_flow'] = list(old_cf.values())
                                
                        new_bctc['updated_at'] = _now_str()
                        commit_multiple_files_to_github({f"bctc_data/{symbol}.json": new_bctc}, f"BCTC {symbol}")
                        bctc_data = new_bctc
                    else:
                        bctc_data = old_bctc
                    
                    # 2. CÀO LỊCH SỬ GIÁ 30 NGÀY & MERGE
                    df = _fetch_history_ssi(symbol, start_date, end_date)
                    if df is not None and not df.empty:
                        new_bars = []
                        for _, row in df.iterrows():
                            time_val = row['time']
                            time_str = time_val.strftime('%Y-%m-%d') if hasattr(time_val, 'strftime') else str(time_val)[:10]
                            new_bars.append({
                                "time": time_str, "open": float(row.get('open', row['close'])), 
                                "high": float(row.get('high', row['close'])),
                                "low": float(row.get('low', row['close'])), 
                                "close": float(row['close']), "volume": int(row.get('volume', 0))
                            })
                        
                        old_hist, _ = get_github_file_data(f"history/{symbol}.json")
                        old_d1 = old_hist.get("history_d1", []) if old_hist else []
                        
                        # Gộp nến cũ và nến mới (Dùng dict để ghi đè nến trùng ngày)
                        merged_dict = {bar['time']: bar for bar in old_d1}
                        for bar in new_bars: merged_dict[bar['time']] = bar
                        history_d1 = sorted(list(merged_dict.values()), key=lambda x: x['time'])
                        
                        hist_file = {
                            "history_d1": history_d1, 
                            "history_w1": aggregate_candles(history_d1, 'W1'), 
                            "history_mn": aggregate_candles(history_d1, 'MN')
                        }
                        commit_multiple_files_to_github({f"history/{symbol}.json": hist_file}, f"History {symbol}")
                        
                        adv_metrics = extract_advanced_metrics_from_bctc(bctc_data)
                        
                        stock_db[symbol] = {
                            "symbol": symbol, "company_name": symbol, "market_cap": 0, "total_shares": 0,
                            "close": int(df['close'].iloc[-1] * 1000), "volume": int(df['volume'].iloc[-1]),
                            "pe": 0, "pb": 0, "ps": 0, "eps": 0, "roe": 0, "roa": 0, "dividend_yield": 0,
                            "cfo_latest": adv_metrics["cfo_latest"], "debt_to_equity": adv_metrics["debt_to_equity"],
                            "gross_margin": adv_metrics["gross_margin"], "net_profit_ttm": adv_metrics["net_profit_ttm"],
                            "recommendation": "Tích lũy", "price_zone": "—", "updated_at": _now_str(),
                            "bctc_history": parse_bctc_history(bctc_data),
                        }
                    time.sleep(1.5)
                
                if stock_db:
                    commit_multiple_files_to_github({DATA_FILE: stock_db, LAST_UPDATE_FILE: {"last_update": _now_str()}}, "Update Data JSON")
                    filter_and_save_cheap_stocks(stock_db)
                
                print(f"[FULL-DATA] ✅ Đã cập nhật xong dữ liệu tĩnh (Incremental).", flush=True)
                time.sleep(120) 
            time.sleep(30)
        except Exception as e:
            print(f"❌ [FULL-DATA] Lỗi: {e}", flush=True)
            time.sleep(60)

# ===================== BACKGROUND WORKER =====================
def auto_crawler_worker():
    print("⏰ [AUTO] Đã bật chế độ cào cuốn chiếu Real-time!", flush=True)
    while True:
        try:
            vn_now = datetime.utcnow() + timedelta(hours=7)
            today_str = vn_now.strftime("%Y-%m-%d")
            is_after_15h = vn_now.hour >= 15

            collection.update_many({"chot_so_today": {"$ne": today_str}}, {"$unset": {"chot_so_today": "", "is_locked": ""}})

            all_symbols = [doc["symbol"] for doc in collection.find({}, {"symbol": 1})]
            total_symbols = len(all_symbols)
            crawled_count = collection.count_documents({"chot_so_today": today_str})

            if is_after_15h and crawled_count >= total_symbols:
                tomorrow = vn_now + timedelta(days=1)
                wake_up_time = tomorrow.replace(hour=8, minute=55, second=0, microsecond=0)
                sleep_seconds = (wake_up_time - vn_now).total_seconds()
                if sleep_seconds > 0:
                    print(f"🌙 [AUTO] Đã chốt sổ xong {crawled_count}/{total_symbols}. Ngủ đông đến {wake_up_time.strftime('%H:%M %d/%m')}...", flush=True)
                    time.sleep(min(sleep_seconds, 3600)) 
                    continue

            if not is_crawling:
                symbols_to_crawl = []
                for sym in all_symbols:
                    doc = collection.find_one({"symbol": sym}, {"chot_so_today": 1})
                    if not doc or doc.get("chot_so_today") != today_str: symbols_to_crawl.append(sym)
                if not symbols_to_crawl: symbols_to_crawl = all_symbols 
                _execute_crawl(symbols_to_crawl, is_manual=False)
            time.sleep(15)
        except Exception as e:
            print(f"❌ [AUTO] Lỗi: {e}", flush=True)
            time.sleep(10)

threading.Thread(target=auto_crawler_worker, daemon=True).start()
threading.Thread(target=full_data_crawler_worker, daemon=True).start()

# ===================== ROUTES =====================
@app.get("/")
def home(): return "API is running!", 200

@app.get("/api/status")
def api_status():
    return jsonify({"status": "success", "is_crawling": is_crawling, "message": "Đang cào real-time" if is_crawling else "Sẵn sàng"})

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

                pct, avail, arrow, color = 0.0, False, "—", "slate"
                if close_price > 0 and prev_close > 0:
                    raw_pct = ((close_price - prev_close) / prev_close) * 100
                    if -30.0 <= raw_pct <= 30.0:
                        pct = round(raw_pct, 2); avail = True
                        if pct > 0: arrow, color = "▲", "emerald"
                        elif pct < 0: arrow, color = "▼", "rose"
                
                doc["price_change_percent"] = pct
                doc["price_change_available"] = avail
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
        updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) for sym in symbols if collection.find_one({"symbol": sym})}
        return jsonify({"status": "success", "updated_at": _now_str() + " (Đang cào ngầm)", "updated_data": updated_data})

    threading.Thread(target=_execute_crawl, args=(symbols, False, True)).start()
    updated_data = {sym: collection.find_one({"symbol": sym}).get("close", 0) for sym in symbols if collection.find_one({"symbol": sym})}
    return jsonify({"status": "success", "updated_at": _now_str() + " (Bắt đầu cào)", "updated_data": updated_data})

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
    return jsonify({"status": "success", "message": f"Đang ép cào lại {len(all_symbols)} mã."})

@app.get("/api/retry-now")
def retry_now():
    if is_crawling: return jsonify({"status": "error", "message": "Đang chạy!"})
    incomplete_symbols = [doc["symbol"] for doc in collection.find({"is_incomplete": True}, {"symbol": 1})]
    if not incomplete_symbols: return jsonify({"status": "success", "message": "Không có mã lỗi!"})
    threading.Thread(target=_execute_crawl, args=(incomplete_symbols, True, True)).start()
    return jsonify({"status": "success", "message": f"Đang cào bù {len(incomplete_symbols)} mã!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
