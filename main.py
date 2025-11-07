# main.py
import os
import json
import csv
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, List

import redis  # pip install redis

API_URL = "https://tradelinks.munirkhanani.com/api_new/getfeedbywatchtype"

# --- Config via ENV ---
BOT_TOKEN   = os.environ["BOT_TOKEN"]            # e.g. 8142878600:xxxxx
CHAT_ID     = int(os.environ["CHAT_ID"])         # e.g. 5884031542
CSV_URL     = os.environ["CSV_URL"]              # raw CSV URL (GitHub raw or any HTTPS)
COOKIE      = os.environ["COOKIE"]               # full cookie string
COOLDOWN_MIN = int(os.environ.get("COOLDOWN_MINUTES", "30"))  # default 30
WATCH_CODES = list(range(1, 11))                 # same as your local script
REDIS_URL   = os.environ["REDIS_URL"]            # Upstash Redis URL (rediss://... with token)
# ----------------------

rconn = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def fetch_thresholds_csv(url: str) -> Dict[str, Dict[str, Optional[float]]]:
    """
    CSV header EXACTLY:
    SYMBOL,BUY,SELL,SLHIT
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    text = resp.text.splitlines()
    reader = csv.DictReader(text)
    expected = ["SYMBOL","BUY","SELL","SLHIT"]
    if [h.strip().upper() for h in reader.fieldnames or []] != expected:
        raise ValueError(f"CSV header must be: {','.join(expected)}")

    thresholds: Dict[str, Dict[str, Optional[float]]] = {}
    for row in reader:
        sym = row["SYMBOL"].strip().upper()
        def to_num(x: str) -> Optional[float]:
            x = (x or "").strip()
            if x in ("", "NA", "N/A", "-", "null", "NULL"):
                return None
            try:
                return float(x)
            except:
                return None
        thresholds[sym] = {
            "BUY":   to_num(row["BUY"]),
            "SELL":  to_num(row["SELL"]),
            "SLHIT": to_num(row["SLHIT"]),
        }
    return thresholds

def fetch_code(cookie: str, code: int) -> List[dict]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    body = f"type=I&code={code}"
    try:
        r = requests.post(API_URL, headers=headers, data=body, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data.get("aData", []) or []
    except Exception as e:
        print(f"[WARN] code {code} fetch failed: {e}")
        return []

def fetch_all_symbols(cookie: str) -> Dict[str, dict]:
    all_rows: List[dict] = []
    for c in WATCH_CODES:
        all_rows.extend(fetch_code(cookie, c))
    by_symbol: Dict[str, dict] = {}
    for row in all_rows:
        sym = str(row.get("SYMBOL_CODE","")).strip().upper()
        if not sym:
            continue
        if sym in by_symbol:
            continue
        by_symbol[sym] = row
    return by_symbol

def to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return None

def evaluate_trigger(ltp: Optional[float], thr: Dict[str, Optional[float]]) -> Optional[str]:
    # Priority: SL HIT -> BUY -> SELL
    if ltp is None:
        return None
    sl = thr.get("SLHIT")
    buy = thr.get("BUY")
    sell = thr.get("SELL")
    if sl is not None and ltp <= sl:   return "SL HIT"
    if buy is not None and ltp <= buy: return "BUY"
    if sell is not None and ltp >= sell: return "SELL"
    return None

def dedup_key(symbol: str, trigger: str) -> str:
    return f"psx_alert:{symbol}:{trigger}"

def can_send(symbol: str, trigger: str, cooldown_min: int) -> bool:
    key = dedup_key(symbol, trigger)
    if rconn.exists(key):
        return False
    # No key → allow send; set TTL so future runs suppress repeats
    rconn.set(key, "1", ex=cooldown_min * 60)
    return True

def send_telegram(bot_token: str, chat_id: int, text: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")
        return False

def fmt(x: Optional[float]) -> str:
    return "-" if x is None else f"{x:.2f}"

def format_alert(symbol: str, trigger: str, ltp, low, high, vol) -> str:
    now_local = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return (
        f"⚠️ <b>{trigger}</b>\n"
        f"<b>{symbol}</b>\n"
        f"LTP: {fmt(ltp)} | Low: {fmt(low)} | High: {fmt(high)} | Vol: {fmt(vol)}\n"
        f"<i>{now_local}</i>"
    )

def heartbeat_if_needed(sent_count: int):
    """
    Optional: If you later want a daily 'still alive' message from cloud,
    you can implement the same cron entry at a specific time that calls this script with
    an env flag like HEARTBEAT_ONLY=true. For now, we skip it in this minimal cloud job.
    """
    pass

def main():
    thresholds = fetch_thresholds_csv(CSV_URL)
    records = fetch_all_symbols(COOKIE)

    sent = 0
    for symbol, thr in thresholds.items():
        rec = records.get(symbol)
        if not rec:
            continue
        ltp  = to_float(rec.get("LAST_TRADE_PRICE"))
        low  = to_float(rec.get("LOW_PRICE"))
        high = to_float(rec.get("HIGH_PRICE"))
        vol  = to_float(rec.get("TOTAL_TRADED_VOLUME"))

        trig = evaluate_trigger(ltp, thr)
        if trig is None:
            continue

        if not can_send(symbol, trig, COOLDOWN_MIN):
            continue

        text = format_alert(symbol, trig, ltp, low, high, vol)
        if send_telegram(BOT_TOKEN, CHAT_ID, text):
            sent += 1

    print(f"[INFO] One-shot run complete. Alerts sent: {sent}")

if __name__ == "__main__":
    main()
