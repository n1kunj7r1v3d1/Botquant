"""
MT5 trading bot for XAU/USD with a 50-pip take-profit and 20-pip stop-loss.

- Trades at IST 10:35, 11:35, 12:35, 13:35, 14:35, 16:35, 17:35, 18:35, 19:35, 20:35
  (we convert these IST targets to SERVER TIME automatically each day).
- Direction from the preceding 5-minute candle colour (closed bar via copy_rates_from).
- Lot scaling (QuantTekel):
    * "quanttekel" mode: DD-based sizing with min 0.02 lots, compounding 0.02 per $100 of virtual DD,
      and a hard cap so SL cannot exceed the daily DD ($37.50 here).
- SL: 20 pips (2.0), TP: 50 pips (5.0) on gold (1 pip = 0.10).
- Logs trades to CSV when the position actually closes (TP/SL), then emails the daily log after the LAST trade closes.
- Also emails a weekly log every Saturday and a monthly log on the last day of the month.
- Email reports are sent ONCE using sentinel flags (prevents duplicate emails).
- Designed for demo/prop use only.
"""

import MetaTrader5 as mt5
from datetime import datetime, timedelta, date, time as dt_time, timezone
import time
from pathlib import Path
import csv
import threading
import smtplib, ssl
from email.message import EmailMessage
from typing import Optional, Tuple
import sys
import os
import re  # for robust time parsing

# ========= CONFIG =========

# >>> Fill these with your QuantTekel details <<<
ACCOUNT  = 10271567                      # MT5 account number
PASSWORD = "6Wp8T32@ig"           # MT5 account password
SERVER   = "QuantTekel-Server"           # MT5 server name (exact)
SYMBOL   = "XAUUSD.QTR"                      # Verify exact symbol in Market Watch (e.g., XAUUSD or XAUUSDm)

# IST Trading schedule (we will convert these IST times to server clock daily)
IST_TRADE_TIMES = [
    "10:35","11:35","12:35","13:35","16:20",
    "16:35","17:35","18:35","19:35","20:35"
]

# Strategy params
SL_PIPS = 20
TP_PIPS = 50
PIP_SIZE = 0.10
DEVIATION = 20
MAGIC = 20250901

# Heartbeat status in console (every second)
HEARTBEAT_EVERY_SEC = True
HEARTBEAT_SINGLE_LINE = True

# Email (use app password for Gmail)
EMAIL_SENDER   = "you@example.com"
EMAIL_PASSWORD = "your_app_password"
EMAIL_RECEIVER = "you@example.com"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465

# Logging
LOG_DIR = Path("trade_logs")
LOG_DIR.mkdir(exist_ok=True)

# ===== Lot sizing mode =====
LOT_MODE = "quanttekel"

# QuantTekel facts: Balance $1,250 ; Max DD 3% = $37.50
DAILY_DD_USD = 37.50            # hard cap used in lot sizing (updated by realized P/L if enabled)
USE_DYNAMIC_VB = True           # realized P/L increases/decreases available DD intraday

# (Legacy fields kept for compatibility; not used in quanttekel mode)
BASELINE_BALANCE = 50000.0

# ===== Timezone mapping (IST -> Server) =====
IST_TZ = timezone(timedelta(hours=5, minutes=30))

# If you know the exact server-IST offset, set it here (minutes).
# QuantTekel often runs around UTC+3 vs IST (+180 min); adjust if needed.
AUTO_SERVER_GMT3_MONTHS = {3,4,5,6,7,8,9,10}
MANUAL_SERVER_DELTA_MINUTES: Optional[int] = 180  # set None to auto-estimate by months

# Trigger window tolerance (seconds)
FIRE_WINDOW_SECONDS = 10

# ==========================

def init_mt5():
    if not mt5.initialize():
        raise RuntimeError(f"MT5 init error: {mt5.last_error()}")
    if not mt5.login(ACCOUNT, password=PASSWORD, server=SERVER):
        raise RuntimeError(f"MT5 login error: {mt5.last_error()}")
    info = mt5.symbol_info(SYMBOL)
    if info is None:
        raise RuntimeError(f"{SYMBOL} not found in Market Watch")
    if not info.visible:
        mt5.symbol_select(SYMBOL, True)

def shutdown_mt5():
    mt5.shutdown()

def lot_size_balance(balance: float) -> float:
    steps = max(int(balance // 100), 1)
    return round(steps * 0.02, 2)

def is_last_day_of_month(dt: datetime) -> bool:
    return (dt + timedelta(days=1)).month != dt.month

def send_email(subject: str, body: str, filepath: Optional[Path] = None):
    msg = EmailMessage()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = subject
    msg.set_content(body)
    if filepath is not None and Path(filepath).exists():
        with open(filepath, "rb") as f:
            msg.add_attachment(f.read(), maintype="application", subtype="csv", filename=Path(filepath).name)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
    print(f"[EMAIL] Sent: {subject} -> {EMAIL_RECEIVER} {'(with attachment)' if filepath else '(no attachment)'}")

def combine_logs(output_file: Path, start_date: date, end_date: date):
    header = ["Date","Time","Signal","Volume","Entry","SL","TP","Result","Profit","Balance"]
    rows = []
    d = start_date
    while d <= end_date:
        f = LOG_DIR / f"{d.isoformat()}.csv"
        if f.exists():
            with open(f, newline="") as fp:
                r = csv.reader(fp)
                _ = next(r, None)
                rows.extend([row for row in r])
        d += timedelta(days=1)
    with open(output_file, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(header)
        w.writerows(rows)
    print(f"[LOG] Combined {len(rows)} rows -> {output_file.name}")

def get_candle_color(candle_start: datetime):
    rates = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, candle_start, 1)
    if rates is None or len(rates) == 0:
        return None
    o, c = rates[0]["open"], rates[0]["close"]
    return "Green" if c > o else "Red"

def margin_ok(order_type, volume, price) -> bool:
    try:
        mr = mt5.order_calc_margin(order_type, SYMBOL, volume, price)
        acc = mt5.account_info()
        ok = (mr is not None) and (acc is not None) and (mr <= acc.margin_free)
        if not ok:
            print(f"[MARGIN] Need {mr}, free {acc.margin_free} -> skip")
        return ok
    except Exception as e:
        print(f"[MARGIN] check error: {e}")
        return False

def place_trade(signal: str, volume: float, tag: str):
    tick = mt5.symbol_info_tick(SYMBOL)
    if not tick:
        print("[TRADE] No tick data.")
        return None
    price = tick.ask if signal == "BUY" else tick.bid
    sl_distance = SL_PIPS * PIP_SIZE
    tp_distance = TP_PIPS * PIP_SIZE
    if signal == "BUY":
        sl = price - sl_distance
        tp = price + tp_distance
        order_type = mt5.ORDER_TYPE_BUY
    else:
        sl = price + sl_distance
        tp = price - tp_distance
        order_type = mt5.ORDER_TYPE_SELL
    if not margin_ok(order_type, volume, price):
        return None
    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": float(volume),
        "type": order_type,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": DEVIATION,
        "magic": MAGIC,
        "comment": f"50pip_bot|{tag}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK
    }
    result = mt5.order_send(req)
    print(f"[TRADE] {signal} vol={volume:.2f} price={price:.3f} SL={sl:.3f} TP={tp:.3f} -> ret={result.retcode}, comment={getattr(res,'comment','') if (res:=result) else ''}")
    return result

def log_row_for_day(day: date, row):
    file = LOG_DIR / f"{day.isoformat()}.csv"
    write_header = not file.exists()
    with open(file, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["Date","Time","Signal","Volume","Entry","SL","TP","Result","Profit","Balance"])
        w.writerow(row)

# ====== DAILY REALIZED P/L TRACKER (thread-safe) ======
DAILY_REALIZED_PNL = 0.0
DAILY_REALIZED_LOCK = threading.Lock()

def watcher_for_trade(trade_tag: str, approx_time: datetime, expected_signal: str, volume: float, entry: float, sl: float, tp: float, position_ticket: Optional[int] = None):
    pos_ticket = position_ticket

    if pos_ticket is None:
        deadline = time.time() + 60
        while time.time() < deadline and pos_ticket is None:
            positions = mt5.positions_get(symbol=SYMBOL)
            if positions:
                cand = [p for p in positions if getattr(p, "magic", 0) == MAGIC]
                if len(cand) == 1:
                    pos_ticket = cand[0].ticket
                    break
                for p in positions:
                    try:
                        if p.magic == MAGIC and (trade_tag in getattr(p, "comment", "")):
                            pos_ticket = p.ticket
                            break
                    except Exception:
                        continue
            if pos_ticket:
                break
            time.sleep(0.5)

    if pos_ticket:
        while True:
            positions = mt5.positions_get(symbol=SYMBOL)
            if not any(p.ticket == pos_ticket for p in positions or []):
                break
            time.sleep(0.5)

    start_hist = approx_time - timedelta(hours=6)
    end_hist = datetime.now(timezone.utc).astimezone().replace(tzinfo=None) + timedelta(minutes=10)
    deals = mt5.history_deals_get(start_hist, end_hist)
    profit = 0.0
    result_text = "Closed"
    if deals:
        by_pos = []
        if pos_ticket:
            by_pos = [d for d in deals if getattr(d, "position_id", 0) == pos_ticket and d.symbol == SYMBOL]
        if not by_pos:
            by_pos = [d for d in deals if getattr(d, "comment", "").find(trade_tag) >= 0 and d.symbol == SYMBOL]
        if by_pos:
            by_pos.sort(key=lambda d: d.time)
            profit = sum(float(d.profit) for d in by_pos)
            last = by_pos[-1]
            dc = getattr(last, "comment", "").lower()
            if "tp" in dc:
                result_text = "TP"
            elif "sl" in dc:
                result_text = "SL"
            else:
                result_text = "WIN" if profit > 0 else "LOSS"

    acc = mt5.account_info()
    balance_after = getattr(acc, "balance", 0.0)

    global DAILY_REALIZED_PNL
    with DAILY_REALIZED_LOCK:
        DAILY_REALIZED_PNL += float(profit)

    open_day = approx_time.date()
    row = [
        open_day.isoformat(),
        approx_time.strftime("%H:%M"),
        expected_signal,
        f"{volume:.2f}",
        f"{entry:.3f}",
        f"{sl:.3f}",
        f"{tp:.3f}",
        result_text,
        f"{profit:.2f}",
        f"{balance_after:.2f}",
    ]
    log_row_for_day(open_day, row)
    print(f"[LOG] {result_text} profit={profit:.2f} logged for {trade_tag}")

# --- One-time send helpers (in-memory + atomic file sentinels) ---
def _sentinel_path(kind: str, key: str) -> Path:
    return LOG_DIR / f"__sent_{kind}_{key}.flag"

def _already_sent(kind: str, key: str) -> bool:
    return _sentinel_path(kind, key).exists()

def _mark_sent_atomic(kind: str, key: str) -> bool:
    p = _sentinel_path(kind, key)
    try:
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(f"sent at {datetime.now(timezone.utc).isoformat()}\n")
        return True
    except FileExistsError:
        return False
    except Exception:
        try:
            if not p.exists():
                with open(p, "w") as f:
                    f.write(f"sent at {datetime.now(timezone.utc).isoformat()}\n")
                return True
        except Exception:
            pass
        return p.exists()

EOD_SENT_DAYS       = set()
WEEKLY_SENT_KEYS    = set()
MONTHLY_SENT_KEYS   = set()

def run_email_end_of_day_if_last_trade_closed(today: date):
    if today in EOD_SENT_DAYS:
        return

    open_positions = mt5.positions_get(symbol=SYMBOL)
    if open_positions and any(getattr(p, "magic", 0) == MAGIC for p in open_positions):
        return

    now = datetime.now()

    # DAILY
    daily_key = today.isoformat()
    daily_file = LOG_DIR / f"{daily_key}.csv"
    if daily_file.exists():
        if not _already_sent("daily", daily_key):
            if _mark_sent_atomic("daily", daily_key):
                send_email(f"Daily Trading Log — {daily_key}", "Attached is today's trading log.", daily_file)
        EOD_SENT_DAYS.add(today)

    # WEEKLY (Saturday)
    if now.weekday() == 5:
        start = (now - timedelta(days=6)).date()
        end = now.date()
        weekly_key = end.isoformat()
        if weekly_key not in WEEKLY_SENT_KEYS:
            if not _already_sent("weekly", weekly_key):
                if _mark_sent_atomic("weekly", weekly_key):
                    weekly_file = LOG_DIR / f"weekly_{weekly_key}.csv"
                    combine_logs(weekly_file, start, end)
                    send_email(f"Weekly Trading Log — week ending {weekly_key}",
                               f"Attached trading log for {start.isoformat()} to {weekly_key}.",
                               weekly_file)
            WEEKLY_SENT_KEYS.add(weekly_key)

    # MONTHLY
    def is_last_day_of_month_local(dt: datetime) -> bool:
        return (dt + timedelta(days=1)).month != dt.month
    if is_last_day_of_month_local(now):
        m = now.month; y = now.year
        monthly_key = f"{y}-{m:02d}"
        if monthly_key not in MONTHLY_SENT_KEYS:
            if not _already_sent("monthly", monthly_key):
                if _mark_sent_atomic("monthly", monthly_key):
                    start_month = date(y, m, 1)
                    end_month = now.date()
                    monthly_file = LOG_DIR / f"monthly_{monthly_key}.csv"
                    combine_logs(monthly_file, start_month, end_month)
                    send_email(f"Monthly Trading Log — {monthly_key}",
                               f"Attached trading log for {monthly_key}.",
                               monthly_file)
            MONTHLY_SENT_KEYS.add(monthly_key)

# ==== Re-anchor helpers & symbol meta ====
def symbol_meta():
    info = mt5.symbol_info(SYMBOL)
    if info is None:
        raise RuntimeError(f"symbol_info failed for {SYMBOL}")
    stops_level = getattr(info, "trade_stops_level", 0)
    return info, info.point, info.digits, stops_level

def normalize_price(price: float) -> float:
    info, point, digits, _ = symbol_meta()
    return round(price, digits)

def compute_sl_tp_from(entry: float, side: str):
    sl_dist = SL_PIPS * PIP_SIZE
    tp_dist = TP_PIPS * PIP_SIZE
    if side == "BUY":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist
    return normalize_price(sl), normalize_price(tp)

def enforce_min_distance(entry: float, sl: float, tp: float, side: str):
    info, point, digits, stops_points = symbol_meta()
    min_dist = stops_points * point if stops_points else 0.0
    if min_dist <= 0:
        return sl, tp
    if side == "BUY":
        if entry - sl < min_dist:
            sl = normalize_price(entry - min_dist)
        if tp - entry < min_dist:
            tp = normalize_price(entry + min_dist)
    else:
        if sl - entry < min_dist:
            sl = normalize_price(entry + min_dist)
        if entry - tp < min_dist:
            tp = normalize_price(entry - min_dist)
    return sl, tp

def get_position_ticket_from_deal(deal_ticket: int, opened_at: datetime) -> Optional[int]:
    if not deal_ticket:
        return None
    start = opened_at - timedelta(minutes=30)
    end   = opened_at + timedelta(minutes=30)
    deals = mt5.history_deals_get(start, end)
    if not deals:
        return None
    for d in deals:
        if getattr(d, "ticket", 0) == deal_ticket:
            pid = int(getattr(d, "position_id", 0))
            return pid or None
    return None

def reanchor_sl_tp_by_position(position_ticket: int, side: str):
    if not position_ticket:
        return
    pos_list = mt5.positions_get(symbol=SYMBOL)
    pos = None
    if pos_list:
        for p in pos_list:
            if int(p.ticket) == int(position_ticket):
                pos = p
                break
    if not pos:
        print(f"[REANCHOR] Position {position_ticket} not found (skip).")
        return
    entry = float(pos.price_open)
    new_sl, new_tp = compute_sl_tp_from(entry, side)
    new_sl, new_tp = enforce_min_distance(entry, new_sl, new_tp, side)
    info, point, digits, _ = symbol_meta()
    diff_sl = abs((pos.sl or 0.0) - new_sl)
    diff_tp = abs((pos.tp or 0.0) - new_tp)
    if diff_sl < point and diff_tp < point:
        print(f"[REANCHOR] No change needed (SL/TP already aligned).")
        return
    req = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   SYMBOL,
        "position": int(position_ticket),
        "sl":       float(new_sl),
        "tp":       float(new_tp),
        "magic":    MAGIC,
        "comment":  f"reanchor|{position_ticket}",
    }
    res = mt5.order_send(req)
    print(f"[REANCHOR] Modify SL/TP -> ret={res.retcode}, sl={new_sl:.3f}, tp={new_tp:.3f}")

# ==== Lot sizing (QuantTekel DD-based) ====
def dollars_per_1usd_move_for_1lot() -> float:
    tick = mt5.symbol_info_tick(SYMBOL)
    if not tick:
        return 100.0
    ref = tick.bid or tick.ask or 0.0
    try:
        pr = mt5.order_calc_profit(mt5.ORDER_TYPE_BUY, SYMBOL, 1.0, ref, ref + 1.0)
        if pr is None:
            return 100.0
        return abs(float(pr))
    except Exception:
        return 100.0

def lot_size_quanntekel() -> float:
    """
    DD-based sizing:
    - Base 'virtual balance' is today's DD budget (e.g., 37.5 USD), optionally adjusted by realized P/L.
    - Nominal compounding: 0.02 lots per $100 of VB.
    - Hard cap: SL risk in USD may not exceed DAILY_DD_USD.
    - Enforces minimum 0.02 lots.
    """
    vb = DAILY_DD_USD
    if USE_DYNAMIC_VB:
        with DAILY_REALIZED_LOCK:
            vb += float(DAILY_REALIZED_PNL)
    vb = max(vb, 0.0)

    steps = int(vb // 100)       # 0 for < $100, grows every +$100 of available DD
    lots = round(steps * 0.02, 2)

    # risk cap by DD
    per_dollar_1lot = dollars_per_1usd_move_for_1lot()
    sl_usd_move = SL_PIPS * PIP_SIZE
    if per_dollar_1lot > 0:
        max_lots_by_dd = DAILY_DD_USD / (per_dollar_1lot * sl_usd_move)
        if lots > max_lots_by_dd:
            lots = max(0.01, round(max_lots_by_dd - 1e-9, 2))

    # Minimum starting size is 0.02 as requested
    return max(0.02, lots)

# ===== Time conversion & heartbeat helpers =====

def _server_gmt_offset_for_date(d: date) -> int:
    return 3 if d.month in AUTO_SERVER_GMT3_MONTHS else 2

def _ist_to_server_delta_minutes_for_date(d: date) -> int:
    if MANUAL_SERVER_DELTA_MINUTES is not None:
        return MANUAL_SERVER_DELTA_MINUTES
    server_gmt = _server_gmt_offset_for_date(d)
    ist_gmt = 5.5
    return int(round((server_gmt - ist_gmt) * 60))

def _parse_hhmm(hhmm: str):
    """
    Accepts 'HH:MM' or 'HH:MM:SS' (also tolerates trailing spaces/comments).
    Returns (hour, minute) as ints.
    """
    s = str(hhmm).strip()
    s = s.split()[0]  # only the first token
    m = re.match(r'^(\d{1,2}):(\d{1,2})(?::\d{1,2})?$', s)
    if not m:
        raise ValueError(f"Bad time format in IST_TRADE_TIMES: {hhmm!r} (expected HH:MM or HH:MM:SS)")
    h = int(m.group(1)); mnt = int(m.group(2))
    if not (0 <= h < 24 and 0 <= mnt < 60):
        raise ValueError(f"Out-of-range time in IST_TRADE_TIMES: {hhmm!r}")
    return h, mnt

def build_server_schedule_for_day(ist_day: date) -> Tuple[dict, int]:
    delta_min = _ist_to_server_delta_minutes_for_date(ist_day)
    delta = timedelta(minutes=delta_min)
    sched = {}
    for tstr in IST_TRADE_TIMES:
        hh, mm = _parse_hhmm(tstr)
        ist_dt = datetime.combine(ist_day, dt_time(hh, mm, tzinfo=IST_TZ))
        server_dt = (ist_dt + delta).replace(tzinfo=None)  # naive server time
        sched[f"{hh:02d}:{mm:02d}"] = server_dt  # normalized key
    return sched, delta_min

def _next_slot_info_server(now_server: datetime, today_server_sched: dict, executed_ist: set) -> Tuple[Optional[str], Optional[int]]:
    for ist_hhmm, server_dt in today_server_sched.items():
        if ist_hhmm in executed_ist:
            continue
        if now_server <= server_dt:
            return ist_hhmm, int((server_dt - now_server).total_seconds())
    return None, None

def _measured_delta_minutes(now_server: datetime, now_ist: datetime) -> int:
    s = datetime(2000,1,1, now_server.hour, now_server.minute, now_server.second)
    i = datetime(2000,1,1, now_ist.hour,   now_ist.minute,   now_ist.second)
    return int(round((s - i).total_seconds() / 60.0))

def _heartbeat_line(now_server: datetime, today_server_sched: dict, executed_ist: set, configured_delta_min: int) -> str:
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST_TZ)
    next_ist, secs = _next_slot_info_server(now_server, today_server_sched, executed_ist)
    fired = len(executed_ist)
    total = len(IST_TRADE_TIMES)
    meas_delta = _measured_delta_minutes(now_server, now_ist)
    sign = "+" if meas_delta >= 0 else "-"
    if next_ist is None:
        tail = "no more IST slots today"
    else:
        hh = secs // 3600
        mm = (secs % 3600) // 60
        ss = secs % 60
        server_dt = today_server_sched[next_ist]
        tail = f"next IST {next_ist} -> fires at server {server_dt.strftime('%H:%M:%S')} in {hh:02d}:{mm:02d}:{ss:02d}"
    return (f"[HB] server {now_server.strftime('%Y-%m-%d %H:%M:%S')} | IST {now_ist.strftime('%Y-%m-%d %H:%M:%S')} "
            f"| server-IST delta {sign}{abs(meas_delta)}m (cfg {configured_delta_min:+}m) | slots {fired}/{total} | {tail}")

# ==========================

def main():
    init_mt5()
    print("[INIT] MT5 initialized and logged in.")

    executed_ist_today = set()
    current_server_day = None
    today_server_sched = {}
    delta_min = 0
    watcher_threads = []

    try:
        while True:
            tick = mt5.symbol_info_tick(SYMBOL)
            now_server = datetime.fromtimestamp(tick.time) if tick else datetime.now()
            server_day = now_server.date()

            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST_TZ)
            ist_day = now_ist.date()

            if server_day != current_server_day:
                executed_ist_today.clear()
                current_server_day = server_day
                today_server_sched, delta_min = build_server_schedule_for_day(ist_day)
                global DAILY_REALIZED_PNL
                with DAILY_REALIZED_LOCK:
                    DAILY_REALIZED_PNL = 0.0

                print(f"\n--- New (server) day {server_day} / IST day {ist_day} ---")
                print(f"[TZ] Using server-IST delta = {delta_min:+} minutes "
                      f"(auto GMT+{_server_gmt_offset_for_date(ist_day)} unless MANUAL override).")
                for ist_hhmm, sdt in today_server_sched.items():
                    print(f"[SCHEDULE] IST {ist_hhmm} -> server {sdt.strftime('%H:%M')}")

            if HEARTBEAT_EVERY_SEC:
                hb = _heartbeat_line(now_server, today_server_sched, executed_ist_today, delta_min)
                if HEARTBEAT_SINGLE_LINE:
                    print(hb.ljust(130), end="\r", flush=True)
                else:
                    print(hb, flush=True)

            for ist_hhmm, fire_dt_server in list(today_server_sched.items()):
                if ist_hhmm in executed_ist_today:
                    continue
                lag = (now_server - fire_dt_server).total_seconds()
                if abs(lag) <= FIRE_WINDOW_SECONDS:
                    candle_start = (fire_dt_server - timedelta(minutes=5)).replace(second=0, microsecond=0)
                    colour = get_candle_color(candle_start)
                    if not colour:
                        print(f"[SKIP] No candle at {candle_start} for IST slot {ist_hhmm}")
                        executed_ist_today.add(ist_hhmm)
                        continue

                    signal = "BUY" if colour == "Green" else "SELL"
                    acc = mt5.account_info()
                    if LOT_MODE == "quanttekel":
                        vol = lot_size_quanntekel()
                    elif LOT_MODE == "balance":
                        vol = lot_size_balance(acc.balance if acc else 100.0)
                    else:
                        vol = lot_size_quanntekel()  # default to DD-based

                    tag = f"{ist_day.isoformat()}_{ist_hhmm}"
                    print(f"[SIGNAL] IST {ist_hhmm} (server {fire_dt_server.strftime('%H:%M')}) prev={colour} -> {signal}, vol={vol:.2f}")
                    res = place_trade(signal, vol, tag)
                    executed_ist_today.add(ist_hhmm)

                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        pos_ticket = None
                        try:
                            pos_ticket = get_position_ticket_from_deal(int(getattr(res, "deal", 0)), now_server)
                        except Exception:
                            pos_ticket = None
                        if pos_ticket:
                            reanchor_sl_tp_by_position(pos_ticket, signal)
                        else:
                            print("[REANCHOR] Could not resolve position ticket from deal (skip re-anchor).")

                        tick2 = mt5.symbol_info_tick(SYMBOL)
                        price = tick2.ask if signal=="BUY" else tick2.bid
                        sl = price - SL_PIPS*PIP_SIZE if signal=="BUY" else price + SL_PIPS*PIP_SIZE
                        tp = price + TP_PIPS*PIP_SIZE if signal=="BUY" else price - TP_PIPS*PIP_SIZE

                        t_thr = threading.Thread(
                            target=watcher_for_trade,
                            args=(f"50pip_bot|{tag}", fire_dt_server.replace(second=0, microsecond=0), signal, vol, price, sl, tp, pos_ticket),
                            daemon=True
                        )
                        t_thr.start()
                        watcher_threads.append(t_thr)
                    else:
                        print(f"[TRADE] Order not executed (ret={getattr(res,'retcode',None)})")

                elif lag > FIRE_WINDOW_SECONDS:
                    executed_ist_today.add(ist_hhmm)

            if len(executed_ist_today) == len(IST_TRADE_TIMES):
                run_email_end_of_day_if_last_trade_closed(server_day)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nBot interrupted by user.")
    finally:
        if HEARTBEAT_EVERY_SEC and HEARTBEAT_SINGLE_LINE:
            print()
        for t in watcher_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        shutdown_mt5()
        print("[SHUTDOWN] MT5 closed.")

if __name__ == "__main__":
    main()
