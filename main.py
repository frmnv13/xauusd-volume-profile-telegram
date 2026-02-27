import datetime
import pytz
import requests
import sys
import os
import zmq
import json
import pandas as pd
from config import (
    SESSION_HOURS, BROKER_TIMEZONE, TELEGRAM_BOT_TOKEN, CHAT_ID, ZMQ_PULL_PORT, ZMQ_PUB_PORT, SYMBOL, CSV_FILE_PATH
)
from core_logic import VolumeProfileEngine

SL_POINTS = 10.0  
TP_POINTS = 10.0  
BACKTEST_START_DATE = "2019-01-01" 
TRADE_EXPIRY_HOUR = 21 

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def send_telegram_message(message):
    """Mengirim pesan ke Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload)
        return response.status_code == 200
    except Exception as e:
        log(f"Error Telegram: {e}")
        return False

def get_session(dt):
    """
    Determines the session for a given datetime (MT4 server time).
    Priority: US > LONDON > ASIA.
    Interval logic: start_hour <= hour < end_hour.
    """
    hour = dt.hour
    for name, start, end in SESSION_HOURS:
        if start <= hour < end:
            return name
    return None

def analyze_csv_file(file_path):
    """Membaca CSV dan menghitung POC per sesi."""
    if not os.path.exists(file_path):
        log(f"File CSV tidak ditemukan: {file_path}")
        return

    try:
        log(f"Membaca data dari {file_path}...")
        
        df_peek = pd.read_csv(file_path, nrows=1)
        peek_cols = [str(c).lower().strip() for c in df_peek.columns]
        
        if 'high' not in peek_cols and 'close' not in peek_cols:
            log("Assuming MT4 History Center format (No Header, Date/Time split).")
            df = pd.read_csv(file_path, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
        else:
            df = pd.read_csv(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]

        if 'date' in df.columns and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        else:
            df['time'] = pd.to_datetime(df['time'])
        
        vol_col = 'volume' if 'volume' in df.columns else 'tick_volume'
        if vol_col not in df.columns:
             log("CSV Error: Column 'volume' missing.")
             return

        now_utc = datetime.datetime.now(pytz.utc)
        now_broker = now_utc.replace(tzinfo=None) + datetime.timedelta(hours=BROKER_TIMEZONE)
        today_date = now_broker.date()
        
        start_date = today_date - datetime.timedelta(days=5)

        df = df[df['time'].dt.date >= start_date].sort_values('time').reset_index(drop=True)
        
        log(f"Data dimuat: {len(df)} baris. Memulai perhitungan Volume Profile...")
        
        engine = VolumeProfileEngine()
        results = []
        
        current_session_key = None
        
        for index, row in df.iterrows():
            timestamp = row['time']
            
            session_name = get_session(timestamp)
            
            if session_name is None:
                if current_session_key is not None:
                    poc, vol = engine.get_poc()
                    results.append({
                        'date': current_session_key[0],
                        'session': current_session_key[1],
                        'poc': poc,
                        'volume': vol
                    })
                    engine.reset()
                    current_session_key = None
                continue

            this_date = timestamp.date()
            
            if current_session_key is None:
                current_session_key = (this_date, session_name)
            elif current_session_key[1] != session_name:
                poc, vol = engine.get_poc()
                results.append({
                    'date': current_session_key[0],
                    'session': current_session_key[1],
                    'poc': poc,
                    'volume': vol
                })
                engine.reset()
                current_session_key = (this_date, session_name)
            elif index > 0 and (timestamp - df.iloc[index-1]['time']).total_seconds() > 3600 * 4:
                poc, vol = engine.get_poc()
                results.append({
                    'date': current_session_key[0],
                    'session': current_session_key[1],
                    'poc': poc,
                    'volume': vol
                })
                engine.reset()
                current_session_key = (this_date, session_name)

            engine.process_candle(row['high'], row['low'], row[vol_col])
        
        if current_session_key is not None:
            poc, vol = engine.get_poc()
            results.append({
                'date': current_session_key[0],
                'session': current_session_key[1],
                'poc': poc,
                'volume': vol
            })
        
        current_session = get_session(now_broker)
        
        unique_dates = sorted(list(set(r['date'] for r in results)))

        print("\n" + "="*65)
        print(f"FILE: {file_path} | {now_broker.strftime('%Y-%m-%d %H:%M')} (Broker Time)")
        print("="*65)
        
        historical_dates = [d for d in unique_dates if d < today_date]
        if historical_dates:
            print(f"{'TANGGAL':<12} | {'SESI':<10} | {'POC PRICE':<12} | {'VOLUME':<10}")
            print("-" * 65)
            for d in historical_dates:
                day_results = [r for r in results if r['date'] == d]
                for r in day_results:
                    print(f"{str(r['date']):<12} | {r['session']:<10} | {r['poc']:<12.2f} | {r['volume']:<10.0f}")
                print("-" * 65)
        
        print(f"\n[DATA HARI INI - {today_date}]")
        
        results_today = [r for r in results if r['date'] == today_date]
        finished_sessions_today = [r for r in results_today if r['session'] != current_session]
        
        if finished_sessions_today:
            print(f"{'SESI':<10} | {'POC PRICE':<12} | {'VOLUME':<10}")
            print("-" * 45)
            for r in finished_sessions_today:
                print(f"{r['session']:<10} | {r['poc']:<12.2f} | {r['volume']:<10.0f}")
            
            last = finished_sessions_today[-1]
            print("-" * 45)
            print(f"ACUAN ENTRY (POC {last['session']}): {last['poc']:.2f}")
        else:
            print(f"Sesi '{current_session}' sedang berlangsung / belum selesai.")
            print(">> Gunakan data POC Kemarin sebagai referensi sementara.")
            
        print("="*65 + "\n")

    except Exception as e:
        log(f"Analisis Selesai: {e}")

def run_backtest(file_path):
    """Backtest strategi Asia POC dari tahun 2023."""
    if not os.path.exists(file_path):
        log(f"File CSV tidak ditemukan: {file_path}")
        return

    try:
        df_peek = pd.read_csv(file_path, nrows=1)
        if 'high' not in [str(c).lower() for c in df_peek.columns]:
            df = pd.read_csv(file_path, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
            df['time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        else:
            df = pd.read_csv(file_path)
            df.columns = [str(c).lower().strip() for c in df.columns]
            df['time'] = pd.to_datetime(df['time'])

        df = df[df['time'] >= BACKTEST_START_DATE].sort_values('time').reset_index(drop=True)
        vol_col = 'volume' if 'volume' in df.columns else 'tick_volume'

        engine = VolumeProfileEngine()
        trades = []
        days_processed = 0
        
        log(f"Memulai Backtest pada {len(df)} baris data...")
        
        asia_conf = next(s for s in SESSION_HOURS if s[0] == "ASIA")
        asia_start, asia_end = asia_conf[1], asia_conf[2]

        for date, day_data in df.groupby(df['time'].dt.date):
            asia_data = day_data[(day_data['time'].dt.hour >= asia_start) & (day_data['time'].dt.hour < asia_end)].copy()
            if len(asia_data) < 4: continue 
            days_processed += 1
            engine.reset()
            for _, row in asia_data.iterrows():
                engine.process_candle(row['high'], row['low'], row[vol_col])
            
            poc, _ = engine.get_poc()
            
            asia_close = asia_data.iloc[-1]['close']
            order_type = "BUY_LIMIT" if asia_close > poc else "SELL_LIMIT"

            entry_price = poc
            sl = entry_price - SL_POINTS if order_type == "BUY_LIMIT" else entry_price + SL_POINTS
            tp = entry_price + TP_POINTS if order_type == "BUY_LIMIT" else entry_price - TP_POINTS
            
            future_data = day_data[day_data['time'].dt.hour >= asia_end]
            
            is_filled = False

            for _, f_row in future_data.iterrows():
                if f_row['time'].hour >= TRADE_EXPIRY_HOUR:
                    if is_filled:
                        pnl = f_row['close'] - entry_price if order_type == "BUY_LIMIT" else entry_price - f_row['close']
                        trades.append({'date': date, 'result': 'EXIT_EOD', 'points': pnl})
                    break

                if not is_filled:
                    if f_row['low'] <= entry_price <= f_row['high']:
                        is_filled = True
                
                if is_filled:
                    if order_type == "BUY_LIMIT":
                        if f_row['low'] <= sl:
                            trades.append({'date': date, 'result': 'SL', 'points': -SL_POINTS})
                            break
                        if f_row['high'] >= tp:
                            trades.append({'date': date, 'result': 'TP', 'points': TP_POINTS})
                            break
                    else: # SELL_LIMIT
                        if f_row['high'] >= sl:
                            trades.append({'date': date, 'result': 'SL', 'points': -SL_POINTS})
                            break
                        if f_row['low'] <= tp:
                            trades.append({'date': date, 'result': 'TP', 'points': TP_POINTS})
                            break

        
        if not trades:
            log("Tidak ada trade yang terpicu selama periode backtest.")
            return

        df_trades = pd.DataFrame(trades)
        total_trades = len(df_trades)
        win_rate = (len(df_trades[df_trades['result'] == 'TP']) / total_trades) * 100
        total_points = df_trades['points'].sum()
        
        success_trades = len(df_trades[df_trades['points'] > 0])
        success_rate = (success_trades / total_trades) * 100
        
        df_trades['cum_points'] = df_trades['points'].cumsum()
        max_drawdown = (df_trades['cum_points'].cummax() - df_trades['cum_points']).max()
        gross_profit = df_trades[df_trades['points'] > 0]['points'].sum()
        gross_loss = abs(df_trades[df_trades['points'] < 0]['points'].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        eod_profit = len(df_trades[(df_trades['result'] == 'EXIT_EOD') & (df_trades['points'] > 0)])
        eod_loss = len(df_trades[(df_trades['result'] == 'EXIT_EOD') & (df_trades['points'] <= 0)])
        
        df_trades['month'] = pd.to_datetime(df_trades['date']).dt.to_period('M')
        monthly_perf = df_trades.groupby('month')['points'].sum()

        print("\n" + "="*40)
        print(f"BACKTEST RESULTS: {SYMBOL} (Since {BACKTEST_START_DATE})")
        print("="*40)
        print(f"Total Days Scanned : {days_processed}")
        print(f"Trades Executed    : {total_trades} ({(total_trades/days_processed)*100:.1f}% fill rate)")
        print(f"Win Rate (TP only) : {win_rate:.2f}%")
        print(f"Success Rate (All) : {success_rate:.2f}%")
        print(f"Profit Factor      : {profit_factor:.2f}")
        print(f"Max Drawdown       : {max_drawdown:.2f} points")
        print("-" * 40)
        print(f"Total Net Points   : {total_points:.2f}")
        print(f"Profit Trades      : {len(df_trades[df_trades['result'] == 'TP'])}")
        print(f"EOD Exit Trades    : {len(df_trades[df_trades['result'] == 'EXIT_EOD'])} ({eod_profit} Win / {eod_loss} Loss)")
        print(f"Loss Trades        : {len(df_trades[df_trades['result'] == 'SL'])}")
        print(f"Settings           : SL {SL_POINTS} | TP {TP_POINTS}")
        print("-" * 40)
        print("Monthly Performance:")
        print(monthly_perf)
        print("="*40 + "\n")

    except Exception as e:
        log(f"Error Backtest: {e}")

def send_trade_to_ea(push_socket, order_type, price, sl, tp):
    """
    Mengirim perintah eksekusi ke Expert Advisor.
    Format: TRADE|SYMBOL|TYPE|PRICE|SL|TP
    TYPE: 0=Buy Limit, 1=Sell Limit
    """
    type_int = 0 if "BUY" in order_type else 1
    command = f"TRADE|{SYMBOL}|{type_int}|{price:.2f}|{sl:.2f}|{tp:.2f}"
    push_socket.send_string(command)
    
    # Notifikasi Telegram
    msg = (
        f"🚀 *Auto Trade Executed*\n"
        f"Symbol: {SYMBOL}\n"
        f"Type: `{order_type}`\n"
        f"Entry: `{price:.2f}`\n"
        f"SL: `{sl:.2f}` | TP: `{tp:.2f}`"
    )
    send_telegram_message(msg)
    log(f"Command sent to EA: {command}")

def warmup_engine(engine):
    """Mengisi engine dengan data dari CSV untuk hari yang sedang berjalan (sejak jam 00:00)."""
    if not os.path.exists(CSV_FILE_PATH):
        log(f"Warmup skipped: File {CSV_FILE_PATH} tidak ditemukan.")
        return None

    try:
        df = pd.read_csv(CSV_FILE_PATH, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        
        now_utc = datetime.datetime.now(pytz.utc)
        now_broker = now_utc.replace(tzinfo=None) + datetime.timedelta(hours=BROKER_TIMEZONE)
        today_date = now_broker.date()

        df_today = df[df['time'].dt.date == today_date].copy()
        
        count = 0
        for _, row in df_today.iterrows():
            engine.process_candle(row['high'], row['low'], row['volume'])
            count += 1
        
        if count > 0:
            poc, _ = engine.get_poc()
            log(f"Warmup Berhasil: Memuat {count} bar dari hari ini. POC Awal: {poc:.2f}")
            return today_date
        else:
            log(f"Warmup: Tidak ada data CSV untuk hari ini ({today_date}).")
            return today_date
            
    except Exception as e:
        log(f"Error saat warmup: {e}")
        return None

def send_historical_summary(days_back=4):
    """Mengirim ringkasan POC dari beberapa hari terakhir ke Telegram sebagai referensi."""
    if not os.path.exists(CSV_FILE_PATH):
        return

    try:
        df = pd.read_csv(CSV_FILE_PATH, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        
        now_utc = datetime.datetime.now(pytz.utc)
        now_broker = now_utc.replace(tzinfo=None) + datetime.timedelta(hours=BROKER_TIMEZONE)
        today_date = now_broker.date()
        
        start_date = today_date - datetime.timedelta(days=days_back)
        df_hist = df[(df['time'].dt.date >= start_date) & (df['time'].dt.date < today_date)].copy()
        
        if df_hist.empty:
            return

        hist_engine = VolumeProfileEngine()
        results = []
        current_key = None
        
        for _, row in df_hist.iterrows():
            s_name = get_session(row['time'])
            if s_name is None:
                if current_key:
                    poc, _ = hist_engine.get_poc()
                    results.append({'date': current_key[0], 'session': current_key[1], 'poc': poc})
                    hist_engine.reset()
                    current_key = None
                continue
            
            this_d = row['time'].date()
            if current_key is None:
                current_key = (this_d, s_name)
            elif current_key[1] != s_name or current_key[0] != this_d:
                poc, _ = hist_engine.get_poc()
                results.append({'date': current_key[0], 'session': current_key[1], 'poc': poc})
                hist_engine.reset()
                current_key = (this_d, s_name)
            
            hist_engine.process_candle(row['high'], row['low'], row['volume'])

        msg = f"📜 *Historical POC (Last {days_back} Days)*\n"
        msg += "━━━━━━━━━━━━━━━\n"
        
        current_date = None
        for r in reversed(results): 
            if r['date'] != current_date:
                current_date = r['date']
                msg += f"📅 *{current_date}*\n"
            msg += f"• {r['session']}: `{r['poc']:.2f}`\n"
        
        msg += "━━━━━━━━━━━━━━━"
        send_telegram_message(msg)
        log(f"Historical summary ({days_back} hari) telah dikirim ke Telegram.")
    except Exception as e:
        log(f"Error historical summary: {e}")

def trigger_asia_from_history(push_socket, target_date):
    """Mencari data Asia di CSV untuk tanggal tertentu dan trigger signal jika valid (Catch-up)."""
    if not os.path.exists(CSV_FILE_PATH):
        return False

    try:
        # Load CSV
        df = pd.read_csv(CSV_FILE_PATH, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        
        asia_conf = next(s for s in SESSION_HOURS if s[0] == "ASIA")
        
        asia_data = df[(df['time'].dt.date == target_date) & 
                       (df['time'].dt.hour >= asia_conf[1]) & 
                       (df['time'].dt.hour < asia_conf[2])]
        
        if asia_data.empty or len(asia_data) < 4:
            return False

        temp_engine = VolumeProfileEngine()
        for _, row in asia_data.iterrows():
            temp_engine.process_candle(row['high'], row['low'], row['volume'])
        
        poc, vol = temp_engine.get_poc()
        last_close = asia_data.iloc[-1]['close']
        
        order_type = "BUY_LIMIT" if last_close > poc else "SELL_LIMIT"
        sl = poc - SL_POINTS if order_type == "BUY_LIMIT" else poc + SL_POINTS
        tp = poc + TP_POINTS if order_type == "BUY_LIMIT" else poc - TP_POINTS
        
        send_trade_to_ea(push_socket, order_type, poc, sl, tp)
        log(f"Catch-up ASIA: Signal dikirim berdasarkan data historis untuk {target_date}")
        return True

    except Exception as e:
        log(f"Error in catch-up ASIA: {e}")
        return False

def start_live_receiver():
    """Menjalankan receiver ZeroMQ untuk data live dari EA."""
    context = zmq.Context()
    
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://localhost:{ZMQ_PUB_PORT}")
    topic_filter = f"{SYMBOL}_M60"
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
    
    push_socket = context.socket(zmq.PUSH)
    push_socket.connect(f"tcp://localhost:{ZMQ_PULL_PORT}")
    
    push_socket.send_string(f"TRACK_RATES|{SYMBOL}|60")
    
    engine = VolumeProfileEngine()
    
    current_date = warmup_engine(engine)

    if current_date is not None:
        send_historical_summary(days_back=4)
    
    log(f"Menghubungkan ke EA... Subscribed ke {topic_filter}")
    
    while True:
        try:
            message = sub_socket.recv_string()
            
            if ":|:" not in message:
                continue
                
            _, payload = message.split(":|:")
            parts = payload.split(";")
            
            if len(parts) < 6:
                continue

            timestamp = pd.to_datetime(int(parts[0]), unit='s') + datetime.timedelta(hours=BROKER_TIMEZONE)
            this_date = timestamp.date()

            if current_date is not None and this_date != current_date:
                log(f"Pergantian hari ({current_date} -> {this_date}). Resetting engine.")
                engine.reset()
            
            current_date = this_date

            if timestamp.hour == TRADE_EXPIRY_HOUR and timestamp.minute == 0:
                push_socket.send_string(f"CANCEL|{SYMBOL}")
                log(f"Sent CANCEL/CLOSE command to EA for {SYMBOL} (Expiry Hour)")
            
            try:
                high = float(parts[2])
                low = float(parts[3])
                volume = float(parts[5])
                engine.process_candle(high, low, volume)

                poc, vol = engine.get_poc()
                
                msg = (
                    f"🕒 *Hourly POC Update ({SYMBOL})*\n"
                    f"Candle Time: `{timestamp.strftime('%H:%M')}`\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"📍 *Daily POC:* `{poc:.2f}`\n"
                    f"💎 *Total Vol:* `{vol:.0f}`\n"
                    f"━━━━━━━━━━━━━━━"
                )
                send_telegram_message(msg)
                log(f"Live Data: POC {poc:.2f} | Vol: {vol:.0f}")
            except (ValueError, IndexError) as e:
                log(f"Error parsing candle data: {e}")

        except Exception as e:
            log(f"Error Live Receiver: {e}")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "LIVE"

    if mode.upper() == "LIVE":
        start_live_receiver()
    elif mode.upper() == "BACKTEST":
        run_backtest(CSV_FILE_PATH)
    else:
        csv_files = [f for f in os.listdir('.') if f.lower().endswith('.csv')]
        
        if not csv_files:
            log("Tidak ada file CSV ditemukan di folder ini.")
        else:
            for f in csv_files:
                analyze_csv_file(f)