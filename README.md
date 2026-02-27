new file
config.py
# Konfigurasi Telegram
TELEGRAM_BOT_TOKEN = "xxxxxxxx"
CHAT_ID = "xxxxxxxxx"
ZMQ_PULL_PORT = 32768 # Port EA menerima command (Python PUSH)
ZMQ_PUB_PORT = 32770  # Port EA mengirim data (Python SUB)

# Konfigurasi Market
BROKER_TIMEZONE = 2
SYMBOL = 'XAUUSD'
PRICE_STEP = 0.10
CSV_FILE_PATH = "XAUUSD60.csv"

# Konfigurasi Waktu
IS_WINTER = True  # Januari = non-DST

if IS_WINTER:
    SESSION_HOURS = [
        ("ASIA", 1, 10),    # 01:00 - 09:59 MT4 (Candle 09:00 masuk ASIA)
    ]
else:
    # DST (Maret–Oktober) — untuk nanti
    SESSION_HOURS = [
        ("ASIA", 1, 10),
    ]
