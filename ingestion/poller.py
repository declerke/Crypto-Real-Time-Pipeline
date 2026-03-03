import os
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"
POLLING_INTERVAL   = int(os.getenv("POLLING_INTERVAL_SECONDS", 30))
TOP_SYMBOLS_HISTORY = int(os.getenv("TOP_SYMBOLS_FOR_HISTORY", 20))

DB_CONFIG = {
    "dbname":   os.getenv("POSTGRES_DB",       "cryptodb"),
    "user":     os.getenv("POSTGRES_USER",     "cryptouser"),
    "password": os.getenv("POSTGRES_PASSWORD", "cryptopass"),
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
}

UPSERT_SQL = """
INSERT INTO crypto_ticker_24h (
    symbol,
    price_usdt,
    price_change_percent,
    volume_usdt,
    high_price_24h,
    low_price_24h,
    last_update
) VALUES (
    %(symbol)s,
    %(price_usdt)s,
    %(price_change_percent)s,
    %(volume_usdt)s,
    %(high_price_24h)s,
    %(low_price_24h)s,
    %(last_update)s
)
ON CONFLICT (symbol) DO UPDATE SET
    price_usdt           = EXCLUDED.price_usdt,
    price_change_percent = EXCLUDED.price_change_percent,
    volume_usdt          = EXCLUDED.volume_usdt,
    high_price_24h       = EXCLUDED.high_price_24h,
    low_price_24h        = EXCLUDED.low_price_24h,
    last_update          = EXCLUDED.last_update;
"""

HISTORY_INSERT_SQL = """
INSERT INTO ticker_history_pg (
    symbol,
    price_usdt,
    price_change_percent,
    volume_usdt,
    high_price_24h,
    low_price_24h,
    captured_at
) VALUES (
    %(symbol)s,
    %(price_usdt)s,
    %(price_change_percent)s,
    %(volume_usdt)s,
    %(high_price_24h)s,
    %(low_price_24h)s,
    %(captured_at)s
);
"""


def fetch_binance_tickers() -> list[dict]:
    try:
        response = requests.get(BINANCE_TICKER_URL, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error("Binance API request timed out.")
        return []
    except requests.exceptions.HTTPError as e:
        logger.error(f"Binance API HTTP error: {e.response.status_code} {e.response.text[:200]}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Binance API request failed: {e}")
        return []


def filter_usdt_pairs(tickers: list[dict]) -> list[dict]:
    return [t for t in tickers if t.get("symbol", "").endswith("USDT")]


def transform_ticker(ticker: dict, ts: datetime) -> dict:
    return {
        "symbol":               ticker["symbol"],
        "price_usdt":           float(ticker["lastPrice"]),
        "price_change_percent": float(ticker["priceChangePercent"]),
        "volume_usdt":          float(ticker["quoteVolume"]),
        "high_price_24h":       float(ticker["highPrice"]),
        "low_price_24h":        float(ticker["lowPrice"]),
        "last_update":          ts,
    }


def select_top_symbols_by_abs_change(tickers: list[dict], n: int) -> set[str]:
    sorted_tickers = sorted(
        tickers,
        key=lambda x: abs(float(x.get("priceChangePercent", 0))),
        reverse=True,
    )
    top = {t["symbol"] for t in sorted_tickers[:n]}
    top.update({"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"})
    return top


def write_to_postgres(records: list[dict], history_symbols: set[str]) -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, UPSERT_SQL, records, page_size=500
                )

                now = datetime.now(timezone.utc)
                history_records = []
                for rec in records:
                    if rec["symbol"] in history_symbols:
                        hr = dict(rec)
                        hr["captured_at"] = now
                        history_records.append(hr)

                if history_records:
                    psycopg2.extras.execute_batch(
                        cur, HISTORY_INSERT_SQL, history_records, page_size=200
                    )
    finally:
        conn.close()


def run_polling_cycle() -> None:
    cycle_start = time.monotonic()
    logger.info("Polling cycle started.")

    raw_tickers = fetch_binance_tickers()
    if not raw_tickers:
        logger.warning("No data received from Binance — skipping cycle.")
        return

    usdt_tickers = filter_usdt_pairs(raw_tickers)
    logger.info(f"Received {len(raw_tickers)} total tickers, {len(usdt_tickers)} USDT pairs.")

    if not usdt_tickers:
        logger.warning("No USDT pairs found after filtering.")
        return

    ts            = datetime.now(timezone.utc)
    records       = [transform_ticker(t, ts) for t in usdt_tickers]
    top_symbols   = select_top_symbols_by_abs_change(usdt_tickers, TOP_SYMBOLS_HISTORY)

    write_to_postgres(records, top_symbols)

    elapsed = time.monotonic() - cycle_start
    logger.info(
        f"Cycle complete in {elapsed:.2f}s — "
        f"upserted {len(records)} symbols, "
        f"history rows written for {len([r for r in records if r['symbol'] in top_symbols])} top movers."
    )

    gainers = sorted(records, key=lambda x: x["price_change_percent"], reverse=True)[:3]
    losers  = sorted(records, key=lambda x: x["price_change_percent"])[:3]
    logger.info(
        "Top gainers: " + ", ".join(
            f"{r['symbol']} {r['price_change_percent']:+.2f}%" for r in gainers
        )
    )
    logger.info(
        "Top losers:  " + ", ".join(
            f"{r['symbol']} {r['price_change_percent']:+.2f}%" for r in losers
        )
    )


def wait_for_postgres(max_retries: int = 30, retry_delay: int = 5) -> bool:
    logger.info("Waiting for PostgreSQL to be ready...")
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("PostgreSQL connection established.")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(
                f"PostgreSQL not ready (attempt {attempt}/{max_retries}): {e} "
                f"— retrying in {retry_delay}s"
            )
            time.sleep(retry_delay)
    logger.error("PostgreSQL never became ready. Exiting.")
    return False


def main() -> None:
    logger.info("=" * 60)
    logger.info("  Crypto Market Poller — Binance → PostgreSQL")
    logger.info(f"  Polling interval : {POLLING_INTERVAL}s")
    logger.info(f"  History symbols  : top {TOP_SYMBOLS_HISTORY} movers + BTC/ETH/SOL/BNB/XRP")
    logger.info("=" * 60)

    if not wait_for_postgres():
        return

    while True:
        try:
            run_polling_cycle()
        except psycopg2.Error as e:
            logger.error(f"Database error during polling cycle: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error during polling cycle: {e}", exc_info=True)

        logger.info(f"Sleeping {POLLING_INTERVAL}s until next cycle...")
        time.sleep(POLLING_INTERVAL)


if __name__ == "__main__":
    main()