import logging
import smtplib
from email.mime.text import MIMEText
import os
import psycopg2

from config import DB_CONN

SMTP_USER = os.getenv("SMTP_USER")
SMTP_MAIL_FROM = os.getenv("SMTP_MAIL_FROM")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
ALERT_EMAIL = os.getenv("ALERT_EMAIL", SMTP_MAIL_FROM)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def setup_alerts_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_marts.market_alerts_log (
            id serial PRIMARY KEY,
            asset_id varchar NOT NULL,
            alert_date date NOT NULL,
            alert_type varchar NOT NULL,
            alert_message text NOT NULL,
            sent_at timestamptz DEFAULT now(),
            UNIQUE (asset_id, alert_date, alert_type)
        )
    """)
    log.info("market_alerts_log table ready.")


def get_todays_alerts(cur):
    cur.execute("""
        SELECT symbol, date, alert_type, alert_message
        FROM public_marts.fact_market_alerts
        WHERE date = CURRENT_DATE
    """)
    return cur.fetchall()


def log_new_alerts(cur, alerts):
    new_alerts = []
    for symbol, date, alert_type, alert_message in alerts:
        cur.execute("""
            INSERT INTO public_marts.market_alerts_log (asset_id, alert_date, alert_type, alert_message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (asset_id, alert_date, alert_type) DO NOTHING
            RETURNING id
        """, (symbol, date, alert_type, alert_message))
        if cur.fetchone():
            new_alerts.append((symbol, alert_type, alert_message))
    return new_alerts


def _display_symbol(symbol):
    """Crypto assets are stored lowercase in dim_asset ('cardano', 'bitcoin');
    stock tickers are already uppercase ('AAPL', 'TSLA'). Capitalizing only
    the first character handles both without lowercasing a ticker."""
    return symbol[:1].upper() + symbol[1:]


def send_alert_email(new_alerts):
    if not SMTP_USER or not SMTP_PASSWORD:
        log.warning("SMTP_USER/SMTP_PASSWORD not set, skipping email send.")
        return

    body_lines = [
        f"- {_display_symbol(symbol)}: {message}"
        for symbol, _, message in new_alerts
    ]
    body = "Market alerts triggered today:\n\n" + "\n".join(body_lines)

    msg = MIMEText(body)
    msg["Subject"] = f"Crypto Pipeline: {len(new_alerts)} new market alert(s)"
    msg["From"] = SMTP_MAIL_FROM
    msg["To"] = ALERT_EMAIL

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    log.info(f"Sent alert email with {len(new_alerts)} alert(s).")


def run():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    setup_alerts_table(cur)
    conn.commit()

    alerts = get_todays_alerts(cur)
    log.info(f"Found {len(alerts)} alert condition(s) today.")

    new_alerts = log_new_alerts(cur, alerts)
    conn.commit()

    if new_alerts:
        send_alert_email(new_alerts)
    else:
        log.info("No new alerts to send (all already logged, or none triggered).")

    cur.close()
    conn.close()
    log.info("Alert check complete.")


if __name__ == "__main__":
    run()
