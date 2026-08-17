import psycopg2
from airflow.utils.email import send_email
from config import DB_CONN

conn = psycopg2.connect(**DB_CONN)
cur = conn.cursor()

new_alerts = []

# 1. Price drop
cur.execute("""
    SELECT da.asset_key, tgl.symbol, tgl.trade_date, tgl.price_change_pct
    FROM public_marts.top_gainers_losers tgl
    JOIN public_marts.dim_asset da 
        ON tgl.asset_id = da.asset_id
        AND tgl.trade_date >= da.valid_from
        AND (da.valid_to IS NULL OR tgl.trade_date < da.valid_to)
    WHERE tgl.price_change_pct <= -10
      AND tgl.trade_date = (SELECT MAX(trade_date) FROM public_marts.top_gainers_losers)
      AND NOT EXISTS (
          SELECT 1 FROM public.market_alerts_log mal
          WHERE mal.asset_key = da.asset_key
            AND mal.alert_date = tgl.trade_date
            AND mal.alert_type = 'price_drop'
      );
""")
for asset_key, symbol, alert_date, value in cur.fetchall():
    new_alerts.append((asset_key, symbol, alert_date, 'price_drop', value))

# 2. Volume spike
cur.execute("""
    SELECT da.asset_key, da.symbol, dd.full_date, fva.volume_change_pct_7d
    FROM public_marts.fact_volume_analysis fva
    JOIN public_marts.dim_asset da ON fva.asset_key = da.asset_key
    JOIN public_marts.dim_date dd ON fva.date_key = dd.date_key
    WHERE fva.is_volume_spike = true
      AND dd.full_date = (SELECT MAX(full_date) FROM public_marts.dim_date WHERE date_key IN (SELECT date_key FROM public_marts.fact_volume_analysis))
      AND NOT EXISTS (
          SELECT 1 FROM public.market_alerts_log mal
          WHERE mal.asset_key = da.asset_key
            AND mal.alert_date = dd.full_date
            AND mal.alert_type = 'volume_spike'
      );
""")
for asset_key, symbol, alert_date, value in cur.fetchall():
    new_alerts.append((asset_key, symbol, alert_date, 'volume_spike', value))

# 3. Anomaly
cur.execute("""
    SELECT da.asset_key, da.symbol, dd.full_date, fmv.z_score
    FROM public_marts.fact_market_volatility fmv
    JOIN public_marts.dim_asset da ON fmv.asset_key = da.asset_key
    JOIN public_marts.dim_date dd ON fmv.date_key = dd.date_key
    WHERE fmv.is_anomaly = true
      AND dd.full_date = (SELECT MAX(full_date) FROM public_marts.dim_date WHERE date_key IN (SELECT date_key FROM public_marts.fact_market_volatility))
      AND NOT EXISTS (
          SELECT 1 FROM public.market_alerts_log mal
          WHERE mal.asset_key = da.asset_key
            AND mal.alert_date = dd.full_date
            AND mal.alert_type = 'anomaly'
      );
""")
for asset_key, symbol, alert_date, value in cur.fetchall():
    new_alerts.append((asset_key, symbol, alert_date, 'anomaly', value))

print(f"Најдени {len(new_alerts)} нови alerts:")
for a in new_alerts:
    print(a)

if new_alerts:
    rows_html = ""
    for asset_key, symbol, alert_date, alert_type, value in new_alerts:
        rows_html += f"<tr><td>{symbol}</td><td>{alert_date}</td><td>{alert_type}</td><td>{value:.4f}</td></tr>"

    html_content = f"""
    <h3>Market Alert Report — {new_alerts[0][2]}</h3>
    <table border="1" cellpadding="5" style="border-collapse: collapse;">
        <tr><th>Asset</th><th>Date</th><th>Type</th><th>Value</th></tr>
        {rows_html}
    </table>
    """

    send_email(
        to="damjan.murgjeski@gmail.com",
        subject=f"Market Alert: {len(new_alerts)} new condition(s) detected",
        html_content=html_content
    )
    print(f"Email испратен за {len(new_alerts)} alerts.")
    for asset_key, symbol, alert_date, alert_type, value in new_alerts:
        cur.execute("""
            INSERT INTO public.market_alerts_log (asset_key, alert_date, alert_type, metric_value)
            VALUES (%s, %s, %s, %s)
        """, (asset_key, alert_date, alert_type, value))
    conn.commit()
    print(f"Запишани {len(new_alerts)} redovi во market_alerts_log.")
else:
    print("Нема нови alerts, email не е испратен.")

cur.close()
conn.close()