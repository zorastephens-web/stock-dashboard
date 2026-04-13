import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# 1. Create Alert
def create_alert(ticker, alert_type, threshold):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO alerts (ticker, alert_type, threshold)
    VALUES (%s, %s, %s)
    """
    cursor.execute(query, (ticker, alert_type, threshold))
    conn.commit()

    cursor.close()
    conn.close()


# 2. Check Alerts
def check_alerts(ticker, predicted_price, rsi_value):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT * FROM alerts 
    WHERE ticker = %s AND is_active = TRUE
    """
    cursor.execute(query, (ticker,))
    alerts = cursor.fetchall()

    for alert in alerts:
        alert_id = alert['id']
        alert_type = alert['alert_type']
        threshold = alert['threshold']

        triggered = False
        message = ""

        if alert_type == "price_above" and predicted_price > threshold:
            triggered = True
            message = f"{ticker} price above {threshold}"

        elif alert_type == "price_below" and predicted_price < threshold:
            triggered = True
            message = f"{ticker} price below {threshold}"

        elif alert_type == "rsi_above" and rsi_value > threshold:
            triggered = True
            message = f"{ticker} RSI above {threshold}"

        elif alert_type == "rsi_below" and rsi_value < threshold:
            triggered = True
            message = f"{ticker} RSI below {threshold}"

        if triggered:
            log_alert(alert_id, ticker, predicted_price, message)

    cursor.close()
    conn.close()


# 3. Log Alert
def log_alert(alert_id, ticker, value, message):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO alert_history (alert_id, ticker, triggered_value, message)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(query, (alert_id, ticker, value, message))
    conn.commit()

    cursor.close()
    conn.close()


# 4. Get Alert History
def get_alert_history():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = "SELECT * FROM alert_history ORDER BY created_at DESC"
    cursor.execute(query)

    history = cursor.fetchall()

    cursor.close()
    conn.close()

    return history
