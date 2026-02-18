"""
Snowflake connection helper - supports both SiS and Streamlit Cloud
"""

import streamlit as st
from datetime import datetime, timedelta

def get_ttl_until_next_refresh():
    """
    Calculate seconds until next data refresh (2x daily: 6h and 23h).
    Returns TTL in seconds, minimum 60 seconds.
    """
    now = datetime.now()
    refresh_hours = [7, 17, 0]

    # Find next refresh time
    for hour in refresh_hours:
        next_refresh = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if now < next_refresh:
            ttl = (next_refresh - now).total_seconds()
            return max(int(ttl), 60)

    # If past 23h, next refresh is 6h tomorrow
    next_refresh = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
    ttl = (next_refresh - now).total_seconds()
    return max(int(ttl), 60)

@st.cache_resource
def get_connection():
    """
    Returns a Snowflake connection that works in both environments:
    - Streamlit in Snowflake (SiS): uses get_active_session()
    - Streamlit Cloud: uses snowflake.connector with key-pair auth
    """
    try:
        # Try SiS first
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session, "sis"
    except:
        # Fall back to Streamlit Cloud connection with key-pair
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        # Load private key from secrets
        private_key_pem = st.secrets["snowflake"]["private_key"]

        # Handle the private key format
        p_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None,
            backend=default_backend()
        )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            private_key=pkb,
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            role=st.secrets["snowflake"]["role"]
        )
        return conn, "cloud"

@st.cache_data(ttl=get_ttl_until_next_refresh())
def run_query(query):
    """
    Execute a query and return results as pandas DataFrame.
    Cache expires at next scheduled refresh (2x daily: 6h and 23h).
    """
    conn, env = get_connection()

    if env == "sis":
        return conn.sql(query).to_pandas()
    else:
        cursor = conn.cursor()
        cursor.execute(query)
        import pandas as pd
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(data, columns=columns)
