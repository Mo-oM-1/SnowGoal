"""
Snowflake connection helper - supports both SiS and Streamlit Cloud
"""

import streamlit as st

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

@st.cache_data(ttl=600)
def run_query(query):
    """
    Execute a query and return results as pandas DataFrame
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
