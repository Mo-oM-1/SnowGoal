"""
Snowflake connection helper - supports both SiS and Streamlit Cloud
"""

import streamlit as st

def get_connection():
    """
    Returns a Snowflake connection that works in both environments:
    - Streamlit in Snowflake (SiS): uses get_active_session()
    - Streamlit Cloud: uses st.connection with secrets
    """
    try:
        # Try SiS first
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session, "sis"
    except:
        # Fall back to Streamlit Cloud connection
        conn = st.connection("snowflake", type="sql")
        return conn, "cloud"

def run_query(query):
    """
    Execute a query and return results as pandas DataFrame
    """
    conn, env = get_connection()

    if env == "sis":
        return conn.sql(query).to_pandas()
    else:
        return conn.query(query)
