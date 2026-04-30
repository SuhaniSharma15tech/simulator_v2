import psycopg2
import psycopg2.pool
import psycopg2.extras
import os
import streamlit as st


_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=5,
    # Replace os.getenv with st.secrets
    host = st.secrets["DB_HOST"],
    port = int(st.secrets["DB_PORT"]),
    user = st.secrets["DB_USER"],
    password = st.secrets["DB_PASSWORD"],
    dbname = st.secrets["DB_NAME"],
    sslmode = "require"

)

def get_conn():
    return _pool.getconn()

def _release(conn):
    _pool.putconn(conn)

def query(sql, params=None):
    """Run a SELECT — returns list of dicts."""
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        _release(conn)

def execute(sql, params=None):
    """Run INSERT / UPDATE / DELETE — auto commits."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        _release(conn)

def execute_many(sql, param_list):
    """Bulk INSERT — auto commits."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, param_list)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        _release(conn)

def transaction(operations):
    """
    Run multiple writes atomically.
    Pass a list of (sql, params) tuples.
    Rolls back everything if any step fails.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        for sql, params in operations:
            cur.execute(sql, params or ())
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _release(conn)
