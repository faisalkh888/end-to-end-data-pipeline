import os
import pandas as pd
import psycopg
import streamlit as st

DSN = os.getenv("DATABASE_URL", "postgresql://loader:local-only@localhost:5432/commerce")
st.set_page_config(page_title="Commerce Metrics", layout="wide")
st.title("Commerce Data Platform")
st.caption("Warehouse metrics from validated, deduplicated order facts")

try:
    with psycopg.connect(DSN) as conn:
        daily = pd.read_sql("SELECT d.full_date, SUM(f.revenue) FILTER (WHERE f.status='completed') AS revenue, COUNT(*) FILTER (WHERE f.status='completed') AS orders FROM warehouse.fact_orders f JOIN warehouse.dim_date d USING(date_key) GROUP BY 1 ORDER BY 1", conn)
        by_country = pd.read_sql("SELECT c.country_code, COUNT(*) FILTER (WHERE f.status='completed') AS orders FROM warehouse.fact_orders f JOIN warehouse.dim_country c USING(country_key) GROUP BY 1 ORDER BY 2 DESC", conn)
        retention = pd.read_sql("""WITH customer_month AS (SELECT dc.customer_id, date_trunc('month',dd.full_date)::date AS month FROM warehouse.fact_orders f JOIN warehouse.dim_customer dc USING(customer_key) JOIN warehouse.dim_date dd USING(date_key) WHERE f.status='completed' GROUP BY 1,2), first_month AS (SELECT customer_id,min(month) AS cohort FROM customer_month GROUP BY 1) SELECT fm.cohort, cm.month, COUNT(DISTINCT cm.customer_id) AS active_customers, COUNT(DISTINCT cm.customer_id) FILTER (WHERE cm.month>fm.cohort) AS retained_customers FROM customer_month cm JOIN first_month fm USING(customer_id) GROUP BY 1,2 ORDER BY 1,2""", conn)
        runs = pd.read_sql("SELECT logical_date,status,finished_at,source_counts,quarantine_count FROM warehouse.freshness_volume ORDER BY started_at DESC LIMIT 20", conn)
    a, b = st.columns(2)
    with a:
        st.subheader("Daily revenue and orders")
        st.line_chart(daily.set_index("full_date")[["revenue"]])
        st.dataframe(daily, use_container_width=True)
    with b:
        st.subheader("Orders by country")
        st.bar_chart(by_country.set_index("country_code"))
        st.subheader("Customer retention by first purchase month")
        if not retention.empty:
            retention["retention_rate"] = retention.apply(lambda r: r.retained_customers / r.active_customers if r.active_customers else 0, axis=1)
            st.dataframe(retention, use_container_width=True)
    st.subheader("Pipeline freshness and volume")
    st.dataframe(runs, use_container_width=True)
except Exception as exc:
    st.info("Warehouse tables appear after the first successful Airflow run.")
    st.code(str(exc))
