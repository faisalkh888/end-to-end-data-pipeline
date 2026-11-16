"""Streamlit warehouse dashboard for commerce metrics and pipeline health."""
import os

import altair as alt
import pandas as pd
import psycopg
import streamlit as st


DSN = os.getenv("DATABASE_URL", "postgresql://loader:local-only@localhost:5432/commerce")
st.set_page_config(page_title="Commerce | Data Platform", page_icon="📦", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');
  :root { --ink:#172b3b; --muted:#718292; --line:#e4ebef; --mint:#d9f4ed; --teal:#087f70; --blue:#eef5fa; }
  html, body, [class*="css"] { font-family:'DM Sans',sans-serif; }
  .stApp { background:linear-gradient(180deg,#f7fafb 0%,#f4f7f8 100%); color:var(--ink); }
  .block-container { padding-top:2.1rem; padding-bottom:3rem; max-width:1440px; }
  h1,h2,h3 { font-family:'Manrope',sans-serif !important; color:var(--ink); letter-spacing:-.035em; }
  h1 { font-size:2.15rem !important; margin-bottom:.15rem !important; }
  h2 { font-size:1.16rem !important; }
  [data-testid="stSidebar"] { background:#fff; border-right:1px solid var(--line); }
  [data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3 { letter-spacing:-.02em; }
  [data-testid="stMetric"] { background:#fff; border:1px solid var(--line); border-radius:16px; padding:17px 19px; box-shadow:0 3px 12px rgba(20,47,63,.035); }
  [data-testid="stMetricLabel"] { color:var(--muted); font-size:.83rem; }
  [data-testid="stMetricValue"] { color:var(--ink); font-family:'Manrope',sans-serif; font-weight:800; }
  [data-testid="stMetricDelta"] { font-size:.77rem; }
  [data-testid="stDataFrame"] { border:1px solid var(--line); border-radius:12px; overflow:hidden; }
  div[data-testid="stVerticalBlockBorderWrapper"] { border-color:var(--line); border-radius:16px; background:#fff; }
  .hero { display:flex; justify-content:space-between; align-items:center; gap:18px; margin-bottom:1.35rem; }
  .eyebrow { color:var(--teal); text-transform:uppercase; font-size:.7rem; font-weight:700; letter-spacing:.14em; margin-bottom:.38rem; }
  .subhead { color:var(--muted); font-size:.95rem; margin-top:.25rem; }
  .live { display:inline-flex; align-items:center; gap:8px; color:#087f70; background:#e5f7f2; border:1px solid #c8eee4; border-radius:999px; padding:8px 13px; font-size:.78rem; font-weight:700; white-space:nowrap; }
  .dot { width:8px;height:8px;border-radius:50%;background:#11a889;box-shadow:0 0 0 4px #c9f0e5; }
  .section-note { color:var(--muted); font-size:.83rem; margin-top:-.45rem; margin-bottom:.8rem; }
  .empty { border:1px dashed #ccd9df; border-radius:16px; background:#fff; padding:28px; text-align:center; color:var(--muted); }
  footer { visibility:hidden; }
</style>
""", unsafe_allow_html=True)


def query_warehouse(date_start, date_end):
    with psycopg.connect(DSN) as conn:
        has_facts = conn.execute("SELECT to_regclass('warehouse.fact_orders')").fetchone()[0]
        if has_facts is None:
            return None, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        bounds = pd.read_sql_query("SELECT min(full_date) AS first_day,max(full_date) AS last_day FROM warehouse.dim_date", conn).iloc[0]
        daily = pd.read_sql_query("""
            SELECT dd.full_date, coalesce(sum(f.revenue) FILTER (WHERE f.status='completed'),0) AS revenue,
                   count(*) FILTER (WHERE f.status='completed') AS orders
            FROM warehouse.fact_orders f JOIN warehouse.dim_date dd USING(date_key)
            WHERE dd.full_date BETWEEN %s AND %s GROUP BY 1 ORDER BY 1
        """, conn, params=(date_start, date_end))
        countries = pd.read_sql_query("""
            SELECT dc.country_code, count(*) FILTER (WHERE f.status='completed') AS orders,
                   coalesce(sum(f.revenue) FILTER (WHERE f.status='completed'),0) AS revenue
            FROM warehouse.fact_orders f JOIN warehouse.dim_date dd USING(date_key)
            JOIN warehouse.dim_country dc USING(country_key)
            WHERE dd.full_date BETWEEN %s AND %s GROUP BY 1 ORDER BY orders DESC, dc.country_code
        """, conn, params=(date_start, date_end))
        retention = pd.read_sql_query("""
            WITH customer_month AS (
              SELECT DISTINCT dc.customer_id,date_trunc('month',dd.full_date)::date AS month
              FROM warehouse.fact_orders f JOIN warehouse.dim_customer dc USING(customer_key)
              JOIN warehouse.dim_date dd USING(date_key)
              WHERE f.status='completed' AND dd.full_date BETWEEN %s AND %s
            ), first_month AS (
              SELECT customer_id,min(month) AS cohort FROM customer_month GROUP BY 1
            ), cohort_sizes AS (
              SELECT cohort,count(*) AS cohort_customers FROM first_month GROUP BY 1
            )
            SELECT fm.cohort,cm.month AS activity_month,cs.cohort_customers,
                   count(DISTINCT cm.customer_id) AS active_customers,
                   count(DISTINCT cm.customer_id) FILTER (WHERE cm.month>fm.cohort) AS retained_customers,
                   round(100.0*count(DISTINCT cm.customer_id) FILTER (WHERE cm.month>fm.cohort)/nullif(cs.cohort_customers,0),1) AS retention_pct
            FROM customer_month cm JOIN first_month fm USING(customer_id)
            JOIN cohort_sizes cs USING(cohort)
            GROUP BY fm.cohort,cm.month,cs.cohort_customers ORDER BY fm.cohort,cm.month
        """, conn, params=(date_start, date_end))
        runs = pd.read_sql_query("""
            SELECT logical_date,status,finished_at,source_counts,quarantine_count,duration
            FROM warehouse.freshness_volume ORDER BY started_at DESC LIMIT 8
        """, conn)
        quarantine = pd.read_sql_query("""
            SELECT entity,count(*) AS records FROM ops.quarantine
            WHERE quarantined_at >= now()-interval '30 days' GROUP BY 1 ORDER BY records DESC
        """, conn)
    return bounds, daily, countries, retention, runs, quarantine


st.markdown("""
<div class="hero">
  <div><div class="eyebrow">Commerce intelligence</div><h1>Data Platform</h1>
  <div class="subhead">A clear view of sales performance, customer activity, and pipeline health.</div></div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Workspace")
    st.caption("E-COMMERCE · WAREHOUSE")
    st.divider()

try:
    bounds, daily_all, countries_all, retention_all, runs, quarantine = query_warehouse("1900-01-01", "2999-12-31")
    st.markdown('<div class="live"><span class="dot"></span>Warehouse connected</div>', unsafe_allow_html=True)
    if bounds is None:
        st.markdown('<div class="empty"><strong>Warehouse online — waiting for the first run</strong><br>Trigger the Airflow DAG to load sales and customer metrics.</div>', unsafe_allow_html=True)
    elif pd.isna(bounds.first_day):
        st.markdown('<div class="empty">Your warehouse is ready. Run the Airflow pipeline to load the first day of commerce data.</div>', unsafe_allow_html=True)
    else:
        first_day, last_day = pd.Timestamp(bounds.first_day).date(), pd.Timestamp(bounds.last_day).date()
        with st.sidebar:
            selected_range = st.date_input("Order date range", value=(first_day, last_day), min_value=first_day, max_value=last_day)
            st.caption("Metrics use completed orders. Customer data is pseudonymized in the warehouse.")
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            date_start, date_end = selected_range
        else:
            date_start = date_end = selected_range
        bounds, daily, countries, retention, runs, quarantine = query_warehouse(date_start, date_end)
        revenue = float(daily.revenue.sum()) if not daily.empty else 0.0
        order_count = int(daily.orders.sum()) if not daily.empty else 0
        aov = revenue / order_count if order_count else 0.0
        country_count = int((countries.orders > 0).sum()) if not countries.empty else 0

        st.caption(f"{date_start:%b %d, %Y}  —  {date_end:%b %d, %Y}")
        k1,k2,k3,k4 = st.columns(4)
        k1.metric("Completed revenue", f"${revenue:,.2f}")
        k2.metric("Completed orders", f"{order_count:,}")
        k3.metric("Average order value", f"${aov:,.2f}")
        k4.metric("Countries served", f"{country_count:,}")

        st.write("")
        left,right = st.columns([1.45,1], gap="large")
        with left:
            with st.container(border=True):
                st.subheader("Revenue over time")
                st.markdown('<div class="section-note">Daily completed-order sales</div>', unsafe_allow_html=True)
                if daily.empty:
                    st.info("No sales in this date range.")
                else:
                    revenue_chart = alt.Chart(daily).mark_area(color="#a9e7d7", opacity=.65, line={"color":"#087f70","strokeWidth":3}).encode(
                        x=alt.X("full_date:T", title=None, axis=alt.Axis(format="%b %d", labelColor="#718292", grid=False, tickColor="#dbe5e9")),
                        y=alt.Y("revenue:Q", title="USD", axis=alt.Axis(format="$,.0f", labelColor="#718292", gridColor="#e8eef1", tickColor="#dbe5e9")),
                        tooltip=[alt.Tooltip("full_date:T", title="Date"), alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f")]
                    ).properties(height=250).configure(background="transparent").configure_view(stroke=None)
                    st.altair_chart(revenue_chart, use_container_width=True)
        with right:
            with st.container(border=True):
                st.subheader("Orders by country")
                st.markdown('<div class="section-note">Completed orders by destination</div>', unsafe_allow_html=True)
                if countries.empty:
                    st.info("No country activity in this date range.")
                else:
                    country_chart = alt.Chart(countries).mark_bar(color="#4c82aa", cornerRadiusEnd=6, size=24).encode(
                        x=alt.X("orders:Q", title="Orders", axis=alt.Axis(labelColor="#718292", gridColor="#e8eef1", tickColor="#dbe5e9")),
                        y=alt.Y("country_code:N", title=None, sort="-x", axis=alt.Axis(labelColor="#425565", tickSize=0)),
                        tooltip=[alt.Tooltip("country_code:N", title="Country"), alt.Tooltip("orders:Q", title="Orders"), alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f")]
                    ).properties(height=150).configure(background="transparent").configure_view(stroke=None)
                    st.altair_chart(country_chart, use_container_width=True)
                    st.dataframe(countries.rename(columns={"country_code":"Country","orders":"Orders","revenue":"Revenue"}),
                                 hide_index=True, use_container_width=True, column_config={"Revenue":st.column_config.NumberColumn(format="$%.2f")})

        left,right = st.columns([1.1,1], gap="large")
        with left:
            with st.container(border=True):
                st.subheader("Customer retention")
                st.markdown('<div class="section-note">Share of each first-purchase cohort returning in later months</div>', unsafe_allow_html=True)
                if retention.empty:
                    st.info("More purchase history is needed to show retention cohorts.")
                else:
                    display = retention.rename(columns={"cohort":"Cohort month","activity_month":"Activity month","cohort_customers":"Cohort size","active_customers":"Active","retained_customers":"Returning","retention_pct":"Retention"})
                    st.dataframe(display, hide_index=True, use_container_width=True,
                        column_config={"Cohort month":st.column_config.DateColumn(format="MMM YYYY"),
                                       "Activity month":st.column_config.DateColumn(format="MMM YYYY"),
                                       "Retention":st.column_config.NumberColumn(format="%.1f%%")})
        with right:
            with st.container(border=True):
                st.subheader("Pipeline health")
                if runs.empty:
                    st.info("No pipeline runs recorded yet.")
                else:
                    latest = runs.iloc[0]
                    status = str(latest.status).upper()
                    st.markdown(f"**Latest run:** `{status}`  ·  {pd.Timestamp(latest.logical_date):%b %d, %Y}")
                    if int(latest.quarantine_count):
                        st.warning(f"{int(latest.quarantine_count)} record(s) quarantined in the latest run.")
                    else:
                        st.success("Latest run completed without quarantined records.")
                    if not quarantine.empty:
                        st.caption("Quarantined in the last 30 days")
                        st.dataframe(quarantine.rename(columns={"entity":"Entity","records":"Records"}), hide_index=True, use_container_width=True)
                    with st.expander("Recent run history"):
                        st.dataframe(runs.rename(columns={"logical_date":"Date","status":"Status","finished_at":"Finished","source_counts":"Source volumes","quarantine_count":"Quarantined","duration":"Duration"}),
                                     hide_index=True, use_container_width=True)
except Exception:
    st.markdown('<div class="empty"><strong>Warehouse unavailable</strong><br>Check that PostgreSQL is healthy and the loader password in .env matches its database role, then rerun <code>docker compose up -d</code>.</div>', unsafe_allow_html=True)
    with st.expander("Connection help"):
        st.code("docker compose ps\ndocker compose logs airflow-init postgres\ndocker compose up -d --force-recreate postgres\ndocker compose exec -T postgres sh /seed/rotate-roles.sh\ndocker compose up -d", language="powershell")
