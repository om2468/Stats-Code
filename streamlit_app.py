import os
import tempfile
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Stats Analysis", layout="wide")
st.title("Stats Analysis — Interactive Charts")
st.caption("Run SQL on your DuckDB file and explore interactive charts. Works fully offline.")

# Sidebar inputs
with st.sidebar:
    st.header("Data source")
    uploaded = st.file_uploader("Upload your .duckdb file", type=["duckdb"]) 
    db_path_input = st.text_input("…or enter a local path to a .duckdb file", value="")
    table_name = st.text_input("Table name in the database", value="analysis_duckdb")
    top_n_pairs = st.number_input("Top N service pairs", min_value=5, max_value=50, value=15, step=1)
    top_n_accounts = st.number_input("Top N accounts (for bar charts)", min_value=10, max_value=200, value=30, step=5)

# Resolve DB path (uploaded or typed)
_db_temp_path = None
if uploaded is not None:
    # Persist uploaded file to a temp location so DuckDB can open it by path
    with tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb") as tmp:
        tmp.write(uploaded.getbuffer())
        _db_temp_path = tmp.name
        db_path = _db_temp_path
elif db_path_input:
    db_path = db_path_input
else:
    db_path = None

@st.cache_resource(show_spinner=False)
def get_conn(path: str):
    return duckdb.connect(path, read_only=True)

@st.cache_data(show_spinner=False)
def run_sql(path: str, sql: str) -> pd.DataFrame:
    con = get_conn(path)
    return con.execute(sql).df()

if not db_path:
    st.info("Upload a .duckdb file or enter a local path in the sidebar to begin.")
    try:
        st.stop()
    except Exception:
        # If not running via `streamlit run`, exit gracefully
        raise SystemExit(0)

# Only check existence when we have a non-empty path
if db_path and not os.path.exists(db_path):
    st.error(f"Database not found: {db_path}")
    try:
        st.stop()
    except Exception:
        raise SystemExit(1)

st.success(f"Using database: {db_path}")

T = table_name  # shorthand

# Define SQLs (same logic as before)
SQL = {
    "consulting": f"""
WITH FirstInitialPurchase AS (
  SELECT account, MIN(date) AS first_initial_date
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), InitialSpend AS (
  SELECT account, SUM(credit) AS total_license_subscription_spend
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), FollowUpConsultingSpend AS (
  SELECT fip.account, SUM(ad.credit) AS total_fme_consulting_spend
  FROM {T} AS ad
  JOIN FirstInitialPurchase AS fip ON ad.account = fip.account
  WHERE ad.type = 'FME Consulting' AND ad.date > fip.first_initial_date
  GROUP BY fip.account
)
SELECT i.account, i.total_license_subscription_spend, f.total_fme_consulting_spend
FROM InitialSpend AS i
JOIN FollowUpConsultingSpend AS f ON i.account = f.account
ORDER BY i.account;""",

    "training": f"""
WITH FirstInitialPurchase AS (
  SELECT account, MIN(date) AS first_initial_date
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), InitialSpend AS (
  SELECT account, SUM(credit) AS total_license_subscription_spend
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), FollowUpTrainingSpend AS (
  SELECT fip.account, SUM(ad.credit) AS total_fme_training_spend
  FROM {T} AS ad
  JOIN FirstInitialPurchase AS fip ON ad.account = fip.account
  WHERE ad.type = 'FME Training' AND ad.date > fip.first_initial_date
  GROUP BY fip.account
)
SELECT i.account, i.total_license_subscription_spend, f.total_fme_training_spend
FROM InitialSpend AS i
JOIN FollowUpTrainingSpend AS f ON i.account = f.account
ORDER BY i.account;""",

    "esri": f"""
WITH FirstInitialPurchase AS (
  SELECT account, MIN(date) AS first_initial_date
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), InitialSpend AS (
  SELECT account, SUM(credit) AS total_license_subscription_spend
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), FollowUpEsRiSpend AS (
  SELECT fip.account, SUM(ad.credit) AS total_esri_consulting_spend
  FROM {T} AS ad
  JOIN FirstInitialPurchase AS fip ON ad.account = fip.account
  WHERE ad.type = 'Esri Consulting' AND ad.date > fip.first_initial_date
  GROUP BY fip.account
)
SELECT i.account, i.total_license_subscription_spend, f.total_esri_consulting_spend
FROM InitialSpend AS i
JOIN FollowUpEsRiSpend AS f ON i.account = f.account
ORDER BY i.account;""",

    "revcon": f"""
WITH CustomerTotalRevenue AS (
  SELECT account, SUM(credit) AS total_revenue
  FROM {T}
  GROUP BY account
), RunningTotal AS (
  SELECT account, total_revenue,
         SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue
  FROM CustomerTotalRevenue
)
SELECT account, total_revenue, cumulative_revenue,
       cumulative_revenue / (SELECT SUM(total_revenue) FROM CustomerTotalRevenue) * 100 AS cumulative_percentage
FROM RunningTotal
ORDER BY total_revenue DESC;""",

    "attach": f"""
WITH CoreProductCustomers AS (
  SELECT DISTINCT account FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
), AttachServiceCustomers AS (
  SELECT DISTINCT account FROM {T}
  WHERE type = 'FME Training'
)
SELECT (SELECT COUNT(*) FROM AttachServiceCustomers WHERE account IN (SELECT account FROM CoreProductCustomers)) * 100.0 /
       (SELECT COUNT(*) FROM CoreProductCustomers) AS training_attach_rate_percentage;""",

    "basket": f"""
SELECT a1.type AS service_1, a2.type AS service_2, COUNT(DISTINCT a1.account) AS number_of_customers
FROM {T} a1
JOIN {T} a2 ON a1.account = a2.account AND a1.type < a2.type
GROUP BY service_1, service_2
ORDER BY number_of_customers DESC;""",

    "timetosvc": f"""
WITH FirstPurchase AS (
  SELECT account, MIN(date) AS first_date
  FROM {T}
  WHERE type IN ('FME Licenses', 'FME Subscription')
  GROUP BY account
), FirstFollowUp AS (
  SELECT t.account, MIN(t.date) AS first_training_date
  FROM {T} t
  JOIN FirstPurchase fp ON t.account = fp.account
  WHERE t.type = 'FME Training' AND t.date > fp.first_date
  GROUP BY t.account
)
SELECT AVG(first_training_date - first_date) AS avg_days_to_training
FROM FirstPurchase fp
JOIN FirstFollowUp ff ON fp.account = ff.account;""",

    "trends": f"""
SELECT strftime(date, '%Y-%m') AS sales_month, type, SUM(credit) AS monthly_revenue
FROM {T}
GROUP BY sales_month, type
ORDER BY sales_month, type;""",
}

# Layout: two columns per row
c1, c2 = st.columns(2)

# 1) Licenses/Subscriptions -> Consulting
with c1:
    st.subheader("Licenses/Subscriptions → FME Consulting")
    df = run_sql(db_path, SQL["consulting"])    
    if not df.empty:
        # Keep top by consulting spend
        df_top = df.sort_values("total_fme_consulting_spend", ascending=False).head(int(top_n_accounts))
        fig = go.Figure()
        fig.add_bar(name="License+Sub Spend", x=df_top["account"].astype(str), y=df_top["total_license_subscription_spend"]) 
        fig.add_bar(name="FME Consulting Spend", x=df_top["account"].astype(str), y=df_top["total_fme_consulting_spend"]) 
        fig.update_layout(barmode="group", xaxis_tickangle=-60, height=420)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# 2) Licenses/Subscriptions -> Training
with c2:
    st.subheader("Licenses/Subscriptions → FME Training")
    df = run_sql(db_path, SQL["training"])    
    if not df.empty:
        df_top = df.sort_values("total_fme_training_spend", ascending=False).head(int(top_n_accounts))
        fig = go.Figure()
        fig.add_bar(name="License+Sub Spend", x=df_top["account"].astype(str), y=df_top["total_license_subscription_spend"]) 
        fig.add_bar(name="FME Training Spend", x=df_top["account"].astype(str), y=df_top["total_fme_training_spend"]) 
        fig.update_layout(barmode="group", xaxis_tickangle=-60, height=420)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# 3) Licenses/Subscriptions -> Esri Consulting
c3, c4 = st.columns(2)
with c3:
    st.subheader("Licenses/Subscriptions → Esri Consulting")
    df = run_sql(db_path, SQL["esri"])    
    if not df.empty:
        df_top = df.sort_values("total_esri_consulting_spend", ascending=False).head(int(top_n_accounts))
        fig = go.Figure()
        fig.add_bar(name="License+Sub Spend", x=df_top["account"].astype(str), y=df_top["total_license_subscription_spend"]) 
        fig.add_bar(name="Esri Consulting Spend", x=df_top["account"].astype(str), y=df_top["total_esri_consulting_spend"]) 
        fig.update_layout(barmode="group", xaxis_tickangle=-60, height=420)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# 4) Revenue concentration
with c4:
    st.subheader("Revenue Concentration")
    df = run_sql(db_path, SQL["revcon"])    
    if not df.empty:
        df_plot = df.reset_index(drop=True).reset_index(names="rank")
        df_plot["rank"] = df_plot["rank"] + 1
        # Prepend a (0,0) point so the curve starts at origin
        start_row = {
            "rank": 0,
            "account": "Start",
            "total_revenue": 0,
            "cumulative_revenue": 0,
            "cumulative_percentage": 0.0,
        }
        import pandas as _pd
        df_plot = _pd.concat([_pd.DataFrame([start_row]), df_plot], ignore_index=True)

        # Interactive line with hover-only account labels (no text on chart)
        fig = px.line(
            df_plot,
            x="rank",
            y="cumulative_percentage",
            markers=True,
            height=420,
            custom_data=["account"],
        )
        fig.update_traces(
            marker=dict(size=6),
            hovertemplate="Rank=%{x}<br>Account=%{customdata[0]}<br>Cumulative %=%{y:.2f}<extra></extra>",
        )
        fig.update_layout(
            yaxis_title="Cumulative % of Revenue",
            xaxis_title="Customers (sorted by revenue)",
            xaxis=dict(range=[0, float(df_plot["rank"].max())], zeroline=True),
            yaxis=dict(range=[0, 100], zeroline=True),
        )
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# 5) Attach rate
c5, c6 = st.columns(2)
with c5:
    st.subheader("Training Attach Rate (%)")
    df = run_sql(db_path, SQL["attach"])    
    val = None
    if not df.empty and "training_attach_rate_percentage" in df.columns:
        try:
            val = float(df.loc[0, "training_attach_rate_percentage"]) if pd.notna(df.loc[0, "training_attach_rate_percentage"]) else None
        except Exception:
            pass
    st.metric(label="Attach rate", value=f"{val:,.2f}%" if val is not None else "—")
    st.dataframe(df, use_container_width=True, hide_index=True)

# 6) Service combinations (Top N)
with c6:
    st.subheader("Service Combinations (Top N)")
    df = run_sql(db_path, SQL["basket"])    
    if not df.empty:
        df["pair"] = df["service_1"].astype(str) + " + " + df["service_2"].astype(str)
        df = df.sort_values("number_of_customers", ascending=False).head(int(top_n_pairs))
        fig = px.bar(df, x="pair", y="number_of_customers", height=420)
        fig.update_layout(xaxis_tickangle=-60)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# 7) Avg days to training after first purchase
c7, c8 = st.columns(2)
with c7:
    st.subheader("Avg Days to Training After First Purchase")
    df = run_sql(db_path, SQL["timetosvc"])    
    # Convert DuckDB interval/timedelta to days
    days_val = None
    if not df.empty and "avg_days_to_training" in df.columns:
        val = df.loc[0, "avg_days_to_training"]
        try:
            if pd.isna(val):
                days_val = None
            elif hasattr(val, 'days'):
                days_val = float(val.days)
            else:
                # Try parse strings like '123 days'
                days_val = float(str(val).split()[0])
        except Exception:
            days_val = None
    st.metric(label="Avg days", value=f"{days_val:,.2f}" if days_val is not None else "—")
    st.dataframe(df, use_container_width=True, hide_index=True)

# 8) Monthly revenue trends
with c8:
    st.subheader("Monthly Revenue by Service Type")
    df = run_sql(db_path, SQL["trends"])    
    if not df.empty:
        fig = px.line(df, x="sales_month", y="monthly_revenue", color="type", markers=True, height=420)
        fig.update_layout(xaxis_tickangle=-60)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# Cleanup temp file on app exit
if _db_temp_path and os.path.exists(_db_temp_path):
    # Streamlit may rerun; best-effort cleanup only when process exits. Left as-is for simplicity.
    pass
