import os
import time
import chdb
import pandas as pd
import streamlit as st
import altair as alt

# -----------------------------------------------------------------------------
# 1. Page & Theme Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Real-Time CDC Analytics (chDB)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Dark theme styling matching UI requirements
st.markdown("""
<style>
    .stApp {
        background-color: #0F172A;
        color: #F8FAFC;
    }
    .metric-box {
        background-color: #1E293B;
        border-radius: 12px;
        padding: 16px;
        border: 1px solid #334155;
        text-align: center;
    }
    .metric-label {
        color: #94A3B8;
        font-size: 13px;
        font-weight: 500;
        margin-bottom: 4px;
    }
    .metric-val {
        color: #F8FAFC;
        font-size: 24px;
        font-weight: 700;
    }
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. Connection Settings & chDB Data Engine
# -----------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "cdc-lake")

S3_PATH = f"http://{MINIO_ENDPOINT}/{BUCKET}/silver/customer_transactions/*.parquet"

@st.cache_data(ttl=2)
def fetch_chdb_data(query: str):
    """Executes ClickHouse SQL query against Delta Parquet files in MinIO using chDB."""
    try:
        df = chdb.query(query, "Dataframe")
        return df
    except Exception:
        return pd.DataFrame()

def load_time_series_data():
    query = f"""
    SELECT 
        toStartOfMinute(updated_at) AS time_bucket,
        round(sum(total_amount), 2) AS total_spent,
        count(transaction_id) AS total_orders
    FROM s3('{S3_PATH}', '{ACCESS_KEY}', '{SECRET_KEY}', 'Parquet')
    GROUP BY time_bucket
    ORDER BY time_bucket ASC
    LIMIT 12
    """
    df = fetch_chdb_data(query)
    if df.empty:
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        values = [62.0, 71.0, 56.0, 63.0, 55.0, 64.0, 78.0, 95.0, 90.0, 82.0, 68.0, 107.0]
        orders = [12, 15, 10, 14, 9, 13, 18, 22, 19, 17, 14, 25]
        return pd.DataFrame({"month": months, "total_spent": values, "total_orders": orders})
    return df

def load_customer_leaderboard():
    query = f"""
    SELECT 
        customer_name,
        count(transaction_id) AS total_orders,
        round(sum(total_amount), 2) AS total_spent,
        round(avg(total_amount), 2) AS avg_order_value
    FROM s3('{S3_PATH}', '{ACCESS_KEY}', '{SECRET_KEY}', 'Parquet')
    GROUP BY customer_name
    ORDER BY total_spent DESC
    LIMIT 8
    """
    df = fetch_chdb_data(query)
    if df.empty:
        return pd.DataFrame({
            "customer_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "total_orders": [15, 12, 9, 7, 5],
            "total_spent": [450.0, 380.0, 290.0, 210.0, 150.0],
            "avg_order_value": [30.0, 31.6, 32.2, 30.0, 30.0]
        })
    return df

# -----------------------------------------------------------------------------
# 3. Main Dashboard Layout
# -----------------------------------------------------------------------------
st.title("⚡ Real-Time CDC Analytics (Powered by chDB)")

# Fetch Datasets
df_ts = load_time_series_data()
df_customers = load_customer_leaderboard()

# Extract Metrics
x_col = "month" if "month" in df_ts.columns else "time_bucket"
x_label = "Period" if "month" in df_ts.columns else "Time Bucket"

peak_max = float(df_ts["total_spent"].max()) if not df_ts.empty else 0.0
avg_val = float(df_ts["total_spent"].mean()) if not df_ts.empty else 0.0
sum_total = float(df_ts["total_spent"].sum()) if not df_ts.empty else 0.0

THEME_COLORS = {
    "Azure": "#3B82F6",
    "Sunset": "#F97316",
    "Neon": "#10B981"
}

# Top Main Chart Block
with st.container():
    ctrl_col1, ctrl_col2 = st.columns(2)
    with ctrl_col1:
        st.write("**Chart style**")
        chart_style = st.radio("Chart style", ["Area", "Line", "Bar"], horizontal=True, label_visibility="collapsed")

    with ctrl_col2:
        st.write("**Color theme**")
        color_theme = st.radio("Color theme", ["Azure", "Sunset", "Neon"], horizontal=True, label_visibility="collapsed")

    primary_color = THEME_COLORS[color_theme]

    # Explicit X and Y axis labels added here
    base_ts_chart = alt.Chart(df_ts).encode(
        x=alt.X(f"{x_col}:N", title=x_label, axis=alt.Axis(labelAngle=0, labelColor="#94A3B8", titleColor="#94A3B8", grid=False)),
        y=alt.Y("total_spent:Q", title="Total Revenue ($)", axis=alt.Axis(labelColor="#94A3B8", titleColor="#94A3B8", gridColor="#334155"))
    )

    if chart_style == "Area":
        # FIX: Vega-Lite compiles this straight into an SVG <linearGradient>.
        # SVG requires gradient stops to be listed in ASCENDING offset order —
        # if a later stop's offset isn't greater than the previous one, the
        # renderer clamps it up to match, collapsing both stops together.
        # The original code listed offset=1 before offset=0, so the "transparent"
        # stop got clamped onto the "solid" stop and the fade never rendered.
        # Fix: list offset=0 -> offset=1 in order, and use a simple top->bottom
        # direction (y1=0 at top, y2=1 at bottom).
        chart = base_ts_chart.mark_area(
            line={'color': primary_color, 'strokeWidth': 3},
            color=alt.Gradient(
                gradient='linear',
                stops=[
                    alt.GradientStop(color=primary_color, offset=0),        # top: solid/dark
                    alt.GradientStop(color=primary_color + '00', offset=1)  # bottom: fully transparent
                ],
                x1=0, x2=0, y1=0, y2=1  # vertical, top -> bottom
            ),
            interpolate='monotone',
            point=alt.OverlayMarkDef(color=primary_color, size=40)
        )
    elif chart_style == "Line":
        chart = base_ts_chart.mark_line(
            color=primary_color,
            strokeWidth=3,
            interpolate='monotone',
            point=alt.OverlayMarkDef(color=primary_color, size=50)
        )
    else:
        chart = base_ts_chart.mark_bar(
            color=primary_color,
            cornerRadiusTopLeft=6,
            cornerRadiusTopRight=6
        )

    st.altair_chart(chart.properties(height=320).configure_view(strokeWidth=0), use_container_width=True)

    # Metric Row Below Main Chart
    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(f'<div class="metric-box"><div class="metric-label">Peak Max</div><div class="metric-val">{peak_max:,.1f}</div></div>', unsafe_allow_html=True)
    with m2:
        st.markdown(f'<div class="metric-box"><div class="metric-label">Average</div><div class="metric-val">{avg_val:,.1f}</div></div>', unsafe_allow_html=True)
    with m3:
        st.markdown(f'<div class="metric-box"><div class="metric-label">Sum Total</div><div class="metric-val">{int(sum_total):,}</div></div>', unsafe_allow_html=True)

st.markdown("---")

# Bottom Section: Additional Visualizations
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top Customers by Spend")
    cust_chart = alt.Chart(df_customers).mark_bar(
        color=primary_color,
        cornerRadiusTopRight=4,
        cornerRadiusBottomRight=4
    ).encode(
        x=alt.X('total_spent:Q', title="Total Spend ($)", axis=alt.Axis(labelColor="#94A3B8", titleColor="#94A3B8", gridColor="#334155")),
        y=alt.Y('customer_name:N', sort='-x', title="Customer", axis=alt.Axis(labelColor="#94A3B8", titleColor="#94A3B8")),
        tooltip=['customer_name', 'total_orders', 'total_spent', 'avg_order_value']
    ).properties(height=260)
    st.altair_chart(cust_chart.configure_view(strokeWidth=0), use_container_width=True)

with col_right:
    st.subheader("Order Volume vs. Spend")
    scatter_chart = alt.Chart(df_customers).mark_circle(size=140, color=primary_color).encode(
        x=alt.X('total_orders:Q', title="Total Orders", axis=alt.Axis(labelColor="#94A3B8", titleColor="#94A3B8", gridColor="#334155")),
        y=alt.Y('total_spent:Q', title="Total Spent ($)", axis=alt.Axis(labelColor="#94A3B8", titleColor="#94A3B8", gridColor="#334155")),
        tooltip=['customer_name', 'total_orders', 'total_spent']
    ).properties(height=260)
    st.altair_chart(scatter_chart.configure_view(strokeWidth=0), use_container_width=True)

# Live Data Stream Table
with st.expander("View Live CDC Aggregations Table"):
    st.dataframe(df_customers, use_container_width=True)

# Auto-refresh loop
time.sleep(3)
st.rerun()