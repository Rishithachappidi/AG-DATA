import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Large-Scale Agricultural Market Data & Real-Time Analysis", layout="wide", initial_sidebar_state="expanded")

# Inject Premium Dark Mode Glassmorphism CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background: radial-gradient(circle at 10% 20%, rgb(20, 20, 30) 0%, rgb(10, 10, 15) 90%); color: #e0e0e0; }
    h1 { background: -webkit-linear-gradient(45deg, #42a5f5, #00e676); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 700; letter-spacing: -0.5px; }
    h2, h3 { color: #b0bec5; font-weight: 600; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; color: #00e676; }
    div[data-testid="metric-container"] { background: rgba(255, 255, 255, 0.03); border-radius: 12px; padding: 15px; border: 1px solid rgba(255, 255, 255, 0.1); box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3); backdrop-filter: blur(10px); transition: transform 0.2s ease-in-out; }
    div[data-testid="metric-container"]:hover { transform: translateY(-5px); box-shadow: 0 8px 15px rgba(0, 230, 118, 0.2); }
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { background-color: transparent !important; border-radius: 4px 4px 0px 0px; color: #90a4ae !important; font-weight: 600; padding: 10px 20px; }
    .stTabs [aria-selected="true"] { color: #42a5f5 !important; border-bottom: 2px solid #42a5f5; }
    hr { border-top: 1px solid rgba(255,255,255,0.1); }
</style>
""", unsafe_allow_html=True)

count = st_autorefresh(interval=5000, limit=None, key="data_refresh")

@st.cache_resource
def get_spark():
    spark = SparkSession.builder.appName("StreamlitDashboard") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@st.cache_data(ttl=60)
def load_analytics(path):
    spark = get_spark()
    try:
        return spark.read.parquet(f"hdfs://namenode:9000/agri-market/analytics/{path}").toPandas()
    except Exception as e:
        import streamlit as st
        st.error(f"Failed to load {path}: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=5)
def load_streaming_data():
    spark = get_spark()
    try:
        df = spark.read.parquet("hdfs://namenode:9000/agri/streaming/real_anomaly_predictions")
        return df.toPandas()
    except Exception as e:
        import streamlit as st
        st.error(f"Failed to load streaming data: {str(e)}")
        return pd.DataFrame()

st.title("Large-Scale Agricultural Market Data & Real-Time Analysis")
st.markdown("**Data Source: Daily Market Prices + India Mandi**")

tab1, tab2 = st.tabs(["HISTORICAL MARKET ANALYSIS", "REAL-TIME MARKET ANALYSIS"])

with tab1:
    st.subheader("Data & Architecture Provenance")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("REAL RECORDS PROCESSED", "132.9M")
    col1.caption("Canonical records: 93,410,019 rows")
    
    col2.metric("REAL DATA VOLUME", "10.9 GB")
    col2.caption("Synthetic Scale Workload: ≈ 7.1 GB\nTotal Experimental Workload: ≈ 18 GB")
    
    col3.metric("HDFS CLUSTER (Storage)", "1 NN + 3 DN")
    col3.caption("Replication Factor: 3")
    
    col4.metric("STREAMING (Event Transport)", "Kafka → Spark")
    col4.caption("Computation: Spark Structured Streaming\nModel: Spark MLlib Random Forest")
    
    st.divider()
    st.subheader("Anomaly Detection (Model Performance)")
    st.info("Final evaluation uses a deterministic 1% entity-safe representative sample with price deviation features evaluated on an untouched 2025+ temporal test set (48,354 records).")
    
    mcol1, mcol2, mcol3, mcol4, mcol5, mcol6, mcol7 = st.columns(7)
    mcol1.metric("Accuracy", "95.66%")
    mcol2.metric("Precision", "86.55%")
    mcol3.metric("Recall", "85.47%")
    mcol4.metric("F1 Score", "86.01%")
    mcol5.metric("FPR", "2.46%")
    mcol6.metric("ROC-AUC", "94.82%")
    mcol7.metric("PR-AUC", "86.15%")
    
    st.divider()
    st.subheader("Price Analysis (Real Data Aggregations)")
    
    df_comm = load_analytics("commodity_statistics")
    df_state = load_analytics("state_statistics")
    df_price = load_analytics("price_statistics")
    
    st.sidebar.header("Historical Filters")
    sel_comm = st.sidebar.multiselect("Filter Commodities", options=df_comm['commodity'].dropna().unique() if not df_comm.empty else [])
    sel_state = st.sidebar.multiselect("Filter States", options=df_state['state'].dropna().unique() if not df_state.empty else [])
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("##### Top States by Record Count")
        if not df_state.empty:
            filt_state = df_state[df_state['state'].isin(sel_state)] if sel_state else df_state
            st.bar_chart(filt_state.set_index('state').sort_values('record_count', ascending=False).head(10))
    with c2:
        st.markdown("##### Top Commodities by Record Count")
        if not df_comm.empty:
            filt_comm = df_comm[df_comm['commodity'].isin(sel_comm)] if sel_comm else df_comm
            st.bar_chart(filt_comm.set_index('commodity').sort_values('record_count', ascending=False).head(10))
            
    st.markdown("##### Commodity Price Statistics")
    if not df_price.empty:
        filt_price = df_price[df_price['commodity'].isin(sel_comm)] if sel_comm else df_price
        st.dataframe(filt_price.head(50), use_container_width=True)


with tab2:
    st.subheader("REAL-DATA STREAM REPLAY")
    stream_df = load_streaming_data()
    
    if stream_df.empty:
        st.warning("No real-time data found on HDFS.")
    else:
        time_col = 'timestamp' if 'timestamp' in stream_df.columns else 'arrival_date'
        stream_df[time_col] = pd.to_datetime(stream_df[time_col])
        
        st.sidebar.header("Live Filters")
        l_state = st.sidebar.multiselect("State (Live)", options=stream_df['state'].unique())
        l_comm = st.sidebar.multiselect("Commodity (Live)", options=stream_df['commodity'].unique())
        
        f_df = stream_df.copy()
        if l_state: f_df = f_df[f_df['state'].isin(l_state)]
        if l_comm: f_df = f_df[f_df['commodity'].isin(l_comm)]
        
        total_preds = len(f_df)
        total_anomalies = f_df['anomaly_prediction'].sum()
        pct = (total_anomalies / total_preds * 100) if total_preds > 0 else 0
        latest_ts = f_df[time_col].max() if not f_df.empty else "N/A"
        latest_prob = f_df.sort_values(time_col).iloc[-1]['anomaly_probability'] if not f_df.empty and 'anomaly_probability' in f_df.columns else "N/A"
        
        scol1, scol2, scol3, scol4, scol5 = st.columns(5)
        scol1.metric("Real Events Processed", total_preds)
        scol2.metric("Real Anomalies Detected", int(total_anomalies))
        scol3.metric("Anomaly %", f"{pct:.2f}%")
        scol4.metric("Latest Real Event", str(latest_ts).split(".")[0])
        scol5.metric("Latest Probability", f"{latest_prob:.2f}" if isinstance(latest_prob, float) else latest_prob)
        
        st.divider()
        lc1, lc2 = st.columns(2)
        with lc1:
            st.markdown("##### Top Anomalous Commodities")
            anoms = f_df[f_df['anomaly_prediction'] == 1.0]
            if not anoms.empty:
                st.bar_chart(anoms['commodity'].value_counts().head(10))
            else:
                st.info("No anomalies")
        with lc2:
            st.markdown("##### Top Anomalous Markets")
            if not anoms.empty:
                st.bar_chart(anoms['market'].value_counts().head(10))
            else:
                st.info("No anomalies")
                
        st.markdown("##### Recent Alerts (Real Data)")
        def hl(val): return 'background-color: #ffcccc' if val == 1.0 else ''
        cols = [c for c in [time_col, 'state', 'district', 'market', 'commodity', 'variety', 'modal_price', 'anomaly_probability', 'anomaly_prediction'] if c in f_df.columns]
        st.dataframe(f_df[cols].sort_values(time_col, ascending=False).head(50).style.applymap(hl, subset=['anomaly_prediction']), use_container_width=True)
