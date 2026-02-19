#dashboard.py
import streamlit as st
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime, timedelta, date

# -------------------------
# Blob 接続情報
# -------------------------
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "aranet-data")

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

st.title("Aranet センサー トレンドダッシュボード（期間選択対応）")

FLASK_URL = os.getenv("FLASK_URL")

# -------------------------
# Blob の日別ファイル一覧を取得
# -------------------------
def list_daily_parquet_files():
    blobs = container_client.list_blobs()
    files = []
    for b in blobs:
        if b.name.startswith("aranet_") and b.name.endswith(".parquet"):
            files.append(b.name)
    return sorted(files)

# -------------------------
# Parquet 読み込み
# -------------------------
@st.cache_data(ttl=60)
def load_parquet(file_name):
    blob_client = container_client.get_blob_client(file_name)
    data = blob_client.download_blob().readall()
    return pd.read_parquet(BytesIO(data))

# -------------------------
# 指定期間のファイルを読み込む
# -------------------------
def load_period(start_date, end_date):
    all_files = list_daily_parquet_files()
    target_files = []

    for f in all_files:
        # ファイル名から日付を抽出
        try:
            date_str = f.replace("aranet_", "").replace(".parquet", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            continue

        if start_date <= file_date <= end_date:
            target_files.append(f)

    if not target_files:
        return pd.DataFrame()

    dfs = [load_parquet(f) for f in target_files]
    return pd.concat(dfs, ignore_index=True)

# -------------------------
# 期間選択 UI
# -------------------------
st.sidebar.header("期間選択")

preset = st.sidebar.selectbox(
    "期間プリセット",
    ["1日", "1か月", "3か月", "6か月", "任意期間"]
)

today = date.today()

if preset == "1日":
    start_date = today
    end_date = today

elif preset == "1か月":
    start_date = today - timedelta(days=30)
    end_date = today

elif preset == "3か月":
    start_date = today - timedelta(days=90)
    end_date = today

elif preset == "6か月":
    start_date = today - timedelta(days=180)
    end_date = today

else:
    st.sidebar.write("任意期間を選択してください")
    start_date = st.sidebar.date_input("開始日", today - timedelta(days=7))
    end_date = st.sidebar.date_input("終了日", today)

    if start_date > end_date:
        st.sidebar.error("開始日は終了日より前にしてください")
        st.stop()

# -------------------------
# データ読み込み
# -------------------------
df = load_period(start_date, end_date)

if df.empty:
    st.warning("指定期間のデータがありません。")
    st.stop()

# -------------------------
# データ型変換
# -------------------------
numeric_cols = [
    "temperature", "humidity", "co2", "atmosphericpressure",
    "ppfd", "bec", "vwc", "pec", "dp"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

if "time_jst" in df.columns:
    df["time_jst"] = pd.to_datetime(df["time_jst"], errors="coerce")

# -------------------------
# センサー選択
# -------------------------
st.sidebar.header("表示設定")
st.sidebar.markdown(f"[Flask API に戻る]({FLASK_URL})")

sensor_ids = sorted(df["sensorId"].unique())
selected_sensor = st.sidebar.selectbox("センサーIDを選択", sensor_ids)

df_sensor = df[df["sensorId"] == selected_sensor].sort_values("time_jst")

# -------------------------
# 表示項目選択
# -------------------------
metric_labels = {
    "temperature": "温度 (°C)",
    "humidity": "湿度 (%)",
    "co2": "CO₂ (ppm)",
    "atmosphericpressure": "気圧 (Pa)",
    "ppfd": "光量 PPFD",
    "bec": "土壌EC (BEC)",
    "vwc": "体積含水率 (VWC)",
    "pec": "電気伝導率 (PEC)",
    "dp": "露点 (DP)"
}

available_metrics = [col for col in numeric_cols if col in df_sensor.columns]
display_names = [metric_labels[m] for m in available_metrics]

selected_display = st.sidebar.multiselect(
    "表示する項目を選択",
    display_names,
    default=display_names[:1]
)

selected_metrics = [key for key, label in metric_labels.items() if label in selected_display]

# -------------------------
# メイン画面
# -------------------------
st.subheader(f"センサー {selected_sensor} のデータ件数: {len(df_sensor)}")
st.caption(f"期間: {start_date} 〜 {end_date}")

for metric in selected_metrics:
    st.subheader(metric_labels[metric])
    st.line_chart(df_sensor.set_index("time_jst")[metric], height=250)

st.success("グラフ描画が完了しました")