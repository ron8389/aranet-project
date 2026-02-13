import streamlit as st
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# -------------------------
# Blob 接続情報（環境変数から取得）
# -------------------------
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "aranet-data")
BLOB_FILE_NAME = "aranet.parquet"

# st.write("DEBUG: Streamlit 起動 OK")
# st.write("DEBUG: BLOB_CONNECTION_STRING =", os.getenv("BLOB_CONNECTION_STRING"))
# st.write("DEBUG: BLOB_CONTAINER_NAME =", os.getenv("BLOB_CONTAINER_NAME"))

st.title("Aranetセンサー トレンドダッシュボード")

FLASK_URL = os.getenv("FLASK_URL")

def test_blob_read():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))
        container_client = blob_service_client.get_container_client(os.getenv("BLOB_CONTAINER_NAME"))
        blob_client = container_client.get_blob_client("aranet.parquet")

        data = blob_client.download_blob().readall()
        # st.write(f"DEBUG: Blob 読み込み成功 {len(data)} bytes")
    except Exception as e:
        st.error("データの読み込みに失敗しました。しばらくしてから再度お試しください。")


test_blob_read()

# -------------------------
# Blob から Parquet を読み込む関数
# -------------------------
@st.cache_data(ttl=60)
def load_parquet_from_blob():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        blob_client = container_client.get_blob_client(BLOB_FILE_NAME)

        # Blob をダウンロード
        data = blob_client.download_blob().readall()

        # Parquet を DataFrame に変換
        df = pd.read_parquet(pd.io.common.BytesIO(data))
        return df

    except Exception as e:
        st.error(f"Blob 読み込みエラー: {e}")
        return pd.DataFrame()

# -------------------------
# Parquet 読み込み
# -------------------------
df = load_parquet_from_blob()

if df.empty:
    st.warning("Blob に Parquet ファイルがまだありません。Flask がデータを保存するまでお待ちください。")
    st.stop()

# -------------------------
# データ型の変換（必要な列だけ数値化）
# -------------------------
numeric_cols = [
    "temperature", "humidity", "co2", "atmosphericpressure",
    "ppfd", "bec", "vwc", "pec", "dp"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# time_jst を datetime に変換
if "time_jst" in df.columns:
    df["time_jst"] = pd.to_datetime(df["time_jst"], errors="coerce")

# -------------------------
# サイドバー UI
# -------------------------
st.sidebar.header("表示設定")
st.sidebar.markdown(f"[Flask API に戻る]({FLASK_URL})")

sensor_ids = sorted(df["sensorId"].unique())
selected_sensor = st.sidebar.selectbox("センサーIDを選択", sensor_ids)

df_sensor = df[df["sensorId"] == selected_sensor].sort_values("time_jst")

available_metrics = [col for col in numeric_cols if col in df_sensor.columns]

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

for metric in selected_metrics:
    st.subheader(metric_labels[metric])
    st.line_chart(df_sensor.set_index("time_jst")[metric], height=250)

st.success("グラフ描画が完了しました")