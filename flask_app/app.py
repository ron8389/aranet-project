#app.py
from flask import Flask
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import os
from datetime import datetime

app = Flask(__name__)

# -------------------------
# Blob 接続情報
# -------------------------
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "aranet-data")
STREAMLIT_URL = os.getenv("STREAMLIT_URL")

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

# -------------------------
# 最新の日付の Parquet ファイルを取得
# -------------------------
def get_latest_parquet():
    blobs = container_client.list_blobs()

    parquet_files = []
    for b in blobs:
        if b.name.startswith("aranet_") and b.name.endswith(".parquet"):
            parquet_files.append(b.name)

    if not parquet_files:
        return None

    # 日付順にソート
    parquet_files.sort()
    return parquet_files[-1]  # 最新ファイル

# -------------------------
# Blob から Parquet を読み込む
# -------------------------
def load_parquet(file_name):
    blob_client = container_client.get_blob_client(file_name)
    data = blob_client.download_blob().readall()
    return pd.read_parquet(BytesIO(data))

# -------------------------
# Flask ルート
# -------------------------
@app.route("/")
def index():
    latest_file = get_latest_parquet()

    if latest_file is None:
        return f"""
        <html>
        <body>
            <h1>まだデータがありません</h1>
            <p>collector が日別 Parquet を保存するまでお待ちください。</p>
            <p><a href="{STREAMLIT_URL}">▶ Streamlit ダッシュボードへ</a></p>
        </body>
        </html>
        """

    df = load_parquet(latest_file)
    latest10 = df.tail(10)

    table_html = latest10.to_html(classes="table table-striped", index=False)

    return f"""
    <html>
    <body>
        <h1>最新10件のセンサーデータ</h1>
        <p>ファイル: {latest_file}</p>
        {table_html}
        <hr>
        <p><a href="{STREAMLIT_URL}">▶ Streamlit ダッシュボードへ</a></p>
    </body>
    </html>
    """

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)