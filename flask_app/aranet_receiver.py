from flask import Flask, redirect
from azure.eventhub import EventHubConsumerClient
import pandas as pd
import json
import threading
import time
import os
import shutil
from datetime import datetime, timezone, timedelta

from azure.storage.blob import BlobServiceClient

# Blob 接続情報（環境変数から取得）
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "aranet-data")

# Blob クライアント初期化
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

# コンテナが無ければ作成
try:
    container_client.create_container()
except Exception:
    pass

JST = timezone(timedelta(hours=9))

app = Flask(__name__)

EVENTHUB_CONNECTION_STRING = "Endpoint=sb://ihsuprodkwres040dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=SY6GyXo3ryAPDKKQFXDGmU+yTmRR3TTPUAIoTPUuu/s=;EntityPath=iothub-ehub-hironori-i-56578767-929b67e183"
EVENTHUB_NAME = "iothub-ehub-hironori-i-56578767-929b67e183"

# -------------------------
# 起動時に Blob から Parquet を読み込む
# -------------------------
try:
    blob_client = container_client.get_blob_client("aranet.parquet")
    data = blob_client.download_blob().readall()
    df = pd.read_parquet(pd.io.common.BytesIO(data))
    print("起動時に Blob の Parquet を読み込みました:", len(df), "件")
except Exception as e:
    print("Blob Parquet 読み込みエラー:", e)
    df = pd.DataFrame()


# -------------------------
# EventHub 受信処理
# -------------------------
def on_event(partition_context, event):
    global df

    try:
        body = json.loads(event.body_as_str())
 #       print("受信:", body)
    except:
        return

    # sensorId 抽出
    try:
        prop_key = list(event.properties.keys())[0]

        # ★ bytes → str に変換
        if isinstance(prop_key, bytes):
            prop_key = prop_key.decode("utf-8")

        sensorId = prop_key.split("/")[2]

    except Exception as e:
        print("sensorId 抽出エラー:", e)
        sensorId = "unknown"


    # UNIX → JST 変換
    try:
        unix_time = int(body.get("time"))
        time_jst = datetime.fromtimestamp(unix_time, tz=JST).strftime("%Y-%m-%d %H:%M:%S")
    except:
        time_jst = None

    # 行データ構築
    row = body.copy()
    row["sensorId"] = sensorId
    row["enqueuedTime"] = event.enqueued_time.isoformat()
    row["time_jst"] = time_jst

    # DataFrame に追加
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    partition_context.update_checkpoint()

def start_eventhub_receiver():
    print("EventHub 受信スレッド開始")

    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        consumer_group="$Default",
        eventhub_name=EVENTHUB_NAME
    )

    while True:
        try:
            client.receive(
                on_event=on_event,
                starting_position="-1"
            )

        except Exception as e:
            print("EventHub 受信エラー:", repr(e))
            time.sleep(5)

# -------------------------
# Parquet 保存（1時間ごと + バックアップ）
# -------------------------
def save_parquet_hourly():
    global df
    while True:
        time.sleep(3600)  # 10分ごとの場合は600秒（本番は3600秒）

        try:
            # DataFrame を文字列化して Parquet 保存（型不整合対策）
            df_clean = df.astype(str)

            # 一時ファイルとしてローカルに保存
            tmp_path = "aranet_tmp.parquet"
            df_clean.to_parquet(tmp_path)

            # Blob 名（例: aranet.parquet）
            blob_main = "aranet.parquet"
            blob_backup = "aranet_backup.parquet"

            # 既存ファイルがあればバックアップとしてコピー
            try:
                # 既存のメインをダウンロードしてバックアップとしてアップロード
                existing = container_client.get_blob_client(blob_main)
                backup = container_client.get_blob_client(blob_backup)

                data = existing.download_blob().readall()
                backup.upload_blob(data, overwrite=True)
                print("Blob バックアップ作成: aranet_backup.parquet")

            except Exception:
                print("バックアップ対象がまだ存在しません")

            # 新しい Parquet をアップロード
            with open(tmp_path, "rb") as f:
                container_client.upload_blob(blob_main, f, overwrite=True)

            print("Blob への Parquet 保存完了")
            df = pd.DataFrame()   # メモリ解放

            # 一時ファイル削除
            os.remove(tmp_path)

        except Exception as e:
            print("Blob 保存エラー:", e)

# -------------------------
# Flask 表示
# -------------------------

STREAMLIT_URL = os.getenv("STREAMLIT_URL")

@app.route("/")
def index():
    global df

    if df.empty:
        return f"""
        <html>
        <head>
            <meta http-equiv="refresh" content="60">
        </head>
        <body>
            <h1>まだデータがありません</h1>
            <p><a href="{STREAMLIT_URL}">▶ Streamlit ダッシュボードへ移動</a></p>
        </body>
        </html>
        """

    latest10 = df.tail(10)
    table_html = latest10.to_html(classes="table table-striped", index=False)

    return f"""
    <html>
    <head>
        <meta http-equiv="refresh" content="60">
    </head>
    <body>
        <h1>最新10件のセンサーデータ</h1>
        {table_html}
        <hr>
        <p><a href="{STREAMLIT_URL}">▶ Streamlit ダッシュボードへ移動</a></p>
    </body>
    </html>
    """

def start_background_threads():
    t1 = threading.Thread(target=start_eventhub_receiver, daemon=True)
    t1.start()

    t2 = threading.Thread(target=save_parquet_hourly, daemon=True)
    t2.start()

# gunicorn で import されたときにも実行される
start_background_threads()
