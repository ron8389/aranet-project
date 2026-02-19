#aranet_receiver.py
import json
import os
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# -------------------------
# Azure Blob 設定
# -------------------------
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "aranet-data")

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

try:
    container_client.create_container()
except Exception:
    pass

# -------------------------
# EventHub 設定
# -------------------------
EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

JST = timezone(timedelta(hours=9))

# -------------------------
# Parquet append（Blob 日別ファイル）
# -------------------------
def append_to_daily_parquet(row: dict):
    """1行の dict を日別 Parquet に追記する"""
    today_str = datetime.now(JST).strftime("%Y-%m-%d")
    blob_name = f"aranet_{today_str}.parquet"

    df_new = pd.DataFrame([row])

    blob_client = container_client.get_blob_client(blob_name)

    try:
        # 既存ファイルがあれば読み込んで結合
        data = blob_client.download_blob().readall()
        df_existing = pd.read_parquet(BytesIO(data))
        df_all = pd.concat([df_existing, df_new], ignore_index=True)
    except Exception:
        # 初回は新規作成
        df_all = df_new

    # 上書き保存
    buffer = BytesIO()
    df_all.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob_client.upload_blob(buffer, overwrite=True)
    print(f"[保存] {blob_name} に 1件追加")

# -------------------------
# EventHub 受信処理
# -------------------------
def on_event(partition_context, event):
    # まず生データを取得
    try:
        body_raw = event.body_as_str()
    except Exception as e:
        print(f"[無視] body_as_str() 取得エラー: {e}")
        return

    # JSON としてパースを試みる
    try:
        body = json.loads(body_raw)
    except Exception:
        print(f"[無視] JSON ではないイベントを受信: {body_raw}")
        return

    # dict 以外は無視（IoT Hub のシステムイベント対策）
    if not isinstance(body, dict):
        print(f"[無視] dict ではないイベントを受信: {body}")
        return

    # sensorId 抽出
    try:
        prop_key = list(event.properties.keys())[0]
        if isinstance(prop_key, bytes):
            prop_key = prop_key.decode("utf-8")
        sensorId = prop_key.split("/")[2]
    except Exception:
        sensorId = "unknown"

    # UNIX → JST
    try:
        unix_time = int(body.get("time"))
        time_jst = datetime.fromtimestamp(unix_time, tz=JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        time_jst = None

    # 行データ構築
    row = body.copy()
    row["sensorId"] = sensorId
    row["enqueuedTime"] = event.enqueued_time.isoformat()
    row["time_jst"] = time_jst

    # 保存
    append_to_daily_parquet(row)

    partition_context.update_checkpoint()

# -------------------------
# メインループ（常時稼働）
# -------------------------
def start_receiver():
    print("=== Aranet EventHub Receiver（B案）起動 ===")

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
            print("EventHub 受信エラー:", e)
            time.sleep(5)

if __name__ == "__main__":
    start_receiver()
