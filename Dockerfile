FROM python:3.11-slim

# 作業ディレクトリ
WORKDIR /app

# 依存パッケージをまとめてインストール
COPY flask_app/requirements.txt /app/flask_requirements.txt
COPY streamlit_app/requirements.txt /app/streamlit_requirements.txt

RUN pip install --no-cache-dir -r flask_requirements.txt
RUN pip install --no-cache-dir -r streamlit_requirements.txt

# アプリケーションコードをコピー
COPY flask_app /app/flask_app
COPY streamlit_app /app/streamlit_app

# データ保存用ディレクトリ
RUN mkdir -p /data

# MODE に応じて起動するエントリポイント
CMD if [ "$MODE" = "flask" ]; then \
        python flask_app/aranet_receiver.py; \
    else \
        streamlit run streamlit_app/dashboard.py --server.port 8501 --server.address 0.0.0.0; \
    fi