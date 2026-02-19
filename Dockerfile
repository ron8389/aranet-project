FROM python:3.11-slim

# 作業ディレクトリ
WORKDIR /app

# 依存パッケージをインストール
COPY flask_app/requirements.txt /app/flask_requirements.txt
COPY streamlit_app/requirements.txt /app/streamlit_requirements.txt
COPY collector/requirements.txt /app/collector_requirements.txt

RUN pip install --no-cache-dir -r flask_requirements.txt
RUN pip install --no-cache-dir -r streamlit_requirements.txt
RUN pip install --no-cache-dir -r collector_requirements.txt

# アプリケーションコードをコピー
COPY flask_app /app/flask_app
COPY streamlit_app /app/streamlit_app
COPY collector /app/collector

# ポート公開（Flask と Streamlit）
EXPOSE 5000
EXPOSE 8501

# MODE に応じて起動プロセスを切り替える
CMD if [ "$MODE" = "flask" ]; then \
        python flask_app/app.py; \
    elif [ "$MODE" = "streamlit" ]; then \
        streamlit run streamlit_app/dashboard.py --server.port=8501 --server.address=0.0.0.0; \
    elif [ "$MODE" = "collector" ]; then \
        python collector/aranet_receiver.py; \
    else \
        echo "Unknown MODE: $MODE" && exit 1; \
    fi