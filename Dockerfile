FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    git \
    subversion \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt
RUN svn checkout https://github.com/ATNoG/pei-nwdaf-comms/trunk/kafka/src utils
COPY src/ ./src/
COPY main.py .
COPY .env .
COPY utils ./utils/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9123"]
