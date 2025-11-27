FROM ghcr.io/astral-sh/uv:python3.11-alpine
WORKDIR /app

# Install system dependencies (Alpine uses apk, not apt-get)
RUN apk add --no-cache \
    git \
    wget \
    unzip \
    gcc \
    g++ \
    make \
    musl-dev \
    python3-dev \
    linux-headers

# Download and extract the repository files
RUN mkdir utils && cd utils \
&& git init \
&& git remote add origin https://github.com/ATNoG/pei-nwdaf-comms.git \
&& git fetch \
&& git checkout origin/main kafka/src/kmw.py \
&& mv kafka/src/kmw.py . \
&& rm -rf .git \
&& rmdir -p kafka/src



# Install dependencies using uv
COPY requirements.txt ./
RUN uv pip install --system -r requirements.txt

COPY src/ ./src/
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8123"]
