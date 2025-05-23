# Dockerfile for Hanafund OCR  
# write by Jaedong, Oh (2025.05.22)
# Builder Image define 
# --- Builder stage ---
FROM python:3.12-slim-bullseye AS builder
WORKDIR /app

# 필수 도구만 설치
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv build-essential git locales && \
    python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .

# 필요 패키지 설치
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt 

# --- Inference image ---
FROM python:3.12-slim-bullseye
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /stock-service

COPY --from=builder /opt/venv /opt/venv
COPY . .