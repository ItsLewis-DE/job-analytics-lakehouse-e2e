#!/bin/bash

# Dừng script nếu có lỗi xảy ra
set -e

# Chuyển context về thư mục root của project
cd "$(dirname "$0")/../.."

echo "======================================"
echo "BẮT ĐẦU BUILD DOCKER IMAGES CHO AIRFLOW"
echo "======================================"

echo "1/4: Đang build image 'job-crawler:latest'..."
docker build -t job-crawler:latest -f config/dockerfile/Dockerfile.crawler .

echo "2/4: Đang build image 'job-spark:latest'..."
docker build -t job-spark:latest -f config/dockerfile/Dockerfile.spark .

echo "3/4: Đang build image 'job-standard:latest'..."
docker build -t job-standard:latest -f config/dockerfile/Dockerfile.standard .

echo "4/5: Đang build image 'job-gold:latest'..."
docker build -t job-gold:latest -f config/dockerfile/Dockerfile.gold .

echo "5/5: Đang build image 'job-bot:latest'..."
docker build -t job-bot:latest -f config/dockerfile/Dockerfile.bot .

echo "======================================"
echo "HOÀN TẤT BUILD TẤT CẢ IMAGES!"
echo "Các image hiện có trong local registry:"
docker images | grep "job-"
echo "======================================"
