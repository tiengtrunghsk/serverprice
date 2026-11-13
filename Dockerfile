# Sử dụng Python bản nhẹ
FROM python:3.11-slim

# Cài đặt các gói hệ thống cần thiết (nếu cần cho thư viện)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Đặt thư mục làm việc
WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code
COPY . .

# Mở cổng 8080 (Google Cloud Run mặc định dùng cổng này)
EXPOSE 8080

# Chạy ứng dụng bằng gunicorn (thay 'main:app' nếu file của bạn tên khác)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
