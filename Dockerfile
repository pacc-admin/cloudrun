# QUAN TRỌNG: Dùng slim-bookworm để cố định Debian 12 (tránh lỗi Trixie/Debian 13)
FROM python:3.10-slim-bookworm

# 1. Cài đặt các gói phụ thuộc
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc-dev \
    ca-certificates \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# 2. Cài đặt Driver MSSQL (Sửa đường dẫn key cho đúng chuẩn Debian 12)
# Lưu ý phần -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17

WORKDIR /app

# 3. Cài đặt thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy mã nguồn
COPY . .

CMD ["python", "main.py"]