# Sử dụng Python 3.9 Slim (Nhẹ, tối ưu)
FROM python:3.9-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# 1. Cài đặt các gói hệ thống cần thiết và ODBC Driver 17 cho SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy file requirements và cài thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy code chính vào
COPY main.py .

# 4. Lệnh chạy mặc định
CMD ["python", "main.py"]