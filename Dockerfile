FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openvpn \
    cifs-utils \
    iputils-ping \
    net-tools \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x main.py && \
    chmod +x scripts/*.sh 2>/dev/null || true

# Default command
CMD ["python", "main.py"]

