FROM apache/spark-py:latest

# Install CBSodata and any other Python dependencies
RUN pip install --no-cache-dir --prefix=/usr/local cbsodata  

# Set the working directory inside the container
WORKDIR /app

# Copy your Spark app
COPY app.py .

# Run your app with spark-submit
CMD ["spark-submit", "app.py"]
