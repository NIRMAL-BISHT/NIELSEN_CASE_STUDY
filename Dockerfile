FROM apache/spark-py:latest


# Switch to root user to install Python packages


# Install cbsodata package
RUN pip install --no-cache-dir --user cbsodata



# Set the working directory inside the container
WORKDIR /app

# Copy your Spark app
COPY app.py .

# Run your app with spark-submit
CMD ["spark-submit", "app.py"]
