FROM apache/spark-py:latest


# Switch to root user to install Python packages
USER root

# Set HOME to /tmp to avoid permission issues
ENV HOME=/tmp

# Install cbsodata package
RUN pip install --no-cache-dir cbsodata

# Switch back to default user (usually 'spark' in this image)
USER spark

# Set the working directory inside the container
WORKDIR /app

# Copy your Spark app
COPY app.py .

# Run your app with spark-submit
CMD ["spark-submit", "app.py"]
