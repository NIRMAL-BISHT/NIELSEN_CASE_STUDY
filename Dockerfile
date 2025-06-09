 FROM bitnami/spark:latest


# Switch to root user to install Python packages
USER root

# Install cbsodata package
RUN pip install --no-cache-dir cbsodata



# Set the working directory inside the container
WORKDIR /app

# Copy your Spark app
COPY app.py .

# Run your app with spark-submit
CMD ["spark-submit", "app.py"]
