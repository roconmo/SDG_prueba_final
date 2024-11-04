FROM python:3.9-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
RUN pip install pyspark==3.5.0

# Set working directory
WORKDIR /app

# Copy your Spark script
COPY spark_script.py /app/

RUN mkdir -p /output

# Run the Spark script
CMD ["python", "spark_script.py"]
