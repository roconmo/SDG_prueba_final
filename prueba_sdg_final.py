import os
import subprocess
import traceback
from pathlib import Path
import time

def create_docker_files():
    # Create project directory structure
    project_dir = Path("spark_docker_project")
    project_dir.mkdir(exist_ok=True)

    # Create Dockerfile with fixed package installation
    dockerfile_content = '''FROM python:3.9-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install required packages
RUN apt-get update && \\
    apt-get install -y --no-install-recommends \\
        wget \\
        ca-certificates \\
        default-jre && \\
    apt-get clean && \\
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
'''

    # Create Spark script
    spark_script_content = '''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, sum, when, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tempfile
import os

# Initialize Spark session
spark = SparkSession.builder \\
    .appName("SparkExample") \\
    .master("local[*]") \\
    .getOrCreate()

try:
    def save_df_to_temp(df, filename):
        output_path = "/app/returns"  # Path inside the container
    
        # Use a fixed directory for output
        file_path = f"/{output_path}/{filename}"
        
        df.write.json(f"{output_path}/your_output_file.json")
        
        # Save the DataFrame as a single JSON file
        df.coalesce(1).write.mode("overwrite").json(file_path + "_temp")
        
        # Locate and move the JSON file to the fixed output directory
        import glob
        json_file = glob.glob(file_path + "_temp/*.json")[0]
        os.rename(json_file, file_path + ".json")
        
        # Clean up the temporary Spark directory
        import shutil
        shutil.rmtree(file_path + "_temp")
        
        return file_path + ".json"

    ##########################
    # Input data
    inputs = [
        {"name": "Xabier", "age": 39, "office": ""},     # "office" is empty
        {"name": "Miguel", "age": None, "office": "RIO"}, # "age" is null
        {"name": "Fran", "age": 31, "office": "RIO"}      # Valid case
    ]
    
    schema = { 
        "dataflows": [
            {
                "name": "prueba-acceso",
                "transformations": [
                    {
                        "name": "validation",
                        "type": "validate_fields",
                        "params": {
                            "input": "person_inputs",
                            "validations": [
                                {
                                    "field": "office",
                                    "validations": [
                                        "notEmpty"
                                    ]
                                },
                                {
                                    "field": "age",
                                    "validations": [
                                        "notNull"
                                    ]
                                }
                            ]
                        }
                    },
                    {
                        "name": "ok_with_date",
                        "type": "add_fields",
                        "params": {
                            "input": "validation_ok",
                            "addFields": [
                                {
                                    "name": "dt",
                                    "function": "current_timestamp"
                                }
                            ]
                        }
                    }
                ],
                "sinks": [
                    {
                        "input": "ok_with_date",
                        "name": "raw-ok",
                        "format": "JSON"
                    },
                    {
                        "input": "validation_ko",
                        "name": "raw-ko",
                        "format": "JSON"
                    }
                ]
            }
        ]
    }
    
    # Define schema for DataFrame based on input data structure
    data_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("office", StringType(), True)
    ])
    
    # Create DataFrame from input data
    data_df = spark.createDataFrame(inputs, schema=data_schema)
    
    # Extract validation rules from schema
    validations = schema["dataflows"][0]["transformations"][0]["params"]["validations"]
    
    # Initialize error columns
    for validation in validations:
        field = validation["field"]
        data_df = data_df.withColumn(f"{field}_error", lit(""))
        
    # Apply validation rules to the DataFrame
    for validation in validations:
        field = validation["field"]
        rules = validation["validations"]
    
        # Apply "notEmpty" validation
        if "notEmpty" in rules:
            data_df = data_df.withColumn(f"{field}_error", when((col(field).isNull()) | (col(field) == ""), "Campo vacio").otherwise(""))
    
        # Apply "notNull" validation
        if "notNull" in rules:
            data_df = data_df.withColumn(f"{field}_error", when(col(field).isNull(), "Campo nulo").otherwise(col(f"{field}_error")))

    # Separate valid and invalid records
    error_columns = [f"{validation['field']}_error" for validation in validations]
    valid_df = data_df.filter(" AND ".join([f"{col} == ''" for col in error_columns]))
    invalid_df = data_df.filter(" OR ".join([f"{col} != ''" for col in error_columns]))
    
    # Add timestamp to valid records if specified in schema
    if "ok_with_date" in [t["name"] for t in schema["dataflows"][0]["transformations"]]:
        valid_df = valid_df.withColumn("dt", current_timestamp())
    
    valid_df = valid_df.drop("office_error", "age_error")
    #valid_df = valid_df.drop("age_error")
    
    # Output results
    save_df_to_temp(valid_df, "STANDARD_OK")
    save_df_to_temp(invalid_df, "STANDARD_KO")
    print("STANDARD_OK.json and STANDARD_KO.json created in /output directory")

    
    # print("Valid Records:")
    valid_df.show(truncate=False)
    
    # print("Invalid Records:")
    invalid_df.show(truncate=False)
    
finally:
    # Stop Spark session
    spark.stop()
'''

    # Write files
    with open(project_dir / "Dockerfile", "w") as f:
        f.write(dockerfile_content)

    with open(project_dir / "spark_script.py", "w") as f:
        f.write(spark_script_content)

    return project_dir

def build_and_run_docker():
    try:
        # Build Docker image
        print("Building Docker image...")
        subprocess.run(["docker", "build", "--no-cache", "-t", "spark-analysis", "."], check=True)
        
        # Verify output directory exists on the host
        host_results_dir = Path(r"C:\Users\adm_rosalia.contre\Documents\SDG_spark_docker_project\results")
        host_results_dir.mkdir(parents=True, exist_ok=True)

        # Run Docker container in interactive mode
        print("\nRunning Spark analysis...")
        subprocess.run([
            "docker", "run", "--name", "spark-analysis-container", "-d", 
            "spark-analysis"
        ], check=True)

        # Check logs to confirm files were created
        subprocess.run(["docker", "logs", "spark-analysis-container"])
        
        # Inspect files inside the container
        subprocess.run(["docker", "exec", "-it", "spark-analysis-container", "ls", "/output"])

    except subprocess.CalledProcessError as e:
        print(f"Error executing Docker commands: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        
        
def copy_results_to_host():
    container_paths = [
        "/output/STANDARD_OK.json",
        "/output/STANDARD_KO.json"
    ]
    host_destination = r"C:\Users\adm_rosalia.contre\Documents\SDG_spark_docker_project\results"

    for file_path in container_paths:
        subprocess.run([
            "docker", "cp",
            f"spark-analysis-container:{file_path}",
            host_destination
        ], check=True)
    print("Files successfully copied to host")
    


#function that stops the running container, removes the container and removes the image
def cleanup_docker_resources():
    try:
        print("\nLimpieza de recursos Docker...")

        container_id = subprocess.run(
            ["docker", "ps", "-a", "-q", "-f", "name=spark-analysis-container"],
            capture_output=True,
            text=True
        ).stdout.strip()
        
        print(container_id)

        if container_id:
            print(f"ID del contenedor encontrado: {container_id}")
            subprocess.run(["docker", "rm", "-f", container_id], check=False)

        image_exists = subprocess.run(
            ["docker", "images", "-q", "spark-analysis"],
            capture_output=True,
            text=True
        ).stdout.strip()

        if image_exists:
            print("Imagen encontrada, eliminando...")
            subprocess.run(["docker", "rmi", "-f", "spark-analysis"], check=False)
            print("Imagen eliminada con éxito.")
        else:
            print("No se encontró imagen para eliminar.")

        print("\n¡Limpieza completada!")
        
    except Exception as e:
        print(f"An unexpected error occurred during cleanup: {e}")
        print("\nIf the container still exists, try these manual commands:")
        print("1. sudo docker rm -f spark-analysis-container")
        print("2. sudo systemctl restart docker")
        print("3. sudo docker system prune -f")

def main():
    try:
        # Create project directory and files
        project_dir = create_docker_files()

        # Change to project directory
        os.chdir(project_dir)

        # Build and run Docker container
        build_and_run_docker()
        
        # Add a delay to ensure Spark job completion
        time.sleep(7)  # Adjust time if necessary
        
        # Copy results from container to local directory
        copy_results_to_host()
        
        # # Clean up Docker resources
        # cleanup_docker_resources()

        print("\nProcess completed!")
    except Exception as e:
        # Write error details to a log file
        error_message = f"An error occurred: {e}\n"
        error_message += traceback.format_exc()  # Get full traceback for detailed error info
        with open("error_log.txt", "w") as error_file:
            error_file.write(error_message)
        print("Error logged to error_log.txt")
    finally:
        # Clean up Docker resources
        cleanup_docker_resources()
        print("\nProcess completed with cleanup.")


if __name__ == "__main__":
    main()
