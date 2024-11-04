from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, sum, when, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tempfile
import os


# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkExample") \
    .master("local[*]") \
    .getOrCreate()




try:
    def save_df_to_temp(df, filename):
        # Use a fixed directory for output
        file_path = f"/output/{filename}"
        
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
