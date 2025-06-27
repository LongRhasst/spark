from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, concat_ws, udf
import uuid
import sys # Import sys to access command-line arguments

def rename_columns(data):
    # Rename columns to match the desired schema
    renamed_data = data.withColumnRenamed('name', 'Dat_nuoc') \
        .withColumnRenamed('independent', 'Doc_lap') \
        .withColumnRenamed('status', 'Trang_thai') \
        .withColumnRenamed('capital', 'Thu_do') \
        .withColumnRenamed('altSpellings', 'Ten_thay_the') \
        .withColumnRenamed('region', 'Khu_vuc') \
        .withColumnRenamed('area', 'dien_tich') \
        .withColumnRenamed('maps', 'ban_do') \
        .withColumnRenamed('timezones', 'Mui_gio') \
        .withColumnRenamed('continents', 'Luc_dia') \
        .withColumnRenamed('flags', 'Quoc_ky') \
        .withColumnRenamed('startOfWeek', 'Ngay_bat_dau_tuan') \
    
    # Save the transformed data as Parquet
    return renamed_data

def process_data(data):
    #join | to each element of altSpelling and captital
    data = data.withColumn(
        'altSpellings',
        concat_ws('|', col('altSpellings'))
    )
    
    #join | to each element of capital
    data = data.withColumn(
        'capital',
        concat_ws('|', col('capital'))
    )
    
    #change latlng element to lat and lng
    data = data.withColumn("lat", col("latlng").getItem(0).cast("double"))
    data = data.withColumn("lng", col("latlng").getItem(1).cast("double"))
    
    #create a new column 'su_dung_tieng_anh' based on the language element has eng
    data = data.withColumn(
        'su_dung_tieng_anh',
        col('languages').getItem('eng').isNotNull()
    )
    
    #convert all 'translation' elements to columns
    # Ensure translations column exists and is a struct before trying to select from it
    if 'translations' in data.columns and isinstance(data.schema['translations'].dataType, types.StructType):
        for trans_field in data.schema['translations'].dataType.fields:
            trans = trans_field.name
            data = data.withColumn(
                f'{trans}',
                col('translations').getField(trans)
            )
    
    data = data.withColumn(
        'Google_Maps',
        col('maps').getField('googleMaps')
    )
    
    data = data.withColumn(
        'timezone_1',
        col('timezones').getItem(0)
    )
    
    data = data.withColumn(
        'svg_flags',
        col('flags').getField('svg')
    )

    return data
MY_NAMESPACE = uuid.UUID("9a5963f8-5a5c-4b8c-aa46-1068af074546")

def generate_uuid5(name):
    try:
        if not name:
            return None
        return str(uuid.uuid5(MY_NAMESPACE, str(name)))
    except Exception as e:
        # Consider logging the error e
        return None

def transform_data(input_path, output_path):
    # Explicitly set the master URL to connect to the Spark cluster
    spark = SparkSession.builder \
        .appName('TransformData') \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    data = spark.read.parquet(input_path)
    
    # Select columns for processing data
    # Ensure all selected columns exist in the DataFrame read from parquet
    # It's safer to select columns that are known to exist or check schema first
    required_columns = ['name', 'ccn3', 'cca3', 'latlng', 'independent', 'status', 'translations',
        'capital', 'altSpellings', 'languages', 'region','area','maps','timezones','continents','flags','startOfWeek']
    
    # Filter out columns that are not in the DataFrame to avoid errors
    select_cols = [c for c in required_columns if c in data.columns]
    data_selected = data.select(*select_cols) # Use unpacking for the list of columns

    # Create name1 from existing name struct, assuming it has common and official fields
    if 'name' in data_selected.columns and isinstance(data_selected.schema['name'].dataType, types.StructType):
        data_with_name1 = data_selected.withColumn(
            'name1',
            concat_ws(' - ', col('name.common'), col('name.official'))
        )
    else: # Fallback if name is not a struct or doesn't have common/official
        data_with_name1 = data_selected.withColumn('name1', col('name')) # Or handle as an error/log

    # Select for UUID generation - ensure ccn3 exists or handle its absence
    id_generation_cols = ['name1']
    if 'ccn3' in data_with_name1.columns:
        id_generation_cols.append('ccn3')
    
    data_for_id = data_with_name1.select(*id_generation_cols)

    uuid_udf = udf(generate_uuid5, types.StringType())

    # data_for_id = data_for_id.limit(2) # Removed limit for production
    data_with_id = data_for_id.withColumn(
        'country_id',
        uuid_udf(col('name1')) # Assuming name1 is the primary field for UUID
    )
    
    # data_with_id.show() # Commented out show and exit for production
    # exit()

    # Process the full selected data, not just the ID part
    processed_data = process_data(data_selected) # Pass the broader selection to process_data
    
    # Join the generated ID back to the processed data
    # This requires a common key; assuming 'name1' can be used or join on original keys if available
    # For simplicity, let's assume we add country_id to processed_data if name1 exists there
    if 'name1' in processed_data.columns and 'name1' in data_with_id.columns:
        final_data = processed_data.join(data_with_id.select("name1", "country_id"), ["name1"], "left_outer")
    else: # Fallback or error handling if name1 is not available for join
        final_data = processed_data # Or add country_id in a different way if possible

    renamed_data = rename_columns(final_data)
    renamed_data.write.mode('overwrite').parquet(output_path)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_path = '/usr/local/airflow/data/trusted' # Adjusted to match docker-compose volume mapping
        if len(sys.argv) > 2:
            output_path = sys.argv[2]
        transform_data(input_path, output_path)
    else:
        print("Usage: transform_data.py <input_path> [output_path]")
        # Example for local testing:
        # transform_data('./foundation', './trusted')
