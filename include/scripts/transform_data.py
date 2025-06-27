from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, concat_ws, udf, to_json, lit
import uuid
import sys
import os

def check_environment():
    """Check if required environment variables and paths are available"""
    print("🔍 Checking environment...")
    
    # Check Java
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"✅ JAVA_HOME: {java_home}")
    else:
        print("⚠️  JAVA_HOME not set")
    
    # Check Spark
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home:
        print(f"✅ SPARK_HOME: {spark_home}")
    else:
        print("⚠️  SPARK_HOME not set")
    
    # Check Python path
    print(f"🐍 Python executable: {sys.executable}")
    print(f"📁 Current working directory: {os.getcwd()}")
    
    return True

def flatten_struct_columns(data):
    """
    Flatten struct columns to simple types compatible with MySQL
    Convert complex nested structures to JSON strings
    """
    print("🔧 Flattening struct columns for MySQL compatibility...")
    
    # Get schema information
    schema = data.schema
    
    for field in schema.fields:
        if isinstance(field.dataType, types.StructType):
            print(f"📋 Converting struct column '{field.name}' to JSON string")
            # Convert struct to JSON string
            data = data.withColumn(field.name, to_json(col(field.name)))
        elif isinstance(field.dataType, types.ArrayType):
            print(f"📋 Converting array column '{field.name}' to JSON string")
            # Convert array to JSON string
            data = data.withColumn(field.name, to_json(col(field.name)))
    
    print("✅ Struct columns flattened successfully")
    return data

def create_spark_session():
    """Create Spark session with fallback mechanism"""
    spark = None
    
    try:
        # Check if we're in a Docker/distributed environment
        is_distributed = os.environ.get('YARN_CONF_DIR') or os.environ.get('HADOOP_CONF_DIR')
        
        if is_distributed:
            print("🔧 Attempting Spark configuration for distributed environment...")
            try:
                # Try YARN first
                builder = SparkSession.builder \
                    .appName("TransformData") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.network.timeout", "800s") \
                    .config("spark.executor.heartbeatInterval", "60s")
                
                spark = builder.getOrCreate()
                print("✅ Successfully connected to YARN cluster")
                
            except Exception as yarn_error:
                print(f"⚠️  YARN connection failed: {yarn_error}")
                print("🔄 Falling back to local mode...")
                is_distributed = False
                
        if not is_distributed:
            print("🔧 Configuring Spark for local environment...")
            builder = SparkSession.builder \
                .appName("TransformData") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s")
            
            spark = builder.getOrCreate()
        
        # Set log level to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark session created successfully.")
        print(f"🔍 Spark Master: {spark.sparkContext.master}")
        print(f"🔍 Spark Version: {spark.version}")
        print(f"🔍 Distributed mode: {is_distributed}")
        
        return spark
        
    except Exception as e:
        print(f"❗ Failed to create Spark session: {e}")
        import traceback
        traceback.print_exc()
        raise e

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
    """Process data and handle complex nested structures"""
    print("🔧 Processing data with improved struct handling...")
    
    # Handle altSpellings array - join with |
    if 'altSpellings' in data.columns:
        data = data.withColumn(
            'altSpellings',
            concat_ws('|', col('altSpellings'))
        )
    
    # Handle capital array - join with |
    if 'capital' in data.columns:
        data = data.withColumn(
            'capital',
            concat_ws('|', col('capital'))
        )
    
    # Handle latlng array - extract lat and lng
    if 'latlng' in data.columns:
        data = data.withColumn("lat", col("latlng").getItem(0).cast("double"))
        data = data.withColumn("lng", col("latlng").getItem(1).cast("double"))
    
    # Handle languages struct - check if English is used
    if 'languages' in data.columns:
        data = data.withColumn(
            'su_dung_tieng_anh',
            col('languages').getItem('eng').isNotNull()
        )
        
        # Convert languages struct to JSON string for MySQL compatibility
        data = data.withColumn('languages', to_json(col('languages')))
    
    # Handle translations struct - convert to individual columns (selective)
    if 'translations' in data.columns and isinstance(data.schema['translations'].dataType, types.StructType):
        # Extract common translations only
        common_translations = ['eng', 'fra', 'spa', 'deu', 'ita', 'por', 'rus', 'ara', 'zho', 'jpn']
        
        for trans in common_translations:
            try:
                # Check if this translation exists in the struct
                if trans in [field.name for field in data.schema['translations'].dataType.fields]:
                    data = data.withColumn(
                        f'translation_{trans}',
                        to_json(col('translations').getField(trans))
                    )
            except Exception as e:
                print(f"⚠️  Warning: Could not extract translation '{trans}': {e}")
        
        # Convert the entire translations struct to JSON
        data = data.withColumn('translations', to_json(col('translations')))
    
    # Handle maps struct
    if 'maps' in data.columns:
        # Extract Google Maps URL if available
        try:
            data = data.withColumn(
                'Google_Maps',
                col('maps').getField('googleMaps')
            )
        except:
            data = data.withColumn('Google_Maps', lit(None).cast("string"))
        
        # Convert maps to JSON
        data = data.withColumn('maps', to_json(col('maps')))
    
    # Handle timezones array
    if 'timezones' in data.columns:
        data = data.withColumn(
            'timezone_1',
            col('timezones').getItem(0)
        )
        # Convert timezones to JSON string
        data = data.withColumn('timezones', to_json(col('timezones')))
    
    # Handle flags struct
    if 'flags' in data.columns:
        try:
            data = data.withColumn(
                'svg_flags',
                col('flags').getField('svg')
            )
        except:
            data = data.withColumn('svg_flags', lit(None).cast("string"))
        
        # Convert flags to JSON
        data = data.withColumn('flags', to_json(col('flags')))
    
    # Handle continents array
    if 'continents' in data.columns:
        data = data.withColumn('continents', concat_ws('|', col('continents')))

    print("✅ Data processing completed with struct handling")
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
    spark = None
    try:
        # Create Spark session with fallback mechanism
        spark = create_spark_session()
        
        print(f"📥 Reading data from: {input_path}")
        print(f"� Input path exists: {os.path.exists(input_path)}")
        
        if not os.path.exists(input_path):
            print(f"❌ Input path does not exist: {input_path}")
            return
        
        data = spark.read.parquet(input_path)
        print(f"📊 Number of records: {data.count()}")
        print("� Schema:")
        data.printSchema()
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
            
            # Also flatten the name struct to JSON for MySQL compatibility
            data_with_name1 = data_with_name1.withColumn('name', to_json(col('name')))
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

        # Flatten any remaining struct columns for MySQL compatibility
        print("🔧 Final flattening for MySQL compatibility...")
        final_data = flatten_struct_columns(final_data)

        renamed_data = rename_columns(final_data)
        
        print("📋 Final schema before saving:")
        renamed_data.printSchema()
        
        print("📊 Sample of final data:")
        renamed_data.show(2, truncate=False)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        
        print(f"💾 Writing transformed data to: {output_path}")
        renamed_data.write.mode('overwrite').parquet(output_path)
        print("✅ Data transformation completed successfully.")
        
    except Exception as e:
        print(f"❌ Failed to process data: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if spark:
            spark.stop()
            print("✅ Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: transform_data.py <input_path> <output_path>")
        print(f"Received arguments: {sys.argv}")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    print(f"🚀 Starting data transformation...")
    print(f"📍 Input path: {input_path}")
    print(f"📍 Output path: {output_path}")
    
    # Check environment before proceeding
    check_environment()
    
    transform_data(input_path, output_path)
