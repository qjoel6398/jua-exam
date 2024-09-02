import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, from_unixtime, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from google.cloud import storage
from netCDF4 import Dataset
import gcsfs
import pandas as pd
import h3
from datetime import datetime, timedelta

def check_file_exists(bucket_name, file_path):
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    return blob.exists()

def read_netcdf(file_content):
    ds = Dataset('memory', mode='r', memory=file_content)
    return ds

@udf(StringType())
def h3_index(lat, lon):
    return h3.geo_to_h3(float(lat), float(lon), 9)  

@udf(TimestampType())
def convert_era5_time(hours_since_1900):
    reference_date = datetime(1900, 1, 1)
    return reference_date + timedelta(hours=int(hours_since_1900))

def process_netcdf(ds, file_path, limit=None):
    # Extract data
    lats = ds.variables["latitude"][:limit]
    lons = ds.variables["longitude"][:limit]
    time = ds.variables["time"][:limit]
    precip = ds.variables["tp"][:limit]

    # Create a list of rows
    rows = []
    for t_idx, t in enumerate(time):
        for lat_idx, lat in enumerate(lats):
            for lon_idx, lon in enumerate(lons):
                rows.append((
                    int(t),
                    float(lat),
                    float(lon),
                    float(precip[t_idx, lat_idx, lon_idx]),
                    file_path
                ))
    return rows

def main(spark, in_prefix, in_suffix, output_path, start_date, end_date, use_parallel, limit=None):
    # Generate list of dates
    date_range = pd.date_range(start=start_date, end=end_date)
    
    if use_parallel:
        # Parallel processing for multiple files
        input_files = [f"{in_prefix}/{date.strftime('%Y/%m/%d')}/{in_suffix}" for date in date_range]
        rdd = spark.sparkContext.binaryFiles(",".join(input_files)).flatMap(
            lambda x: process_netcdf(read_netcdf(x[1]), x[0], limit)
        )
        
        schema = StructType([
            StructField("timestamp", IntegerType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("precipitation", DoubleType(), False),
            StructField("file_path", StringType(), False)
        ])
        
        df = spark.createDataFrame(rdd, schema)
        df = df.withColumn("timestamp", convert_era5_time("timestamp")) \
               .withColumn("h3_index", h3_index("latitude", "longitude"))
        
        df.write.partitionBy("timestamp").parquet(output_path, mode="overwrite")
        print(f"Written to: {output_path}")
    else:
        # Sequential processing
        for date in date_range:
            date_str = date.strftime("%Y/%m/%d")
            bucket_name = in_prefix.split('/')[2]
            file_path = f"{os.path.join(*in_prefix.split('/')[3:])}/{date_str}/{in_suffix}"
            full_path = f"gs://{bucket_name}/{file_path}"
            
            if check_file_exists(bucket_name, file_path):
                print(f"Processing: {full_path}")
                fs = gcsfs.GCSFileSystem(token='anon')
                with fs.open(full_path, 'rb') as f:
                    file_content = f.read()
                ds = read_netcdf(file_content)
                rows = process_netcdf(ds, full_path, limit)
                
                schema = StructType([
                    StructField("timestamp", IntegerType(), False),
                    StructField("latitude", DoubleType(), False),
                    StructField("longitude", DoubleType(), False),
                    StructField("precipitation", DoubleType(), False),
                    StructField("file_path", StringType(), False)
                ])
                
                df = spark.createDataFrame(rows, schema)
                df = df.withColumn("timestamp", convert_era5_time("timestamp")) \
                       .withColumn("h3_index", h3_index("latitude", "longitude"))
                
                output_file = f"{output_path}/{date.strftime('%Y-%m-%d')}"
                df.write.partitionBy("timestamp").parquet(output_file, mode="overwrite")
                print(f"Written to: {output_file}")
            else:
                print(f"File does not exist: {full_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ERA5 to Parquet Conversion")
    parser.add_argument("--in_prefix", required=True, help="Input prefix (e.g., gs://gcp-public-data-arco-era5/raw/date-variable-single_level)")
    parser.add_argument("--in_suffix", required=True, help="Input suffix (e.g., total_precipitation/surface.nc)")
    parser.add_argument("--output", required=True, help="Output directory for Parquet files")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing for multiple files")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of data points (for testing)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ERA5ToParquet").getOrCreate()
    
    main(spark, args.in_prefix, args.in_suffix, args.output, args.start_date, args.end_date, args.parallel, args.limit)
    
    spark.stop()