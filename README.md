# ERA5 to Parquet Conversion 

## Solution and Scaling Choices
This project provides a scalable solution for converting ERA5 NetCDF files to Parquet format using PySpark. It's designed to work both locally for testing and on Google Cloud Dataproc for large-scale processing.

My estimation of the data volume for a full year of ERA5 hourly single-variable data is reasonably large, in the range of ~30-50 GB and would need larger resources than my personal laptop. Additionally, I'd like to argue that my choice of tooling used in this exam is scalable beyond a single year of ERA5 data, and would suit your company's needs long-term. 

The script found in `scripts/era5_to_parquet.py` showcases this solution. The script uses python's netcdf library to read netcdfs from the gcs location, pyspark to transform and write to parquet, and Uber's H3 indexing system as the spatial index. Spark was chosen for interoperability with GCP's Dataproc service, which is a managed spark implementation. Dataproc should be able to take this script and parallelize the transformation and netcdf reads over multiple nodes. Without testing this in Dataproc, I can share that my process to optimize this over cloud resources would involve configuring a Dataproc cluster with minimal resources at first, testing small units of the data pipeline, then scaling up as needed. 

Parallelizing the netcdf data read was added near the end and not tested, but I wanted to add this in order to address what could to be a bottleneck in this pipeline; the sequential file data read and creation of the pyspark dataframe.

There are other optimizations that I didn't have time to add. Depending on the type of spatial data access we'd like to do, there are different schemes of partitioning on the H3 indices that we could try. Additionally, there are configurations and tuning we could apply to spark itself to get the most out of the Dataproc clusters, such as strategic persistence, caching, executor/spark node configurations, etc

## Requirements

- Python 3.7+
- PySpark 3.1+
- Google Cloud SDK (for GCS and Dataproc usage)
- Java 8

## Installation

1. Clone this repository:
git clone https://github.com/qjoel6398/jua-exam.git
cd era5-to-parquet

2. Install the required packages:
pip install -r requirements.txt


## Usage

### Local Testing

For local testing with a single file or a small date range:

```bash
spark-submit scripts/era5_to_parquet.py \
--in_prefix gs://gcp-public-data-arco-era5/raw/date-variable-single_level \
--in_suffix total_precipitation/surface.nc \
--output ./output \
--start_date 2022-01-01 \
--end_date 2022-01-02 \
--limit 10
```

### Dataproc Execution

For processing multiple files in parallel on Dataproc:

```bash
gcloud dataproc jobs submit pyspark scripts/era5_to_parquet.py \
  --cluster=your-cluster-name \
  --region=your-region \
  -- \
  --in_prefix gs://gcp-public-data-arco-era5/raw/date-variable-single_level \
  --in_suffix total_precipitation/surface.nc \
  --output gs://your-bucket/output \
  --start_date 2022-01-01 \
  --end_date 2022-12-31 \
  --parallel
```

### Performance Estimates

Local Processing: Expect to process about 1-5 days of data per hour, depending on your machine's size.
Dataproc (Small Cluster): With a 3-node cluster, you might process 20-50 days of data per hour.
Dataproc (Large Cluster): With a 10+ node cluster, you could potentially process a full year of data in a few hours.

### Query examples

The Jupyter Notebook `scripts/query_examples.ipynb` showcases various ways to use the timestamp and spatially-indexed parquet file once created. 