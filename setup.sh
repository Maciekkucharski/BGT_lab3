sudo apt-get update -y
sudo apt-get install git -y
sudo apt-get install python3 python3-pip -y
pip install dask
pip install fastparquet
pip install pyarrow

gsutil -m cp gs://pjwstk-bigdata/*.parquet .
