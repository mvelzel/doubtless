# Databricks notebook source
!pip3 install progressbar

# COMMAND ----------

import progressbar

pbar = None

def show_progress(block_num, block_size, total_size):
    global pbar
    if pbar is None:
        pbar = progressbar.ProgressBar(maxval=total_size)
        pbar.start()

    downloaded = block_num * block_size
    if downloaded < total_size:
        pbar.update(downloaded)
    else:
        pbar.finish()
        pbar = None

# COMMAND ----------

import urllib.request
import gzip
import shutil

# Download the .json.gz file to /tmp
url = "http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/data/offers_english.json.gz"
# url = "http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/samples/sample_offersenglish.json"
gz_path = "/tmp/offers_english.json.gz"
json_path = "/dbfs/offers_english.json"

print("Downloading file")
urllib.request.urlretrieve(url, gz_path, show_progress)

print("Unpacking file")
# Unzip the .json.gz file
with gzip.open(gz_path, 'rb') as f_in:
# with open(gz_path, 'rb') as f_in:
    with open(json_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)