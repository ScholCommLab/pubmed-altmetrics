import csv
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from tqdm import tqdm

with open('../config.yml', 'r') as f:
    config = yaml.load(f)

data_dir = Path("../data/")

# choose most recent crawl
folders = list(data_dir.glob("*_*"))
times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
base_dir = folders[times.index(max(times))]
results_dir = base_dir / "results"
if not os.path.exists(str(results_dir)):
    os.makedirs(str(results_dir))

all_queries = list(config['queries'].keys())
subqueries = config['subqueries']

# Queries to be merged
for merge, queries in config['subqueries'].items():
    # Merge Metadata
    articles = []
    metrics = []

    for query in queries:
        articles.append(pd.read_csv(base_dir / query / "articles.csv"))
        metrics.append(pd.read_csv(base_dir / query / "metrics.csv"))
        all_queries.remove(query)

    articles = pd.concat(articles)
    articles = articles.drop_duplicates(subset="pmid")
    articles.to_csv(results_dir / "{}_metadata.csv".format(merge), index=False)

    metrics = pd.concat(metrics)
    metrics = metrics.drop_duplicates(subset="pmid")
    metrics.to_csv(results_dir / "{}_metrics.csv".format(merge), index=False)


for query in all_queries:
    shutil.move(str(base_dir / query / "articles.csv"),
                str(results_dir / "{}_metadata.csv".format(query)))
    shutil.move(str(base_dir / query / "metrics.csv"),
                str(results_dir / "{}_metrics.csv".format(query)))
