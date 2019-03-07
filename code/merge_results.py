import csv
import json
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

for merge, queries in config['query_merges'].items():
    # Merge Metadata
    dfs = []
    for query in queries:
        dfs.append(pd.read_csv(base_dir / query / "articles.csv"))
    df_meta = pd.concat(dfs)
    df_meta = df_meta.drop_duplicates(subset="pmid")
    df_meta.to_csv(results_dir / "{}_metadata.csv".format(merge), index=False)

    # Merge metrics
    dfs = []
    for query in queries:
        dfs.append(pd.read_csv(results_dir / "{}_metrics.csv".format(query)))
    df_metrics = pd.concat(dfs)
    df_metrics = df_metrics.drop_duplicates(subset="pmid")
    df_metrics.to_csv(results_dir / "{}_metrics.csv".format(merge), index=False)