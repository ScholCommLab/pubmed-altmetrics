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


# Merge metadata files
pcor1_meta = pd.read_csv(base_dir / "PCOR1/articles.csv")
pcor2_meta = pd.read_csv(base_dir / "PCOR2/articles.csv")
pcor_meta = pd.concat([pcor1_meta, pcor2_meta])
pcor_meta = pcor_meta.drop_duplicates(subset="pmid")
pcor_meta.to_csv(base_dir / "results/PCOR_metadata.csv", index=False)

# Merge metrics
pcor1_metrics = pd.read_csv(base_dir / "results/PCOR1_metrics.csv")
pcor2_metrics = pd.read_csv(base_dir / "results/PCOR2_metrics.csv")
pcor_metrics = pd.concat([pcor1_metrics, pcor2_metrics])
pcor_metrics = pcor_metrics.drop_duplicates(subset="pmid")
pcor_metrics.to_csv(base_dir / "results/PCOR_metrics.csv", index=False)