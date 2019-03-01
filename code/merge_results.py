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
folders = list(data_dir.glob("*"))
times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
base_dir = folders[times.index(max(times))]

output_dir = base_dir / "results"

pcor1 = pd.read_csv(output_dir / "PCOR1.csv")
pcor2 = pd.read_csv(output_dir / "PCOR2.csv")
pd.concat([pcor1, pcor2], ignore_index=True).to_csv(output_dir / "PCOR.csv", index=False)