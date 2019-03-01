import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from altmetric import Altmetric
from tqdm import tqdm

with open('../config.yml', 'r') as f:
    config = yaml.load(f)

# init altmetric
ALTMETRIC_KEY = config['keys']['altmetric']
altmetric = Altmetric(ALTMETRIC_KEY)

data_dir = Path("../data/")

# choose most recent crawl
folders = list(data_dir.glob("*"))
times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
output_dir = folders[times.index(max(times))]

# iterate over each query in config and collect altmetrics
for query in config['queries'].keys():
    input_df = pd.read_csv(output_dir / query / "articles.csv")

    out_columns = ["pmid", "doi", "am_resp", "am_err", "ts"]

    f = open(str(output_dir / query / "altmetrics_raw.csv"), "a")
    csv_writer = csv.writer(f)
    csv_writer.writerow(out_columns)

    for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi), total=len(input_df)):
        now = datetime.now()

        try:
            am_resp = altmetric.pmid(str(pmid))
            am_err = None
        except Exception as e:
            am_resp = None
            am_err = e

        row = [pmid, doi, json.dumps(am_resp), str(am_err), str(now)]
        csv_writer.writerow(row)
    
    f.close()