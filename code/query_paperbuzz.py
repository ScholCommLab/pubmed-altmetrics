import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml
from tqdm import tqdm

if __name__ == "__main__":
    with open('../config.yml', 'r') as f:
        config = yaml.load(f)

    # Init Paperbuzz API
    paperbuzz_baseurl = "https://api.paperbuzz.org/v0/doi/"
    params = {
        'email': config['contact']['email']
    }

    # Base directory
    data_dir = Path("../data/")

    # choose most recent pubmed run
    folders = list(data_dir.glob("*_*"))
    times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
    output_dir = folders[times.index(max(times))]

    # iterate over each query in config and collect altmetrics
    for query in config['queries'].keys():
        input_df = pd.read_csv(output_dir / query / "articles.csv")
        input_df = input_df[input_df.error.isna()]

        out_columns = ["pmid", "doi", "pb_resp", "pb_err", "ts"]

        f = open(str(output_dir / query / "paperbuzz.csv"), "a")
        csv_writer = csv.writer(f)
        csv_writer.writerow(out_columns)

        for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi), total=len(input_df)):
            # Paperbuzz only provides metrics for articles with DOIs
            if not doi:
                continue

            now = datetime.now()

            try:
                r = requests.get(paperbuzz_baseurl + doi, params=params)
                if r.status_code == 200:
                    resp = r.json()
                    err = None
                else:
                    resp = None
                    err = r.status_code
            except Exception as e:
                resp = None
                err = e

            row = [pmid, doi, json.dumps(resp), str(err), str(now)]
            csv_writer.writerow(row)
        f.close()
