import os
from datetime import datetime
from pathlib import Path

import csv
import numpy as np
import pandas as pd
import yaml
import json
from tqdm import tqdm

# Define metrics and their respecitve altmetric field names in this dict
# more info here: https://docs.google.com/spreadsheets/d/1ndVY8Q2LOaZO_P_HDmSQulagjeUrS250mAL2N5V8GvY/edit#gid=0
metrics = {
    'twitter': 'cited_by_tweeters_count',
    'facebook': 'cited_by_fbwalls_count',
    'reddit': 'cited_by_rdts_count:',
    'wikipedia': 'cited_by_wikipedia_count',
}

if __name__ == "__main__":

    print("Loading config")
    with open('../config.yml', 'r') as f:
        config = yaml.load(f)

    data_dir = Path("../data/")

    # choose most recent crawl
    folders = list(data_dir.glob("*"))
    times = [datetime.strptime(folder.name, "%Y%m%d_%H%M%S") for folder in folders]
    base_dir = folders[times.index(max(times))]

    output_dir = base_dir / "results"
    if not os.path.exists(str(output_dir)):
        os.makedirs(str(output_dir))

    # Basic columns
    out_columns = ["pmid", "doi", "queried_on", "altmetric_id"]

    # Add metrics
    sorted_metrics = sorted(metrics.keys())
    out_columns = out_columns + sorted_metrics

    for query in config['queries'].keys():
        input_df = pd.read_csv(base_dir / query / "altmetrics_raw.csv", na_values="None")

        f = open(str(output_dir / query) + ".csv", "w")
        csv_writer = csv.writer(f)
        csv_writer.writerow(out_columns)

        for ix, row in tqdm(input_df.iterrows(), total=len(input_df)):
            if not pd.isna(row['am_err']):
                continue

            outrow = []

            try:
                response = json.loads(str(row['am_resp']))
            except:
                continue

            outrow.append(row['pmid'])
            outrow.append(row['doi'])
            outrow.append(row['ts'])

            outrow.append(response['altmetric_id'])

            for metric in sorted_metrics:
                try:
                    outrow.append(response[metrics[metric]])
                except:
                    outrow.append(np.nan)

            csv_writer.writerow(outrow)
        f.close()
