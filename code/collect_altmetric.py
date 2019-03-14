import csv
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml
from altmetric import Altmetric
from tqdm import tqdm

from helpers import load_config, select_basedir

# Init logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Init
logging.info('Initialise configuration')
data_dir = Path("../data/")
config = load_config("../config.yml")

output_dir = select_basedir(data_dir)

# init altmetric
ALTMETRIC_KEY = config['keys']['altmetric']
altmetric = Altmetric(ALTMETRIC_KEY)

# columns for dataframe containing API responses
out_columns = ["pmid", "doi", "resp", "err", "ts"]

# iterate over each query in config and collect altmetrics
logging.info('Query Altmetric.com for each query')
for query in config['queries'].keys():
    input_df = pd.read_csv(output_dir / query / "articles.csv")
    input_df = input_df[input_df.error.isna()]

    f = open(str(output_dir / query / "altmetrics.csv"), "a")
    csv_writer = csv.writer(f)
    csv_writer.writerow(out_columns)

    for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi),
                          total=len(input_df),
                          desc="{}".format(query),
                          position=None,
                          bar_format="  {desc:<20}{percentage:3.0f}%|{bar}{r_bar}"):
        now = datetime.now()

        try:
            resp = altmetric.pmid(str(pmid))
            err = None
        except requests.exceptions.RequestException as e:
            resp = None
            err = e

        row = [pmid, doi, json.dumps(resp), str(err), str(now)]
        csv_writer.writerow(row)

    f.close()
