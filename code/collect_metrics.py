import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from altmetric import Altmetric
from tqdm import tqdm

from helpers import load_config, select_basedir
from helpers import PaperbuzzAPI

# Output columns for results
METRICS_COLUMNS = ["pmid", "doi", "resp", "err", "ts"]

# Init logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Init
logging.info('Initialise configuration')
data_dir = Path("../data/")
config = load_config("../config.yml")
output_dir = select_basedir(data_dir)

# Initialize APIs
ALTMETRIC_KEY = config['keys']['altmetric']
altmetric = Altmetric(ALTMETRIC_KEY)
paperbuzz = PaperbuzzAPI(config['contact']['email'])


for idx, query in enumerate(config['queries'].keys()):
    input_df = pd.read_csv(output_dir / query / "articles.csv")
    input_df = input_df[input_df.error.isna()]

    # Query Paperbuzz API
    with open(str(output_dir / query / "paperbuzz.csv"), "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(METRICS_COLUMNS)

        for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi),
                              total=len(input_df),
                              desc="Query {} | Paperbuzz".format(idx),
                              bar_format="{desc:<20} {percentage:3.0f}%|{bar}{r_bar}"):
            now = datetime.now()

            try:
                resp = paperbuzz.search(doi)
                err = None
            except Exception as e:
                resp = None
                err = e

            row = [pmid, doi, json.dumps(resp), str(err), str(now)]
            csv_writer.writerow(row)

    # Query Altmetric API
    with open(str(output_dir / query / "altmetric.csv"), "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(METRICS_COLUMNS)

        for pmid, doi in tqdm(zip(input_df.pmid, input_df.doi),
                              total=len(input_df),
                              desc="Query {} | Altmetric".format(idx),
                              bar_format="{desc:<20} {percentage:3.0f}%|{bar}{r_bar}"):
            now = datetime.now()

            try:
                resp = altmetric.pmid(str(pmid))
                err = None
            except Exception as e:
                resp = None
                err = e

            row = [pmid, doi, json.dumps(resp), str(err), str(now)]
            csv_writer.writerow(row)
