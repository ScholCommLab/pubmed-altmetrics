import csv
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from helpers import load_config, select_basedir

# Init logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Init
logging.info('Initialise configuration')
config = load_config("../config.yml")
data_dir = Path("../data/")
base_dir = select_basedir(data_dir)

# Create new dirs
results_dir = base_dir / "results"
if not os.path.exists(str(results_dir)):
    os.makedirs(str(results_dir))

# Metrics and fields to be exported
am_metrics = config['altmetric_fields']
ced_metrics = config['paperbuzz_fields']

# Add metrics to the columns
# Basic columns
out_columns = ["pmid", "doi", "pub_year", "altmetric_id",
               "altmetric_queried", "paperbuzz_queried"]
sorted_am_metrics = sorted(am_metrics.keys())
sorted_ced_metrics = sorted(ced_metrics.keys())
out_columns = out_columns + sorted_am_metrics + sorted_ced_metrics

logging.info('Processing article metrics ...')
for query in config['queries'].keys():
    articles = pd.read_csv(base_dir / query / "articles.csv", na_values="None")
    am_input = pd.read_csv(base_dir / query / "altmetric.csv",
                           na_values="None", index_col="pmid")
    ced_input = pd.read_csv(base_dir / query / "paperbuzz.csv",
                            na_values="None", index_col="pmid")

    f = open(str(base_dir / query / "metrics.csv"), "w")
    csv_writer = csv.writer(f)
    csv_writer.writerow(out_columns)

    rows = list(articles.iterrows())
    for ix, article in tqdm(rows,
                            desc="{}".format(query),
                            position=None,
                            bar_format="  {desc:<20}{percentage:3.0f}%|{bar}{r_bar}"):
        field_vals = {k: None for k in out_columns}

        # Add basic fields
        field_vals['pmid'] = article['pmid']
        field_vals['doi'] = article['doi']
        field_vals['pub_year'] = article['pub_year']

        # Parse altmetric results (if available)
        try:
            am_row = am_input.loc[article['pmid']]
            response = json.loads(str(am_row['resp']))

            field_vals['altmetric_id'] = response['altmetric_id']
            field_vals['altmetric_queried'] = am_row['ts']

            for metric in sorted_am_metrics:
                field_vals[metric] = response[am_metrics[metric]]
        except Exception as e:
            pass

        # Parse Paperbuzz results (if available)
        try:
            ced_row = ced_input.loc[article['pmid']]
            response = json.loads(str(ced_row['resp']))
            field_vals['paperbuzz_queried'] = ced_row['ts']

            counts = {v: None for k, v in ced_metrics.items()}

            for source in response['altmetrics_sources']:
                if source['source_id'] in ced_metrics.values():
                    counts[source['source_id']] = source['events_count']

            for m in sorted_ced_metrics:
                field_vals[m] = counts[ced_metrics[m]]

        except:
            pass

        outrow = []
        for c in out_columns:
            outrow.append(field_vals[c])

        csv_writer.writerow(outrow)
    f.close()

# ===========
# Process queries and subqueries
logging.info('Merging files and writing results ...')
normal_queries = []
subqueries = {}

for query, v in config['pubmed_queries'].items():
    if type(v) == list:
        subqueries[query] = [query+str(i+1) for i in range(len(v))]
    else:
        normal_queries.append(query)

# Queries to be merged
for merge, queries in subqueries.items():
    # Merge Metadata
    articles = []
    metrics = []

    for query in queries:
        articles.append(pd.read_csv(base_dir / query / "articles.csv"))
        metrics.append(pd.read_csv(base_dir / query / "metrics.csv"))

    articles = pd.concat(articles)
    articles = articles.drop_duplicates(subset="pmid")
    articles.to_csv(results_dir / "{}_metadata.csv".format(merge), index=False)

    metrics = pd.concat(metrics)
    metrics = metrics.drop_duplicates(subset="pmid")
    metrics.to_csv(results_dir / "{}_metrics.csv".format(merge), index=False)


for query in normal_queries:
    shutil.copy(str(base_dir / query / "articles.csv"),
                str(results_dir / "{}_metadata.csv".format(query)))
    shutil.copy(str(base_dir / query / "metrics.csv"),
                str(results_dir / "{}_metrics.csv".format(query)))
