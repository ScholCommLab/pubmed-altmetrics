import os
from datetime import datetime
from pathlib import Path

import csv
import numpy as np
import pandas as pd
import yaml
import json
from tqdm import tqdm


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
    out_columns = ["pmid", "doi", "pub_year", "altmetric_id",
                   "altmetric_queried", "paperbuzz_queried"]

    # Metrics and fields to be exported
    am_metrics = config['altmetric_fields']
    ced_metrics = config['paperbuzz_fields']

    # Add metrics to the columns
    sorted_am_metrics = sorted(am_metrics.keys())
    sorted_ced_metrics = sorted(ced_metrics.keys())
    out_columns = out_columns + sorted_am_metrics + sorted_ced_metrics

    for query in config['queries'].keys():
        articles = pd.read_csv(base_dir / query / "articles.csv", na_values="None")
        am_input = pd.read_csv(base_dir / query / "altmetrics.csv",
                               na_values="None", index_col="pmid")
        ced_input = pd.read_csv(base_dir / query / "paperbuzz.csv",
                                na_values="None", index_col="pmid")

        f = open(str(output_dir / query) + ".csv", "w")
        csv_writer = csv.writer(f)
        csv_writer.writerow(out_columns)

        for ix, article in tqdm(articles.iterrows(), total=len(articles)):
            field_vals = {k: None for k in out_columns}

            # Add basic fields
            field_vals['pmid'] = article['pmid']
            field_vals['doi'] = article['doi']
            field_vals['pub_year'] = article['pub_year']

            # Parse altmetric results (if available)
            try:
                am_row = am_input.loc[article['pmid']]
                response = json.loads(str(am_row['am_resp']))

                field_vals['altmetric_id'] = response['altmetric_id']
                field_vals['altmetric_queried'] = am_row['ts']

                for metric in sorted_am_metrics:
                    try:
                        field_vals[metric] = response[am_metrics[metric]]
                    except:
                        pass
            except Exception as e:
                pass

            # Parse Paperbuzz results (if available)
            try:
                ced_row = ced_input.loc[article['pmid']]
                response = json.loads(str(ced_row['pb_resp']))
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
