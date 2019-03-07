# PCOR-wikipedia
> Retrieving Wikipedia mentions of patient-centered outcomes research

This project retrieves articles from Pubmed and collects available altmetrics from [Altmetric.com](https://www.altmetric.com/) and [Paperbuzz](http://paperbuzz.org/) which ingests data from [Crossref Event Data (CED)](https://www.crossref.org/services/event-data/).

For each specified Pubmed query the whole pipeline will thus produce a single spreadsheet containing metadata from Pubmed and specified metrics from Altmetric and Paperbuzz.

The processing pipeline:

1. Collect papers from Pubmed based on query. This step also produces a new timestamped folder for the results. The following steps _**will always be applied to the newest data collection**_ (i.e. newest folder) available. Thus, make sure to have the correct folders and files in the data folder.
2. Retrieve available metrics from Altmetric based on PMID
3. Retrieve available metrics from Paperbuzz based on DOI
4. Merge results into one single spreadsheet

## Instructions

To run the code you require Python 3 and R installed on your system. Python requirements are specified in [`requirements.txt`](./requirements.txt). Make sure to install the following 4 packages for the R script: `yaml`, `jsonlite`, `rentrez`, `XML`.

### Setting up the configuration

Create a copy of `example_config.yml` and rename it to `config.yml`.

1. Define your Pubmed queries.
2. Define the queries that need to be merged together. The entrez API does has a character limit for URLs, thus, some longer Pubmed queries need to broken down into separate sub-queries, even if the long query is working for the advanced search interface.
3. Specify which metrics should be exported from the Altmetric/Paperbuzz results
   - **Altmetric**: The names of (most) available fields can be found in this [spreadsheet](https://docs.google.com/spreadsheets/d/1ndVY8Q2LOaZO_P_HDmSQulagjeUrS250mAL2N5V8GvY/edit#gid=0). The key in the YAML config corresponds to the desired name of the metric in the output CSV. _You can choose the name of the key!_ The value on the other hand corresponds to the name of the field within Altmetric's database (you can find them in the link above)
   - **Paperbuzz/CED**: Once again, _you can name the key as you wish_. In this case, the value should be one of the available `source_id`s defined in the Crossref Event Data docs. For example, The [CED docs page for Twitter](https://www.eventdata.crossref.org/guide/sources/twitter/) shows that the `source_id` for Twitter is `twitter`. 
4. Provide your altmetric key to access their API
5. Provide contact details for the Paperbuzz API (which is free)

### Running the code

Once the configuration is done, simply execute scripts in the following order:

1. `collect_pubmed.R` to collect all raw results from Pubmed. This script will create temporary JSON files in the respective folders (both `Rscript collect_pubmed.R` from the console or Rstudio should work)
2. `process_pubmed.py` to process these temporary JSON files. This script creates a CSV with basic metadata for all articles
3. `collect_altmetric.py` to retrieve altmetrics from Altmetric.com. This step creates a CSV with a dump of the JSON responses from the API
4. `collect_paperbuzz.py` creates a CSV with the JSON dumps from the Paperbuzz API
5. `export_data.py` finally extracts specified metrics from the two spreadsheets and creates a final CSV containing metadata + metrics
6. `merge_results.py` is a final step required for this project to merge two of the queries as the initial Pubmed query was too long

## Notes on metrics

While we are calling both types of returned results "metrics" it is important to note that Altmetric and CED represent fundamentally different types of observations. While the exact definitions differ for various types of engagements, Altmetric tries to aggregate events that relate to a singular source. Crossref on the other hand sticks to observations of activities related to DOIs.

- More readings on Altmetric [here](https://api.altmetric.com/) and the previously mentioned link also contains some useful descriptives of each metric.
- CED has an amazing [user guide](https://www.eventdata.crossref.org/guide/index.html) that is quite comprehensive and useful.

## License 

The project has been created by [Asura Enkhbayar](github.com/bubblbu) and is shared under the MIT license. It also relies on the [rOpenSci](https://ropensci.org/) package [rentrez](https://cran.r-project.org/web/packages/rentrez/index.html).