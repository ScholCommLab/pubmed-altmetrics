# ##################################################
## Project: PubMed - 
## Script purpose: Main script for PubMed data pipeline
## Date: 04.10.2017
## Author: Asura Enkhbayar
####################################################

require(yaml)
require(jsonlite)
require(rentrez)
require(XML)


# Fetches data from PubMed
fetch_pubmed_data <- function(query, output_folder, batchsize=50) {
  # Perform entrez search with web_history to retrieve web_history object
  res <- entrez_search(db="pubmed", term=query, use_history=TRUE)
  print(paste0("Found ", res$count, " results"))
  
  number_loops <- ceiling(res$count / batchsize)
  
  file_count <- 0
  for(seq_start in seq(file_count*batchsize, res$count, batchsize)){
    print(paste0("Starting batch ", file_count, " out of ", number_loops))
    recs <- entrez_fetch(db="pubmed", web_history=res$web_history, rettype="XML", retmax=batchsize, retstart=seq_start)
    json <- toJSON(xmlToList(xmlParse(recs)))
    write(json, file=paste0(output_folder, "records_",file_count,".json"))
    file_count <- file_count + 1
    if (file_count == number_loops) {
      return()
    }
  }
}

config <- yaml.load_file("../config.yml")

# Create dir for current run
datetime <- format(Sys.time(), "%Y%m%d_%H%M%S")
data_dir <- file.path("../data", datetime)
dir.create(data_dir, showWarnings = FALSE)

# Prepare queries (config can contain queries and subqueries)
queries <- list()

q_items <- config$pubmed_queries
q_names <- names(q_items)

for (i in seq(length(q_items))) {
	if (length(q_items[[i]])==1) {
		queries[[q_names[[i]]]] <- q_items[[i]]
	} else {
		for (j in seq(length(q_items[[i]]))) {
			queries[[paste0(q_names[[i]], j)]] <- q_items[[i]][[j]]
		}
	}
}


# Run collector for each subquery
for (i in seq(1, length(queries))) {
	query <- queries[[i]]
	query_name <- names(queries)[[i]]
	out_dir <- file.path(data_dir, query_name)
	temp_dir <- file.path(out_dir, "temp/")
	
	dir.create(out_dir, showWarnings = FALSE)
	dir.create(temp_dir, showWarnings = FALSE)
	
  print(paste("Collecting query", i ,":", query))
	fetch_pubmed_data(query, temp_dir)
}