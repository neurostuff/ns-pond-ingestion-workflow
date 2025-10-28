# Purpose

Add data into NS-Pond. This tool will be a workflow that keeps track of papers and adds them to neurostore.
It will be modular, allowing for different extractors to be used to get papers from different sources.


# Uses Cases

1. Migrate existing entries already in neurostore to have the updated coordinate/metadata extraction.
2. Find new papers to add to neurostore from a list of pmids/dois/pmcids.




# Stages

## Gathering of relevant IDs
1. Use pubmed to gather a list of pmids (search fmri, neuroimaging, etc)
2. Use papers users added to the database (try to get pmids from there)
outputs a list of identifiers (pmids, dois, pmcids) for each paper.
3. Is able to take an intersection/union with existing pmids/identifiers to flexibly filter the list of papers to process.
4. run de-duplication on the list of identifiers (find duplicates based on pmid/doi/pmcid, remove duplicates), look up pmid, via pubmed api or semantic scholar api if needed.

## Download
Use a hiearchy of approaches to get the papers
1. Pubmed Central (PMC) Open Access Subset (https://github.com/neuroquery/pubget)
2. Semantic Scholar (https://www.semanticscholar.org/)
4. Elsevier
5. ACE (https://github.com/neurosynth/ACE)

We should have an option to select which sources to use, and the order to try them in,
but the default should be the above order. Identify whether the paper is openaccess or not.
There is also an opportunity for the ACE extractor to cache its results to avoid re-downloading papers that have already been downloaded. And this should be true for all extractors, that they will cache their results locally to avoid re-downloading papers that have already been downloaded.

## Process
1. For each downloaded paper, get the full text
2. Try to identify any tables containing coordinates
3. If found, extract the table and standardize the format
4. Also extract raw form of the table, one file per table.
5. Extract metadata (title, authors, journal, year, abstract, etc.)
  a. Use semantic scholar/pubmed to get metadata if possible
  b. fall back to parsing the paper if needed
  c. cache the metadata lookup to avoid re-querying for the same paper multiple times,
     this will be independent of the download caching and independent of the source used to
     download the paper.
6. move this data in a standard format into a temporary directory.


## Extract Coordinates
1. For each table in each paper, extract the coordinates using an llm
2. Standardize the coordinates into a common format.
3. Add analyses.json withing the "processed" folder.
Find tables from pubget using the tables.xml file and the table reference id in in the processed folder. This table id tells us that this table was used for coordinate extraction.
(there will be many tables in tables.xml, but only some were used for coordinate extraction,
and we do not want to include all of them in the analyses.json, only the ones that were used for coordinate extraction).


## upload to database
1. For each paper, upload the metadata and coordinates into the neurostore database.
2. If a paper already exists, update the existing entry with any new data (https://neurostore.org/)


## Copy DB ids
Reflect the uploaded data to a local storage location that contains all the papers and their extracted data/full text for easy access.


## Copy text/outputs to folder
With the existing db ids, copy the relevant text and outputs to a specified folder for further analysis.
This process needs to be able to run incrementally, and allow for everwriting existing data if needed.



# Notes
The Download and Process stages will run for each extractor sequentially, removing the papers that were downloaded by a previous extractor.
Once a source is successful, we move on to processing that paper, and do not try to download from the remaining sources:
- pubmed central
- europe pmc
- semantic scholar
- elsevier
- ace

Before the information is added to the database, it will be stored in a temporary folder in a standard format, the ids used will be a hash of the paper's identifiers (pmid, doi, pmcid, etc) to ensure uniqueness.

most of the processing will occur in a temporary folder, and only once all processing is complete will the data be moved to a final storage location.

The eventual final storage format will look like this:

<hash_id>/
    processed/
        pubget/
            coordinates.csv
            text.txt
            metadata.json
        europe_pmc/
            coordinates.csv
            text.txt
            metadata.json
        ace/
            coordinates.csv
            text.txt
            metadata.json
        elsevier/
            coordinates.csv
            text.txt
            metadata.json
        semantic_scholar/
            coordinates.csv
            text.txt
            metadata.json
    source/
        pubget/
            <pmid>.xml
            tables/
                table_000.csv
                table_000_info.json
                tables.xml
        europe_pmc/
        ace/
            <pmid>.html
        elsevier/
            <doi>.xml
        semantic_scholar/
            <paper_id>.xml
    identifiers.json
