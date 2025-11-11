"""
This module will orchestrate the entire ingestion workflow,
calling each step in order:
1. Gather article identifiers (gether.py)
2. Download articles (download.py)
3. Extract tables/coordinates from downloaded articles (extract.py)
4. Create analyses for extracted tables (analyze.py)
5. Upload extracted tables/coordinates to database (upload.py)
6. Sync all data to remote storage (sync.py)

This module will manage the flow of data and initialize
each step with the appropriate parameters from settings.
"""
