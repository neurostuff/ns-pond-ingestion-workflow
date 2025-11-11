"""Caching services for the ingestion workflow.
This module will be applied at the pipeline level.
It will cache:
1. id lookups (in workflow/gather.py)
2. extractor specific downloads (in workflow/download.py)
3. extractor specific extraction results (in workflow/extract.py)
4. metadata retrievals (in workflow/extract.py)
5. created analyses (in workflow/create_analyses.py)

Cached files will be stored in several directories that may not
be under the cache root.
The cache root will however contain indices for all cached results.

Caching will be decoupled from other services and the extractors.

The cache will be applied in the pipeline files as a way of filtering
what files need to be processed, then those files will be passed to the
next step in the pipeline while the cached results are read from disk.
"""
