"""
Step 2. Extract tables/coordinates from downloaded articles.
This module will check the cache for extraction results,
depending on settings, and then run extraction
on the uncached files, updating the cache and
returning all extraction results (cached and new).
Additionally, for each article that has coordinates extracted,
the metadata for that article will be retrieved
(and cached) and returned alongside the extraction results.
"""
