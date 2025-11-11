"""Step 4. Upload extracted tables/coordinates to database.
This module will upload the extraction results
(and associated metadata) to the database.
If the base study for an article already exists,
then a new study version will be created,
For ace and pubget articles, a new version will
be created only if results have changed."""
