"""Offloaded stages: runs made on a cluster from a bundle, and imported back.

`export` writes a bundle, `remote` pushes it and submits and watches its jobs, `pull`
fetches and imports what they wrote (through `importer`), and `contract` is the bundle's
format. `cluster/` holds the two scripts a job runs, shipped with every bundle.
"""
