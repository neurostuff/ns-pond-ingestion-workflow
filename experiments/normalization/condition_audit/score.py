"""Grade counts per stratum, precision and recall, from gold.tsv: `python score.py [gold.tsv]`."""
import collections
import csv
import sys

GRADES = ["E", "A", "P", "W", "Um", "Uc", "X", "Xm"]
rows = list(csv.DictReader(open(sys.argv[1] if len(sys.argv) > 1 else "gold.tsv"), delimiter="\t"))
for vocab in ("mondo", "onvoc"):
    print(f"== {vocab}")
    for stratum in dict.fromkeys(r["stratum"] for r in rows):
        c = collections.Counter(r[f"{vocab}_grade"] for r in rows if r["stratum"] == stratum)
        print(f"{stratum:16s} n={sum(c.values()):3d} " + " ".join(f"{g}={c[g]}" for g in GRADES))
    for name, pool in (("random", [r for r in rows if r["stratum"] == "random"]), ("all", rows)):
        c = collections.Counter(r[f"{vocab}_grade"] for r in pool)
        has_id = c["E"] + c["A"] + c["P"] + c["W"]
        print(f"{name}: precision {c['E'] + c['A']}/{has_id}, recall {c['E'] + c['A']}/{has_id + c['Um']}, "
              f"healthy {c['X']}/{c['X'] + c['Xm']}")
