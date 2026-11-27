"""Read-only survey: every group's medical_condition items and their MONDO/ONVOC entries, from the
latest ok normalize artifact of each article. Writes groups.jsonl to the cwd."""
import gzip, json, sqlite3, sys
from multiprocessing import Pool

CAT = "/data/alejandro/projects/ns-pond/catalog"

def v(x):
    return x.get("value") if isinstance(x, dict) else x

def one(job):
    art, blob, run = job
    try:
        with gzip.open(f"{CAT}/blobs/{blob[:2]}/{blob}.json.gz", "rt") as fh:
            d = json.load(fh)
    except Exception as e:
        return [{"article": art, "error": str(e)}]
    out = []
    for g in d.get("groups") or []:
        if not isinstance(g, dict):
            continue
        mc = g.get("medical_condition") if isinstance(g.get("medical_condition"), dict) else {}
        items = v(mc) if isinstance(v(mc), list) else ([] if v(mc) in (None, "") else [v(mc)])
        ents = [e for e in mc.get("normalized") or [] if "normalization.medical" in str(e.get("mapping_tool", ""))]
        out.append({"article": art, "run": run, "group": v(g.get("local_id")), "name": v(g.get("name")),
                    "is_healthy": v(g.get("is_healthy")), "status": mc.get("extraction_status"),
                    "items": items, "entries": [{k: e.get(k) for k in ("item", "head", "vocabulary", "status", "value",
                                                  "label", "method", "confidence", "rollup")} for e in ents]})
    return out

if __name__ == "__main__":
    con = sqlite3.connect(f"file:{CAT}/catalog.sqlite?mode=ro", uri=True)
    jobs = con.execute("select article_id, blob, run_id from artifacts where stage='normalize' and status='ok' and blob is not null").fetchall()
    n = 0
    with Pool(int(sys.argv[1]) if len(sys.argv) > 1 else 12) as p, open("groups.jsonl", "w") as fh:
        for rows in p.imap_unordered(one, jobs, chunksize=64):
            for r in rows:
                fh.write(json.dumps(r) + "\n"); n += 1
    print(len(jobs), "articles", n, "groups")
