"""One shard of a bundle, on a cluster node: merge the adapter (once per adapter revision),
serve the model with vLLM, send each request, and write results/<shard>.jsonl as rows
finish. A rerun keeps the rows that have an answer and sends the rest again.

    python job.py BUNDLE_DIR SHARD

Run with the machine's serve python. Standard library only: ns-pond is not installed on
the cluster. Reads BUNDLE_DIR/bundle.json and BUNDLE_DIR/cluster.json (`ingest offload
push` writes it: the base model, adapter and merged directories, the train python and the
merge script). Ported from pondie tacc/eval_record.py, as the ns extraction ran it.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

#: How long a shard waits for another shard's merge of the same adapter.
MERGE_WAIT_S = 3600


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 24), b""):
            digest.update(block)
    return digest.hexdigest()


def merged_model(cluster, spec):
    """The merged model's directory: merged here, or by another shard that got there first."""
    merged, adapter = Path(cluster["merged_dir"]), Path(cluster["adapter_dir"])
    ok = merged / ".ok"
    if ok.exists() and ok.read_text().strip() == spec["adapter_sha256"]:
        return merged
    weights = adapter / "adapter_model.safetensors"
    found = sha256(weights)
    if found != spec["adapter_sha256"]:
        sys.exit(f"adapter {weights}: sha256 {found}, the bundle names {spec['adapter_sha256']}")
    lock = merged.with_name(merged.name + ".lock")
    try:
        lock.mkdir(parents=True)
    except FileExistsError:   # another shard is merging: wait for it
        t = time.time()
        while time.time() - t < MERGE_WAIT_S:
            if ok.exists():
                return merged
            time.sleep(30)
        sys.exit(f"waited {MERGE_WAIT_S}s for {lock}'s merge; remove the lock if its job died")
    try:
        print(f"MERGE {adapter} -> {merged}", flush=True)
        shutil.rmtree(merged, ignore_errors=True)
        # GH200: the HBM is a NUMA node, so page cache from writing the merged shards can fill
        # the GPU. Prefer CPU memory (node 0) for the merge and its writes.
        numa = ["numactl", "--preferred=0"] if shutil.which("numactl") else []
        r = subprocess.run(numa + [cluster["train_python"], "-I", cluster["merge_script"],
                                   cluster["base_dir"], str(adapter), str(merged), "cpu"])
        if r.returncode:
            sys.exit(f"merge failed rc={r.returncode}")
        ok.write_text(spec["adapter_sha256"])
    finally:
        shutil.rmtree(lock, ignore_errors=True)
    return merged


def drop_page_cache(model_dir):
    """On GH200 the merge's page cache can sit in the HBM node and shrink vLLM's share."""
    for f in Path(model_dir).iterdir():
        if f.is_file() and f.suffix == ".safetensors":
            fd = os.open(f, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)


def main():
    root, shard = Path(sys.argv[1]), sys.argv[2]
    bundle = json.loads((root / "bundle.json").read_text())
    cluster = json.loads((root / "cluster.json").read_text())
    spec, serve = bundle["spec"], bundle["spec"]["serve"]
    vis = os.environ.get("CUDA_VISIBLE_DEVICES")
    if vis is not None and len([g for g in vis.split(",") if g]) < serve["tp"]:
        sys.exit(f"tp {serve['tp']} needs {serve['tp']} GPUs; CUDA_VISIBLE_DEVICES={vis!r}")
    t0 = time.time()
    model_dir = merged_model(cluster, spec)
    drop_page_cache(model_dir)
    port = 8391 + int(os.environ.get("SLURM_LOCALID", "0"))
    log = root / "logs" / f"vllm-{shard}-{os.environ.get('SLURM_JOB_ID', 'local')}.log"
    log.parent.mkdir(exist_ok=True)
    vllm = str(Path(sys.executable).parent / "vllm")
    srv = subprocess.Popen(
        [vllm, "serve", str(model_dir), "--served-model-name", "model", "--host", "127.0.0.1",
         "--port", str(port), "--max-model-len", str(serve["max_len"]),
         "--gpu-memory-utilization", str(serve["gpu_mem"]),
         "--max-num-seqs", str(serve["workers"]),
         "--language-model-only", "--tensor-parallel-size", str(serve["tp"]), *serve["vllm_args"]],
        stdout=open(log, "w"), stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=dict(os.environ, PATH=f"{Path(sys.executable).parent}:" + os.environ["PATH"]))
    url = f"http://127.0.0.1:{port}/v1/chat/completions"
    try:
        for _ in range(180):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/models", timeout=3)
                break
            except Exception:  # noqa: BLE001 - not up yet
                if srv.poll() is not None:
                    break
                time.sleep(10)
        else:
            srv.poll()
        if srv.poll() is not None:
            print(log.read_text()[-3000:])
            sys.exit("vllm did not start")
        print(f"[{time.time() - t0:.0f}s] vllm up", flush=True)
        run(root, shard, url)
    finally:
        if srv.poll() is None:
            srv.terminate()
            try:
                srv.wait(60)
            except Exception:  # noqa: BLE001
                srv.kill()


def request_body(row, serve, schema=None):
    """The chat request for one input row: greedy unless the spec samples, and decoded
    against `schema` when the spec names one."""
    response_format = ({"type": "json_schema", "json_schema": {"name": serve["json_schema"],
                                                               "schema": schema}}
                       if schema is not None else {"type": "json_object"})
    body = {"model": "model", "temperature": serve.get("temperature", 0.0),
            "max_tokens": row["max_tokens"],
            "messages": [{"role": "user", "content": row["content"]}],
            "chat_template_kwargs": row["chat_template_kwargs"],
            "response_format": response_format}
    if serve.get("seed") is not None:
        body["seed"] = serve["seed"]
    return body


def run(root, shard, url):
    rows = [json.loads(line) for line in open(root / "inputs" / f"{shard}.jsonl")]
    out = root / "results" / f"{shard}.jsonl"
    out.parent.mkdir(exist_ok=True)
    have = set()
    if out.exists():   # a rerun keeps the answered rows and sends the rest again
        keep = [line for line in open(out)
                if line.strip() and json.loads(line).get("finish_reason")]
        have = {json.loads(line)["article_id"] for line in keep}
        out.write_text("".join(keep))
    rows = [r for r in rows if r["article_id"] not in have]
    serve = json.loads((root / "bundle.json").read_text())["spec"]["serve"]
    workers = serve["workers"]
    schema = (json.loads((root / "schema.json").read_text())
              if serve.get("json_schema") else None)
    print(f"resume: have {len(have)}, todo {len(rows)}", flush=True)
    lock, done, t1 = threading.Lock(), [0, 0], time.time()
    handle = open(out, "a")

    def one(r):
        t = time.time()
        res = {"article_id": r["article_id"], "text_sha256": r["text_sha256"], "prediction": None,
               "finish_reason": None, "error": None, "prompt_tokens": None,
               "completion_tokens": None}
        body = request_body(r, serve, schema)
        try:
            q = urllib.request.Request(url, json.dumps(body).encode(),
                                       {"Content-Type": "application/json"})
            j = json.load(urllib.request.urlopen(q, timeout=7200))
            choice = j["choices"][0]
            res.update(finish_reason=choice.get("finish_reason"),
                       prompt_tokens=j["usage"]["prompt_tokens"],
                       completion_tokens=j["usage"]["completion_tokens"])
            text = choice["message"].get("content")
            try:
                res["prediction"] = json.loads(text)
            except Exception as e:  # noqa: BLE001
                res["error"] = "invalid_json: " + repr(e)[:150]
        except urllib.error.HTTPError as e:
            res["error"] = f"http {e.code}: " + e.read().decode()[:400]
        except Exception as e:  # noqa: BLE001
            res["error"] = "exc: " + repr(e)[:300]
        res["wall_s"] = round(time.time() - t, 1)
        with lock:
            handle.write(json.dumps(res) + "\n")
            handle.flush()
            done[0] += 1
            done[1] += res["completion_tokens"] or 0
            if done[0] <= 5 or done[0] % 20 == 0:
                el = time.time() - t1
                print(f"[{el:.0f}s] done {done[0]}/{len(rows)} out_tok {done[1]} "
                      f"({done[1] / el:.0f}/s) last {res['finish_reason']} err={res['error']}",
                      flush=True)

    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(one, sorted(rows, key=lambda r: -r["n_input_tokens"])))
    handle.close()
    print("SHARD DONE", json.dumps({"shard": shard, "rows": done[0], "out_tokens": done[1],
                                    "elapsed_s": round(time.time() - t1)}), flush=True)


if __name__ == "__main__":
    main()
