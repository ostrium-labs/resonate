"""Export the bench's spans from the collector's file into results.

    python3 -I bench/scripts/export-traces.py <collector traces dir> <out dir>

The collector (bench/stacks/signoz/otelcol.yaml) writes OTLP/JSON export
requests, each zstd-compressed with a 4-byte big-endian length prefix. This
keeps only the bench's own services (other processes on the host also sent
spans to 127.0.0.1:4317) and writes one standard `.jsonl.zst` per service
group, one OTLP/JSON export request per line, which `zstdcat` and any OTLP
JSON reader can read.
"""

import collections
import json
import os
import struct
import sys
import compression.zstd as zstd

KEEP = (
    ("rbench-resonate", "resonate"),
    ("resonate-server", "resonate"),
    ("resonate-server-tidb", "resonate-tidbcloud"),
    ("rbench-resonate-tidb", "resonate-tidbcloud"),
    ("rbench-loams-durable", "loams-durable"),
    ("loams-engine", "loams-durable"),
    ("rbench-temporal", "temporal"),
    ("temporal-server", "temporal"),
    ("rbench-airflow", "airflow"),
    ("airflow", "airflow"),
)


def group(service):
    if service.startswith("resonate-server-tidb5x-"):
        return "resonate-tidb5x"
    for prefix, g in KEEP:
        if service == prefix or service.startswith(prefix + "."):
            return g
    return None


def frames(path):
    with open(path, "rb") as f:
        data = f.read()
    i = 0
    while i + 4 <= len(data):
        (n,) = struct.unpack(">I", data[i : i + 4])
        chunk = data[i + 4 : i + 4 + n]
        i += 4 + n
        if len(chunk) < n:
            break
        yield zstd.decompress(chunk)


def service_of(rs):
    for a in rs.get("resource", {}).get("attributes", []):
        if a.get("key") == "service.name":
            return a.get("value", {}).get("stringValue", "")
    return ""


def main():
    src, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    files = sorted(os.path.join(src, f) for f in os.listdir(src) if f.startswith("spans"))
    writers = {}
    counts = collections.Counter()
    dropped = collections.Counter()
    for path in files:
        for raw in frames(path):
            for line in raw.splitlines():
                if not line.strip():
                    continue
                req = json.loads(line)
                by = collections.defaultdict(list)
                for rs in req.get("resourceSpans", []):
                    svc = service_of(rs)
                    g = group(svc)
                    only = os.environ.get("ONLY")
                    if g is not None and only and g not in only.split(","):
                        g = None
                    n = sum(len(ss.get("spans", [])) for ss in rs.get("scopeSpans", []))
                    if g is None:
                        dropped[svc] += n
                        continue
                    by[g].append(rs)
                    counts[(g, svc)] += n
                for g, rss in by.items():
                    if g not in writers:
                        writers[g] = zstd.open(os.path.join(out, f"{g}.jsonl.zst"), "wb", level=19)
                    writers[g].write(json.dumps({"resourceSpans": rss}, separators=(",", ":")).encode() + b"\n")
    for w in writers.values():
        w.close()
    summary = {
        "kept_spans": {f"{g}/{s}": n for (g, s), n in sorted(counts.items())},
        "dropped_spans_other_services": sum(dropped.values()),
    }
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
