"""fetch_sources: pretend to pull documents from a few sources (replace with real fetching)."""

import sys
import time

from flowstep import log, output

topic, sources = sys.argv[1], sys.argv[2].split(",")
docs = []
for source in sources:
    log(f"fetching {source} for {topic!r}...")
    time.sleep(1)
    text = (f"{source} on {topic}: Acme Corp and Globex reviewed the {topic} rollout. "
            f"One region saw a short outage; Initech is evaluating the same approach.")
    docs.append({"source": source, "text": text})
    log(f"  got {len(text)} chars")
log(f"fetched {len(docs)} documents")
output({"docs": docs})
