"""extract_entities: find organisation names in the fetched documents."""

import re
import time

from flowstep import context, log, output

docs = context()["steps"]["fetch_sources"]["output"]["docs"]
found = set()
for doc in docs:
    names = set(re.findall(r"\b[A-Z][a-z]+(?: Corp)?\b", doc["text"])) - {"One"}
    log(f"{doc['source']}: {', '.join(sorted(names))}")
    found |= names
    time.sleep(.5)
output({"entities": sorted(found)})
