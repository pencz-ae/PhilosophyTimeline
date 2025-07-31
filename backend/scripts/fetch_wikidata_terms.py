# backend/scripts/fetch_wikidata_terms.py
import os

import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper

# 1) configura endpoint Wikidata
endpoint = SPARQLWrapper("https://query.wikidata.org/sparql")
endpoint.setReturnFormat(JSON)

# 2) SPARQL: todos itens (instâncias ou subclasses)
# de "philosophical concept" (Q33104279)

query = """
SELECT DISTINCT ?item ?itemLabel WHERE {
  # instancia ou subclasse recursiva
  { ?item wdt:P31/wdt:P279* wd:Q33104279. }
  UNION
  { ?item wdt:P279/wdt:P279* wd:Q33104279. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
"""
endpoint.setQuery(query)

# 3) roda consulta
results = endpoint.query().convert()

# 4) parse em lista de dicts
terms = []
for b in results["results"]["bindings"]:
    uri = b["item"]["value"]
    qid = uri.rsplit("/", 1)[-1]
    label = b["itemLabel"]["value"]
    terms.append({"qid": qid, "label": label})

df = pd.DataFrame(terms)

# 5) salva CSV bruto
os.makedirs("../data/raw", exist_ok=True)
df.to_csv("../data/raw/wikidata_terms_raw.csv", index=False)

print(f"{len(df)} termos extraídos e salvos em data/raw/wikidata_terms_raw.csv")
