# backend/scripts/filter_terms.py
import pandas as pd

df = pd.read_csv("../data/raw/wikidata_terms_raw.csv")

# 1. drop duplicatas exatas
df = df.drop_duplicates(subset="label")

# 2. remover rótulos muito curtos (< 3 caracteres) e genéricos
df = df[df["label"].str.len() > 2]

# 3. opcional: ordenar alfabeticamente
df = df.sort_values("label").reset_index(drop=True)

# 4. opcional: se vc quiser exatamente 600, pegue os primeiros 600
df = df.head(600)

# salva
df.to_csv("../data/raw/wikidata_terms.csv", index=False)
print(f"{len(df)} termos filtrados e salvos em data/raw/wikidata_terms.csv")
