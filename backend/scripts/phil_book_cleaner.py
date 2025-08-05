import re

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", 1000)

# 1) Carrega o CSV
df = pd.read_csv("data/raw/phil/works_with_pub.csv", dtype=str)

# 2) Limpeza colunas de texto
df = df.loc[~df["work_label"].str.match(r"^Q\d{3}", na=False)]
df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")
df = df[df["pub_date"].notna()]

# 4) Monta a máscara de filtro de publicação
#    - Publicações entre 1801 e 1900:
mask = (df["pub_date"] > "1801-01-01") & (df["pub_date"] < "1901-01-01")

# 5) Aplica e vê o resultado
df = df[mask]
print(df.head())

# 6) Salvar o DataFrame filtrado em CSV
output_path = "data/processed/phil_works_1801_1900.csv"
df.to_csv(output_path, index=False)
print(f"Arquivo salvo em {output_path}")
