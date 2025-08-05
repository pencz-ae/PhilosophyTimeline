import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", 1000)

# 1) Carrega o CSV
df = pd.read_csv("data/raw/phil/phil_persons_by_occ.csv", dtype=str)

# 2) Converte as colunas para datetime (NaT onde falhar)
df["birth"] = pd.to_datetime(df["birth"], errors="coerce")
df["death"] = pd.to_datetime(df["death"], errors="coerce")

# 3) Opcional: remover linhas que não converteram (birth ou death = NaT)
df = df[df["birth"].notna() & df["death"].notna()]

# 4) Monta a máscara de filtro corretamente:
mask = (df["death"] > "1800-01-01") & (df["birth"] < "1901-01-01")

# 5) Aplica e vê o resultado
filtered = df[mask]
print(filtered.head())

# 6) Salvar o DataFrame filtrado em CSV
output_path = "data/processed/phil_persons_1800_1900.csv"
filtered.to_csv(output_path, index=False)
print(f"Arquivo salvo em {output_path}")
