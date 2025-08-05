#!/usr/bin/env python3
"""
Script robusto para extrair todas as informações de filósofos do Wikidata e
listar todas as obras produzidas por cada autor, incluindo coautores.
"""
import argparse
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import pandas as pd
import requests

# Configurações padrão
DEFAULT_BATCH_SIZE = 25
DEFAULT_MAX_RETRIES = 3
DEFAULT_THREADS = 4
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "PhilosopherFetcher/1.0 (youremail@example.com)"
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

# Logger
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("PhilosopherFetcher")


def init_db(path: str) -> sqlite3.Connection:
    """Inicializa o banco SQLite com tabelas e índices."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    # Tabela de claims do filósofo
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS claims (
            person_id TEXT,
            property TEXT,
            property_label TEXT,
            value TEXT,
            value_label TEXT,
            PRIMARY KEY(person_id, property, value)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_claims_person ON claims(person_id);")
    # Tabela de obras e coautores
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS works (
            person_id TEXT,
            work_qid TEXT,
            work_label TEXT,
            author_qid TEXT,
            author_label TEXT,
            PRIMARY KEY(person_id, work_qid, author_qid)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_works_work ON works(work_qid);")
    conn.commit()
    logger.info("SQLite DB inicializado: %s", path)
    return conn


def sparql_query(session: requests.Session, query: str, retries: int) -> Dict[str, Any]:
    """Executa SPARQL com retry/backoff exponencial."""
    for attempt in range(1, retries + 1):
        try:
            response = session.get(
                SPARQL_ENDPOINT, params={"query": query, "format": "json"}, timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff = 2**attempt
            logger.warning(
                "SPARQL falhou (tentativa %d/%d): %s. Retry em %ds.",
                attempt,
                retries,
                e,
                backoff,
            )
            time.sleep(backoff)
    logger.error("SPARQL falhou após %d tentativas.", retries)
    return {}


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    """Divide lista em chunks de tamanho fixo."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def fetch_claims_batch(
    session: requests.Session, ids: List[str], retries: int
) -> List[tuple]:
    """Busca todas as claims diretas (wdt:) para cada philosopher."""
    values = " ".join(f"wd:{pid}" for pid in ids)
    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT ?person_id ?p ?pLabel ?o ?oLabel WHERE {{
  VALUES ?person_id {{ {values} }}
  ?person_id ?p ?o .
  FILTER(STRSTARTS(STR(?p), STR(wdt:)))
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,pt". }}
}}"""
    data = sparql_query(session, query, retries).get("results", {}).get("bindings", [])
    rows = []
    for b in data:
        pid = b["person_id"]["value"].split("/")[-1]
        prop = b["p"]["value"].split("/")[-1]
        prop_lbl = b.get("pLabel", {}).get("value", "")
        o = b["o"]
        val = o["value"].split("/")[-1] if o.get("type") == "uri" else o["value"]
        val_lbl = b.get("oLabel", {}).get("value", "")
        rows.append((pid, prop, prop_lbl, val, val_lbl))
    logger.debug("Batch claims: %d registros", len(rows))
    return rows


def fetch_works_batch(
    session: requests.Session, ids: List[str], retries: int
) -> List[tuple]:
    """Busca todas as obras produzidas e coautores de cada person_id."""
    values = " ".join(f"wd:{pid}" for pid in ids)
    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT ?person_id ?work ?workLabel ?author ?authorLabel WHERE {{
  VALUES ?person_id {{ {values} }}
  {{ ?work wdt:P50   ?person_id }} UNION
  {{ ?work wdt:P170  ?person_id }} UNION
  {{ ?person_id wdt:P800 ?work }}
  ?work (wdt:P50|wdt:P170) ?author .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,pt". }}
}}"""
    data = sparql_query(session, query, retries).get("results", {}).get("bindings", [])
    rows = []
    for b in data:
        pid = b["person_id"]["value"].split("/")[-1]
        wqid = b["work"]["value"].split("/")[-1]
        wlbl = b.get("workLabel", {}).get("value", "")
        auth = b["author"]["value"].split("/")[-1]
        albl = b.get("authorLabel", {}).get("value", "")
        rows.append((pid, wqid, wlbl, auth, albl))
    logger.debug("Batch works: %d registros", len(rows))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Wikidata Philosopher Fetcher")
    parser.add_argument("--input", default="data/processed/phil_persons_1800_1900.csv")
    parser.add_argument("--db", default="data/raw/phil/wikidata_cache.db")
    parser.add_argument("--out-meta", default="data/raw/phil/phil_data_enriched.csv")
    parser.add_argument("--out-works", default="data/raw/phil/phil_works_enriched.csv")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--log", default="INFO")
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log.upper(), logging.INFO))
    logger.info("Starting with args: %s", args)

    # Carrega CSV
    df = pd.read_csv(args.input, dtype=str)
    if "person_id" not in df.columns:
        logger.error("Coluna 'person_id' não encontrada no CSV")
        sys.exit(1)
    ids = df["person_id"].dropna().unique().tolist()
    logger.info("Total philosophers: %d", len(ids))

    # Inicializa DB
    conn = init_db(args.db)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Batches de IDs
    batches = chunk_list(ids, args.batch)
    logger.info("Gerando %d batches de tamanho %d", len(batches), args.batch)

    # 1) Claims sequenciais
    for i, batch in enumerate(batches, start=1):
        rows = fetch_claims_batch(session, batch, args.retries)
        if rows:
            conn.executemany("INSERT OR IGNORE INTO claims VALUES(?,?,?,?,?)", rows)
            conn.commit()
        logger.info("Claims batch %d: %d linhas", i, len(rows))

    # 2) Works em paralelo
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_batch = {
            executor.submit(fetch_works_batch, session, b, args.retries): idx
            for idx, b in enumerate(batches, start=1)
        }
        for future in as_completed(future_to_batch):
            idx = future_to_batch[future]
            try:
                rows = future.result()
                if rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO works VALUES(?,?,?,?,?)", rows
                    )
                    conn.commit()
                logger.info("Works batch %d: %d linhas", idx, len(rows))
            except Exception as e:
                logger.error("Erro na works batch %d: %s", idx, e)

    # 3) Construção de CSVs finais
    df_claims = pd.read_sql_query("SELECT * FROM claims", conn)
    df_works = pd.read_sql_query("SELECT * FROM works", conn)

    # Enriquecer metadata com colunas fixas do CSV original
    meta_cols = [
        "person_id",
        "label_en",
        "description",
        "birth",
        "death",
        "gender",
        "nationality",
        "ethnicity",
        "religion",
        "movement",
        "occ_label",
    ]
    df_meta = df[meta_cols].merge(df_claims, on="person_id", how="left")
    df_meta.to_csv(args.out_meta, index=False)
    logger.info("Salvo metadata enriquecido em %s", args.out_meta)

    # Enriquecer obras com nome de autor
    df_works = df_works.merge(df[["person_id", "label_en"]], on="person_id", how="left")
    df_works.rename(columns={"label_en": "orig_author_label"}, inplace=True)
    df_works.to_csv(args.out_works, index=False)
    logger.info("Salvo works enriquecido em %s", args.out_works)


if __name__ == "__main__":
    main()
