#!/usr/bin/env python3
"""
phil_crawler.py

1. Lê Q-IDs de ocupações do CSV.
2. Chunking automático para SPARQL (tamanho configurável).
3. Consulta robusta com retry/backoff e tratamento de timeouts.
4. Parsing seguro e completo de bindings.
5. Logging detalhado de progresso, debug e erros em cada passo.
6. Escrita final em CSV único, deduplicado.
"""

import argparse
import logging
import os
import time
from typing import Any, Dict, List

import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper
from tenacity import (RetryError, retry, retry_if_exception_type,
                      stop_after_attempt, wait_exponential)

# Logger global configurado no main
logger = logging.getLogger(__name__)


def load_occupations(csv_path: str) -> pd.DataFrame:
    """Carrega e valida CSV de ocupações."""
    logger.debug(f"Carregando ocupações do CSV: {csv_path}")
    df = pd.read_csv(csv_path, header=None, names=["occ_id", "occ_label"], dtype=str)
    logger.debug(f"CSV carregado, total linhas: {len(df)}")
    if df["occ_id"].isnull().any():
        logger.error("Há Q-IDs vazios no CSV de ocupações.")
        raise ValueError("Há Q-IDs vazios no CSV de ocupações.")
    df = df.drop_duplicates(subset=["occ_id"])
    logger.debug(f"Removidos duplicados, Q-IDs únicos: {len(df)}")
    return df


def chunk_list(lst: List[str], chunk_size: int) -> List[List[str]]:
    """Divide lista em pedaços de até chunk_size."""
    logger.debug(
        f"Quebrando lista de {len(lst)} itens em chunks de tamanho {chunk_size}"
    )
    chunks = [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
    logger.info(f"Total de chunks gerados: {len(chunks)}")
    return chunks


def build_sparql_query(chunk: List[str]) -> str:
    """Monta SPARQL com VALUES para o chunk de ocupações."""
    logger.debug(f"Construindo SPARQL para chunk com Q-IDs: {chunk}")
    values = " ".join(f"wd:{qid}" for qid in chunk)
    query = f"""
SELECT
  ?person
  ?personLabel
  ?itemDescription
  ?dob
  ?dod
  ?genderLabel
  ?nationalityLabel
  ?ethnicityLabel
  ?religionLabel
  ?movementLabel
  (GROUP_CONCAT(DISTINCT ?workLabel; SEPARATOR="|") AS ?notable_work)
  ?occ
  ?occLabel
WHERE {{
  VALUES ?occ {{ {values} }}
  ?person wdt:P106 ?occ.

  OPTIONAL {{ ?person wdt:P569 ?dob. }}
  OPTIONAL {{ ?person wdt:P570 ?dod. }}
  OPTIONAL {{ ?person wdt:P21  ?gender. }}
  OPTIONAL {{ ?person wdt:P27  ?nationality. }}
  OPTIONAL {{ ?person wdt:P172 ?ethnicity. }}
  OPTIONAL {{ ?person wdt:P140 ?religion. }}
  OPTIONAL {{ ?person wdt:P135 ?movement. }}
  OPTIONAL {{ ?person wdt:P800 ?work. }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY
  ?person ?personLabel ?itemDescription
  ?dob ?dod
  ?genderLabel
  ?nationalityLabel
  ?ethnicityLabel
  ?religionLabel
  ?movementLabel
  ?occ ?occLabel
"""
    logger.debug(f"SPARQL construído (tamanho {len(query)} chars)")
    return query


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def execute_query(endpoint_url: str, query: str) -> Dict[str, Any]:
    """Executa SPARQL com retry/backoff."""
    logger.info(f"Executando consulta SPARQL no endpoint {endpoint_url}")
    sparql = SPARQLWrapper(endpoint_url, agent="PhilCrawler/1.0")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    start = time.time()
    result = sparql.query().convert()
    elapsed = time.time() - start
    logger.info(f"Consulta executada em {elapsed:.1f}s")
    return result


def parse_bindings(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converte bindings SPARQL em registros planos."""
    logger.debug(f"Parseando {len(bindings)} bindings")
    records = []
    for idx, b in enumerate(bindings, start=1):

        def val(k: str) -> str:
            return b.get(k, {}).get("value", "")

        rec = {
            "person_id": b["person"]["value"].rsplit("/", 1)[-1],
            "label_en": val("personLabel"),
            "description": val("itemDescription"),
            "birth": val("dob"),
            "death": val("dod"),
            "gender": val("genderLabel"),
            "nationality": val("nationalityLabel"),
            "ethnicity": val("ethnicityLabel"),
            "religion": val("religionLabel"),
            "movement": val("movementLabel"),
            "notable_work": val("notable_work"),
            "occ_id": b["occ"]["value"].rsplit("/", 1)[-1],
            "occ_label": val("occLabel"),
        }
        logger.debug(f"Registro {idx}: {rec}")
        records.append(rec)
    logger.info(f"Parse concluído, registros gerados: {len(records)}")
    return records


def main():
    parser = argparse.ArgumentParser(
        description="Crawls Wikidata persons by occupation Q-IDs."
    )
    parser.add_argument(
        "-i",
        "--input",
        default="data/raw/phil/Phil_occupations.csv",
        help="CSV de ocupações (qid,label), sem cabeçalho.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data/raw/phil/phil_persons_by_occ.csv",
        help="CSV de saída com as pessoas.",
    )
    parser.add_argument(
        "--endpoint",
        default="https://query.wikidata.org/sparql",
        help="URL do endpoint SPARQL.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=20,
        help="Número de Q-IDs por consulta para evitar timeouts.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nível de verbosidade do log.",
    )
    args = parser.parse_args()

    # Configura logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=getattr(logging, args.log_level),
    )
    global logger
    logger = logging.getLogger(__name__)
    logger.info(f"Início da execução com args: {args}")

    df_occ = load_occupations(args.input)
    qids = df_occ["occ_id"].tolist()
    logger.debug(f"Q-IDs carregados: {qids}")
    chunks = chunk_list(qids, args.chunk_size)

    all_records: List[Dict[str, Any]] = []
    for idx, chunk in enumerate(chunks, start=1):
        logger.info(f"--- Iniciando chunk {idx}/{len(chunks)} ---")
        try:
            query = build_sparql_query(chunk)
            result = execute_query(args.endpoint, query)
            bindings = result.get("results", {}).get("bindings", [])
            logger.info(f"Bindings recebidos no chunk {idx}: {len(bindings)}")
            recs = parse_bindings(bindings)
            all_records.extend(recs)
        except RetryError as re:
            logger.error(f"Chunk {idx} falhou após retries: {re}")
        except Exception as e:
            logger.exception(f"Erro inesperado no chunk {idx}")

    logger.info(f"Total de registros antes de deduplicar: {len(all_records)}")
    df_out = pd.DataFrame(all_records).drop_duplicates(
        subset=["person_id", "occ_id", "notable_work"]
    )
    logger.info(f"Total de registros após deduplicar: {len(df_out)}")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df_out.to_csv(args.output, index=False)
    logger.info(f"Salvo {len(df_out)} registros em {args.output}")


if __name__ == "__main__":
    main()
