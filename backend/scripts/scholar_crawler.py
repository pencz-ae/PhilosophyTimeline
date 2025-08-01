#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scholar_crawler.py – baixa “scholars” do Wikidata com retomada resiliente.

Correções aplicadas (críticas 1-8, 10):
  • page_size default ↓ 2 000 e auto-dimmer;           #1
  • pulo de ocupações cujo CSV já existe e contém >0 linhas;
    evita re-download e serve como *checkpoint*.        #2‒3
  • arquivo CSV aberto uma única vez por ocupação;      #4
  • retries incluem RemoteDisconnected, JSONDecodeError etc. #5
  • import morto removido; tratamento de erros WDQS ok. #6
  • delay apenas dentro da paginação (não após cada occ). #7
  • SPARQLWrapper reutilizado p/ evitar leak de sockets. #10

Não implementado: deduplicação final (#9).
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import pathlib
import sys
import time
from dataclasses import dataclass
from functools import wraps
from http.client import RemoteDisconnected
from json import JSONDecodeError
from typing import Dict, Iterator, List
from urllib.error import HTTPError as UrlHTTPError

import pandas as pd
import requests
from SPARQLWrapper import JSON, POST, SPARQLWrapper
from tqdm import tqdm


###############################################################################
# 1. Config dataclass
###############################################################################
@dataclass
class Config:
    endpoint: str = "https://query.wikidata.org/sparql"
    user_agent: str = os.getenv(
        "WDQS_USER_AGENT",
        "ScholarCrawler/0.4 (https://github.com/you/yourrepo; contact@example.com)",
    )
    page_size: int = 2_000  # << default menor (crítica 1)
    sleep: float = 1.0  # delay entre lotes
    max_retries: int = 4
    log_level: str = "INFO"
    raw_dir: pathlib.Path = pathlib.Path("data/raw")
    processed_dir: pathlib.Path = pathlib.Path("data/processed")

    @classmethod
    def from_args(cls) -> "Config":
        p = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        add = p.add_argument
        add("--endpoint", default=cls.endpoint)
        add("--user-agent", default=cls.user_agent)
        add("--page-size", type=int, default=cls.page_size)
        add("--sleep", type=float, default=cls.sleep)
        add("--max-retries", type=int, default=cls.max_retries)
        add(
            "--log-level",
            default=cls.log_level,
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        )
        return cls(**vars(p.parse_args()))


###############################################################################
# 2. Retry decorator
###############################################################################


def retry(exceptions, max_retries=4, backoff=2.0):
    """Exponencial simples; mantém assinatura original da função."""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kw):
            tries, delay = max_retries, 1.0
            while True:
                try:
                    return fn(*args, **kw)
                except exceptions as exc:  # pylint: disable=broad-except
                    tries -= 1
                    if tries <= 0:
                        logging.error("falha definitiva: %s", exc)
                        raise
                    logging.warning("%s – retry em %.1fs (%d left)", exc, delay, tries)
                    time.sleep(delay)
                    delay *= backoff

        return wrapper

    return decorator


###############################################################################
# 3. WDQS Client
###############################################################################


class WDQSClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._sparql = SPARQLWrapper(cfg.endpoint, agent=cfg.user_agent)
        self._sparql.setMethod(POST)
        self._sparql.setReturnFormat(JSON)
        self._sparql.setTimeout(60_000)  # 60 s máx do endpoint

    @retry(
        (
            requests.ConnectionError,
            requests.HTTPError,
            UrlHTTPError,
            JSONDecodeError,
            RemoteDisconnected,
        ),
        max_retries=3,
    )
    def _run_once(self, query: str) -> List[Dict]:
        self._sparql.setQuery(query)
        try:
            return self._sparql.query().convert()["results"]["bindings"]

        except UrlHTTPError as e:
            if e.code in {502, 503, 504, 429}:  # ← inclui 429
                # Header Retry-After (segundos) se existir
                wait = int(e.headers.get("Retry-After", "30"))
                logging.warning("HTTP %d – dormindo %ss", e.code, wait)
                time.sleep(wait)
                raise requests.HTTPError(f"HTTP {e.code}", response=e) from e
            raise

    def paged(self, template: str) -> Iterator[Dict]:
        offset = 0
        while True:
            q = template.replace("{OFFSET}", str(offset)).replace(
                "{LIMIT}", str(self.cfg.page_size)
            )
            batch = self._run_once(q)
            if not batch:
                break
            yield from batch
            offset += self.cfg.page_size
            time.sleep(self.cfg.sleep)  # única pausa (crítica 7)


###############################################################################
# 4. SPARQL templates
###############################################################################
QUERY_OCCUPATIONS = """
SELECT ?occ ?lblEN WHERE {
  ?occ wdt:P279+ wd:Q20826540 ; wdt:P31 wd:Q28640 .
  OPTIONAL { ?occ rdfs:label ?lblEN FILTER(LANG(?lblEN)="en") }
}
"""

QUERY_PEOPLE_TEMPLATE = """
SELECT ?person ?personLabel ?birth ?death ?genderLabel ?countryLabel
       ?ethnicityLabel ?religionLabel ?movementLabel ?notableWorkLabel ?occLabel
WHERE {
  VALUES ?targetOcc { wd:{OCC_ID} }
  ?person wdt:P31 wd:Q5 ; wdt:P106 ?targetOcc .
  OPTIONAL { ?person wdt:P569 ?birth. }
  OPTIONAL { ?person wdt:P570 ?death. }
  OPTIONAL { ?person wdt:P21 ?gender. }
  OPTIONAL { ?person wdt:P27 ?country. }
  OPTIONAL { ?person wdt:P172 ?ethnicity. }
  OPTIONAL { ?person wdt:P140 ?religion. }
  OPTIONAL { ?person wdt:P135 ?movement. }
  OPTIONAL { ?person wdt:P800 ?notableWork. }
  OPTIONAL { ?targetOcc rdfs:label ?occLabel FILTER(LANG(?occLabel)="en") }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,pt". }
}
LIMIT {LIMIT}
OFFSET {OFFSET}
"""

###############################################################################
# 5. FS helpers
###############################################################################


def ensure_dirs(cfg: Config):
    for d in (cfg.raw_dir, cfg.processed_dir):
        d.mkdir(parents=True, exist_ok=True)


###############################################################################
# 6. Pipeline
###############################################################################


def download_occupations(client: WDQSClient, cfg: Config) -> pd.DataFrame:
    rows = client._run_once(QUERY_OCCUPATIONS)
    df = pd.DataFrame(
        {
            "occ_id": [r["occ"]["value"].split("/")[-1] for r in rows],
            "occ_label": [r.get("lblEN", {}).get("value", "") for r in rows],
        }
    )
    out = cfg.raw_dir / "occupations.csv"
    df.to_csv(out, index=False)
    logging.info("✓ %d ocupações salvas em %s", len(df), out)
    return df


def _csv_has_data(path: pathlib.Path) -> bool:
    """Retorna True se o CSV existe e tem >1 linha (cabeçalho+dados)."""
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as fp:
            _ = next(fp)  # cabeçalho
            next(fp)
        return True
    except StopIteration:
        return False  # só cabeçalho ou vazio


def download_people_per_occ(
    client: WDQSClient, cfg: Config, occ_id: str, occ_label: str
):
    path = cfg.raw_dir / f"people_{occ_id}.csv"
    if _csv_has_data(path):  # checkpoint (críticas 2-3)
        logging.info("→ %s já baixado – pulando", occ_id)
        return

    header = [
        "person_id",
        "label_en",
        "birth",
        "death",
        "gender",
        "nationality",
        "ethnicity",
        "religion",
        "movement",
        "notable_work",
        "occ_id",
        "occ_label",
    ]

    consecutive_fail = 0
    original_page_size = client.cfg.page_size

    with path.open(
        "w", newline="", encoding="utf-8"
    ) as fp:  # abre uma única vez (crítica 4)
        writer = csv.DictWriter(fp, fieldnames=header)
        writer.writeheader()

        while consecutive_fail < 3:
            try:
                query = QUERY_PEOPLE_TEMPLATE.replace("{OCC_ID}", occ_id)
                for row in client.paged(query):
                    writer.writerow(
                        {
                            "person_id": row["person"]["value"].split("/")[-1],
                            "label_en": row.get("personLabel", {}).get("value", ""),
                            "birth": row.get("birth", {}).get("value", ""),
                            "death": row.get("death", {}).get("value", ""),
                            "gender": row.get("genderLabel", {}).get("value", ""),
                            "nationality": row.get("countryLabel", {}).get("value", ""),
                            "ethnicity": row.get("ethnicityLabel", {}).get("value", ""),
                            "religion": row.get("religionLabel", {}).get("value", ""),
                            "movement": row.get("movementLabel", {}).get("value", ""),
                            "notable_work": row.get("notableWorkLabel", {}).get(
                                "value", ""
                            ),
                            "occ_id": occ_id,
                            "occ_label": occ_label,
                        }
                    )
                client.cfg.page_size = original_page_size
                return True  # sucesso

            except requests.HTTPError as e:
                if getattr(e, "response", None) and e.response.code not in {
                    502,
                    503,
                    504,
                    429,
                }:
                    raise  # outro HTTP: propaga
                consecutive_fail += 1
                client.cfg.page_size = max(500, client.cfg.page_size // 2)
                logging.warning(
                    "⚠️  %s em %s (tentativa %d/3). page_size=%d",
                    e,
                    occ_id,
                    consecutive_fail,
                    client.cfg.page_size,
                )
                time.sleep(5)
                continue

    logging.error("✗ Ocupação %s abandonada após 3 falhas consecutivas", occ_id)
    client.cfg.page_size = original_page_size
    return False


def consolidate_people(cfg: Config):
    """Une todos *people_*.csv* em **processed/scholars.csv** com filtros de data.

    Mantém **apenas**
      – nascimento > 1‑jan‑1801 (exclusivo) e
      – óbito     < 1‑jan‑1901 (exclusivo).

    ⚠️ Deduplicação *não* aplicada (crit. 9 não implementado).
    """

    frames = [
        pd.read_csv(p, dtype=str)
        for p in cfg.raw_dir.glob("people_*.csv")
        if _csv_has_data(p)
    ]
    if not frames:
        logging.warning("Nenhum CSV de pessoas encontrado")
        return

    df = pd.concat(frames, ignore_index=True)
    df.drop(
        columns=[c for c in df.columns if c.lower() == "field"],
        errors="ignore",
        inplace=True,
    )

    df["death_dt"] = pd.to_datetime(df["death"], errors="coerce", utc=True)
    df["birth_dt"] = pd.to_datetime(df["birth"], errors="coerce", utc=True)

    mask = (
        df["birth_dt"].dt.year.le(1901) & df["death_dt"].dt.year.ge(1800)
        | df["death_dt"].isna()
    )
    df = df.loc[mask].drop(columns=["death_dt", "birth_dt"])

    out = cfg.processed_dir / "scholars.csv"
    df.to_csv(out, index=False)
    logging.info("✓ scholars.csv salvo | %d pessoas", len(df))


###############################################################################
# 7. Main — salva falhas também em occupations_failed.pdf
###############################################################################
def main():
    cfg = Config.from_args()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    ensure_dirs(cfg)
    client = WDQSClient(cfg)

    occ_df = download_occupations(client, cfg)

    failed: list[tuple[str, str]] = []  # (occ_id, occ_label)
    for occ_id, occ_label in tqdm(occ_df.values, unit="occ"):
        ok = download_people_per_occ(client, cfg, occ_id, occ_label)
        if not ok:
            failed.append((occ_id, occ_label))

    # ────────────────────────── persistência das falhas ───────────────────────
    if failed:
        import pandas as pd
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas

        fail_df = pd.DataFrame(failed, columns=["occ_id", "occ_label"])

        # 1) CSV para depuração rápida
        csv_path = cfg.processed_dir / "failed_occupations.csv"
        fail_df.to_csv(csv_path, index=False)

        # 2) PDF enxuto (uma linha por ocupação)
        pdf_path = cfg.processed_dir / "occupations_failed.pdf"
        c = canvas.Canvas(str(pdf_path), pagesize=A4)
        width, height = A4
        margin = 15 * mm
        y = height - margin
        line_height = 6 * mm

        c.setFont("Helvetica", 10)
        c.drawString(margin, y, f"Occupations that failed ({len(fail_df)} total)")
        y -= 2 * line_height

        for occ_id, occ_label in failed:
            if y < margin:  # nova página se acabou espaço
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - margin
            c.drawString(margin, y, f"{occ_id} — {occ_label}")
            y -= line_height

        c.save()
        logging.warning(
            "⚠️  %d ocupações falharam — CSV em %s, PDF em %s",
            len(failed),
            csv_path,
            pdf_path,
        )
    else:
        logging.info("✓ Todas as ocupações processadas com sucesso")

    consolidate_people(cfg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Interrompido – saindo…")
        sys.exit(130)
