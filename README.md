# Wissenschaft ▶ 19 th-Century Knowledge Graph

Small research toolkit for mapping **nineteenth-century “scholars”** in Wikidata,
collecting their works and scoring their contribution to the Hegelian idea of
*Wissenschaft*.

---

## What it does

| step | script | output |
|------|--------|--------|
| 1 | `grab_wikidata.py` | `data/raw/people_*.csv` – one file per root occupation (1 584 total) |
| 2 | `consolidate_people.py` | `data/processed/scholars.csv` – people who lived **any time** between 1801-01-01 and 1900-12-31 |
| 3 | `works_bigquery.sql` | `author_works.parquet` – all works linked by P50, P57, P800 |
| 4 | `wissenchaft_relevance.py` | ranking of each author (semantic + graph + temporal) |

Failed downloads are logged to `failed_occupations.csv` and a one-page
`occupations_failed.pdf` for quick review.

---

## Quick start (local)

```bash
# clone + create env
git clone https://github.com/youruser/PhilosophyTimeline.git
cd PhilosophyTimeline
conda env create -f environment.yml
conda activate philosophy

# crawl scholars (~1 h, obeys WDQS limits)
python backend/scripts/grab_wikidata.py --page-size 1000 --sleep 2

# clean / filter
python backend/scripts/consolidate_people.py
