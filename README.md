# Wissenschaft | 19th-Century Knowledge Graph

Small research toolkit for mapping **nineteenth-century “scholars”** in Wikidata,
collecting their works and scoring their contribution to the Hegelian idea of
*Wissenschaft*.
_(The project is at an early-stage)_

---

## What it does

| step | script | output |
|------|--------|--------|
| 1 | `scholar_crawler.py` | `data/raw/people_*.csv` – one file per root occupation (1 584 total) |
| 2 | `filter_terms.py` | `data/processed/scholars.csv` – # trims raw term list (stub for now) |

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
