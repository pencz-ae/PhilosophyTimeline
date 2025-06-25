# Filo‑Concept Timeline

Mapeia a ascensão e o declínio de **500 conceitos filosóficos** em milhões de livros (1800‑2020) — dos primórdios da modernidade até a virada do século XXI. O projeto reúne coleta de dados, ciência aberta e visualização interativa, servindo como portfólio prático de *Data Science* + *Digital Humanities*.

---

## ✨ Principais Funcionalidades

| Camada            | Destaques                                                                                                                                                                            |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Backend**       | • Extração automática de termos do Wikidata <br>• Coleta Google Books Ngram <br>• Pipeline de séries temporais com STL & detecção de picos <br>• Banco de dados PostgreSQL + Alembic |
| **Frontend**      | • Linha do tempo animada em React + D3 <br>• Filtros de ano, escala log/linear <br>• Destaque visual de pontos de inflexão <br>• Visualização de redes de coocorrência               |
| **DevOps / FAIR** | • Docker Compose para dev / prod <br>• GitHub Actions: lint, testes, build, deploy <br>• Dataset publicado no Zenodo com DOI                                                         |

---

## 🚀 Como Rodar

### 1. Pré‑requisitos

* **Docker ≥ 24** (ou Podman)
* Git

### 2. Clonar & subir os serviços

```bash
git clone https://github.com/seu‑usuario/filo‑concept‑timeline.git
cd filo‑concept‑timeline
docker compose up --build
```

* `backend` expõe **[http://localhost:8000/api/](http://localhost:8000/api/)**
* `frontend` disponível em **[http://localhost:5173/](http://localhost:5173/)**

### 3. Ambiente Conda (opcional)

Se preferir rodar localmente:

```bash
conda env create -f environment.yml
conda activate filo-concept
pre-commit install
```

---

## 🛠️ Tech Stack

* **Python 3.11** · Pandas · Statsmodels · NetworkX
* **FastAPI** + Uvicorn
* **PostgreSQL 15** + SQLAlchemy + Alembic
* **React 19** · Vite · D3 v7 · TypeScript
* **Docker Compose** · GitHub Actions CI/CD

---

## 📄 Licença

Este projeto utiliza a licença **MIT**. Sinta‑se livre para usar, modificar e distribuir. Veja o texto completo abaixo.

```
MIT License

Copyright (c) 2025 [Seu Nome]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
... (texto completo da MIT License até o fim) ...
```
