# FedComp Index

Open source competitive instrumentation system for federal contractors. Scores every contractor in a state from 0 to 100 using public obligation data from USASpending.gov.

**[fedcompindex.org](https://fedcompindex.org/)**

## Scoring

Two Index Drivers, no normalization, absolute scale:

| Driver | Weight | Method |
|--------|--------|--------|
| Award Volume | 90% | log10 of total obligated dollars (5-year window), mapped to [0, 100] |
| Award Recency | 10% | Step function on last award date, bucketed at year boundaries |

Posture Class thresholds (fixed, state-invariant):
- **Posture Class 1** - score 60+ (~$100M+ in 5-year obligations)
- **Posture Class 2** - score 40-59 (~$5M-$100M)
- **Posture Class 3** - below 40

Each scored contractor receives a Contractor Dossier with score decomposition, obligation history, Proximity Map (6 nearest firms in Competitive Topology), and SBA certifications.

Full specification: [fedcompindex.org/methodology](https://fedcompindex.org/methodology/)

## Coverage

- [Nevada](https://fedcompindex.org/nv/) (348 contractors scored)

## Structure

```
pipeline.py          entry point (ingest + score + build)
ingest/              USASpending.gov + SAM.gov data fetchers
score/               FedComp Index scoring algorithm
generate/            static site builder (Jinja2)
site/templates/      HTML templates
site/static/         CSS, images, assets
data/                scored output (JSON)
```

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env   # add your SUPABASE_URL and SUPABASE_KEY
python pipeline.py --state NV
# output: site/dist/
```

## Datasets

- [HuggingFace](https://huggingface.co/datasets/fedcompindex/nevada-federal-contractors)
- [Kaggle](https://www.kaggle.com/datasets/fedcompindex/nevada-federal-contractors-fedcomp-index)

## Packages

Python:
```bash
pip install fedcomp-index-scoring
```

npm:
```bash
npm install fedcomp-index-scoring
```

- [fedcomp-index-scoring](https://pypi.org/project/fedcomp-index-scoring/) (PyPI)
- [fedcomp-index-data](https://pypi.org/project/fedcomp-index-data/) (PyPI)
- [fedcomp-index-scoring](https://www.npmjs.com/package/fedcomp-index-scoring) (npm)
- [fedcomp-index-data](https://www.npmjs.com/package/fedcomp-index-data) (npm)

## Data sources

All public:
- [USASpending.gov](https://usaspending.gov) - federal contract obligations
- [SAM.gov](https://sam.gov) - entity registration, certifications, NAICS codes

## Resources

- [Methodology](https://fedcompindex.org/methodology/)
- [FAQ](https://fedcompindex.org/faq/)
- [Tabularium](https://fedcompindex.org/tabularium/)

## License

[MIT](LICENSE)
