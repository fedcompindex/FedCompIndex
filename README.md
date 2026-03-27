# FedComp Index

![FedComp Index Tabularium](https://fedcompindex.org/static/img/FedComp-Index-Tabularium-Wide-1.png)

Open source competitive intelligence for federal contractors. Scores every small business in a state from 0 to 100 based on public award data.

**[fedcompindex.org](https://fedcompindex.org/)**

## Why this exists

If you're a small business trying to win federal contracts, you're competing blind. The primes have data teams and market intelligence. Everyone else has a SAM.gov login and guesswork.

FedComp Index pulls five years of USASpending award data, scores every contractor on an absolute scale, and publishes the results. Free, open, no account required. Every contractor gets a public dossier with their score, class, contract history, and a proximity map showing who they're actually competing against.

## Live

- [Nevada](https://fedcompindex.org/nv/)

## How scoring works

Two drivers, no normalization:

| Driver | Weight | How it works |
|--------|--------|-------------|
| Award volume | 90% | log10 of total dollars won, mapped to 0-100 |
| Award recency | 10% | Last award date, bucketed by age |

Classes are fixed thresholds:
- **Class 1** - score 60+ (~$100M+ in awards)
- **Class 2** - score 40-59 (~$5M-$100M)
- **Class 3** - below 40

Full methodology: [fedcompindex.org/methodology](https://fedcompindex.org/methodology/)

## Architecture

Static site. No frameworks, no build pipeline.

```
ingest/          SAM.gov, USASpending, SBA data pulls
score/           Scoring engine (Python)
generate/        Static site builder (Python + Jinja2)
site/            Templates, CSS, JS
api/             Cloudflare Worker (spectator page views)
```

Python generates HTML. Cloudflare Pages serves it. The spectator API runs on a Cloudflare Worker with D1 for anonymous page view counts.

## Datasets

- [HuggingFace](https://huggingface.co/npetro6)
- [Kaggle](https://www.kaggle.com/npetro6)

## Packages

Python:
```bash
pip install fedcomp-index-scoring
```

npm:
```bash
npm install fedcomp-index
```

## Data sources

All public, no API keys required:
- [USASpending.gov](https://www.usaspending.gov/) - award history, dollar amounts, agencies, NAICS, PSC
- [SAM.gov](https://sam.gov/) - entity registration, certifications, CAGE/UEI
- [SBA](https://www.sba.gov/) - certification verification

## Resources

- [Methodology](https://fedcompindex.org/methodology/) - how the FedComp Index score is calculated
- [FAQ](https://fedcompindex.org/faq/) - posture classes, index drivers, proximity maps, and more
- [Tabularium](https://fedcompindex.org/tabularium/) - all data, tools, and downloads

## Local setup

```bash
pip install -r requirements.txt
python generate/build.py --state NV
# site/dist/ has the output
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports, data improvements, and new state coverage are all welcome.

## License

[MIT](LICENSE)
