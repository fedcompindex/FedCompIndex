# FedComp Index

![FedComp Index Tabularium](https://fedcompindex.org/static/img/FedComp-Index-Tabularium-Wide-1.png)

Open source competitive intelligence for federal contractors. Classifies every small business in a state based on public award data.

**[fedcompindex.org](https://fedcompindex.org/)**

## Why this exists

If you're a small business trying to win federal contracts, you're competing blind. The primes have data teams and market intelligence. Everyone else has a SAM.gov login and guesswork.

FedComp Index pulls five years of USASpending award data, classifies every contractor on two axes (volume and frequency), and publishes the results. Free, open, no account required. Every contractor gets a public dossier with their Posture Class, contract history, and a proximity map showing who they're actually competing against.

## Live

- [Nevada](https://fedcompindex.org/nv/)

## How classification works

Two axes, no composite score:

| Axis | Threshold | Measure |
|------|-----------|---------|
| Base contract dollars | $5,000,000 | Total obligated from base contracts (excludes delivery orders) |
| Base contract count | 3 | Distinct contract actions in 5-year window |

Four Posture Classes from the intersection:
- **Class 1** - High volume, high frequency (systematic winners)
- **Class 2** - High volume, low frequency (concentrated risk)
- **Class 3** - Low volume, high frequency (growth pipeline)
- **Class 4** - Low volume, low frequency (entry level)

Contractors with 2+ base contracts get a **velocity label** based on award cadence: accelerating, on pace, slowing, declining, or inactive. Velocity measures each contractor against their own historical award rhythm.

The **proximity map** on each contractor's dossier finds similar contractors by shared NAICS and PSC codes, showing who is competing for the same work.

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
- [Kaggle](https://www.kaggle.com/npetro6/datasets)

## Packages

Python:
```bash
pip install fedcomp-index
```

npm:
```bash
npm install fedcomp-index
```

## Data sources

All public, no API keys required:
- [USASpending.gov](https://www.usaspending.gov/) - award history, dollar amounts, agencies, NAICS, PSC
- [SAM.gov](https://sam.gov/) - entity registration, certifications
- [SBA](https://www.sba.gov/) - certification verification

## Resources

- [Methodology](https://fedcompindex.org/methodology/) - how the FedComp Index classification works
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
