# Contributing

FedComp Index is a small project and contributions are welcome. Here's how to get involved.

## Reporting bugs

Open an issue. Include what you expected to happen, what actually happened, and steps to reproduce if possible. Screenshots help.

## Suggesting features

Open an issue with the label `enhancement`. Describe the problem you're trying to solve, not just the solution you want. Context helps us evaluate whether it fits the project.

## Pull requests

1. Fork the repo
2. Create a branch (`git checkout -b fix/thing-you-fixed`)
3. Make your changes
4. Test locally: `python generate/build.py --state NV`
5. Open a PR against `main`

Keep PRs focused. One fix or feature per PR. If you're changing the classification model or methodology, open an issue first to discuss.

## What we're looking for

- Bug fixes
- Data source improvements
- New state coverage
- UI/UX improvements
- Documentation corrections
- Performance improvements to the build pipeline

## What we won't merge

- Changes that add build dependencies or frameworks (no React, no Tailwind, no bundlers)
- Changes that require paid services
- Features that collect user data beyond anonymous page view counts

## Local setup

```bash
pip install -r requirements.txt
python generate/build.py --state NV
# Output lands in site/dist/
```

## Style

- Python: standard library where possible, no type annotations required
- HTML/CSS: no frameworks, everything in `global.css`
- JS: vanilla only, no npm
- Commit messages: short, lowercase, describe the change

## Questions

Open an issue. There's no Discord or Slack.
