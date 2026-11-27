# Contributing

Thanks for helping improve the course. The full guide, with a worked example, is on the website:
**[Contributing: adding a notebook to a module](https://worldbank.github.io/BDA-with-Python/guides/contributing.html)**.

In short:

1. **Branch** from an up-to-date `main`: `git checkout main && git pull && git checkout -b add-<topic>`.
2. **Add the notebook** to its module folder, `notebooks/NN-module-name/`, using lowercase, hyphenated
   file names. Start it with a title cell, then the Open in Colab badge and a requirements line.
   Use relative data paths (`../../data/raw/<file>`) and never include personal or restricted data.
3. **Update the module page** (`notebooks/NN-module-name/index.qmd`) with a card for the notebook,
   and add the notebook to the module's sidebar entry in `_quarto.yml`.
4. **Run all cells** from a restarted kernel so the stored outputs are clean, and optionally check
   the site with `quarto preview`.
5. **Open a pull request** against `main` and wait for the "Publish site" check to pass. The
   instructor reviews and merges it; please don't merge it yourself.
6. **Delete your branch** after the merge.
