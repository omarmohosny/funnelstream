# Documentation build scripts

These two scripts rebuild the project documentation. They need Google Chrome (for the Mermaid diagrams and the PDF) and internet access (Mermaid is loaded from a CDN).

```powershell
# one time: installs the Markdown library into a local folder that is not part of the repository
.venv\Scripts\python -m pip install --target scripts\docs_build\pylibs markdown pygments
.venv\Scripts\python scripts\docs_build\make_diagrams.py     # redraws docs\images\diagrams\*.png (all, or name them: architecture quality)
.venv\Scripts\python scripts\docs_build\build_doc.py         # writes docs\FunnelStream-Project-Documentation.html and .pdf
```

If Chrome is installed elsewhere, set the environment variable `CHROME` to its path. `make_diagrams.py` also needs Pillow, which is in the project virtual environment.

The Grafana screenshots in `docs/images/grafana/` and the Power BI screenshots in `docs/images/powerbi/` are taken by hand or with headless Chrome (the recipe is in `docs/HANDOFF.md`). The terminal figures in `docs/images/terminal/` were rendered from captured command output and are committed as images.
