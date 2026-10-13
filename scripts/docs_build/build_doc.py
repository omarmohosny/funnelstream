"""Build docs/FunnelStream-Project-Documentation.html and .pdf from the Markdown source."""
import io
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "pylibs"))
import markdown  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = os.path.join(ROOT, "docs")
SRC = os.path.join(DOCS, "FunnelStream-Project-Documentation.md")
HTML = os.path.join(DOCS, "FunnelStream-Project-Documentation.html")
PDF = os.path.join(DOCS, "FunnelStream-Project-Documentation.pdf")
CHROME = os.environ.get("CHROME", r"C:\Program Files\Google\Chrome\Application\chrome.exe")

CSS = """
:root{--navy:#0d366b;--blue:#2a78d6;--ink:#1c2433;--muted:#5b6578;--line:#d6dce8;--bg:#fff;--soft:#f3f6fb}
@page{size:A4;margin:16mm 14mm 18mm 14mm;
  @bottom-center{content:"FunnelStream project documentation   |   page " counter(page);font:9pt Segoe UI,Arial;color:#5b6578}}
*{box-sizing:border-box}
body{font:10.5pt/1.55 "Segoe UI",Arial,sans-serif;color:var(--ink);background:var(--bg);margin:0 auto;max-width:980px;padding:0 28px}
h1{font-size:30pt;color:var(--navy);margin:34px 0 4px;letter-spacing:-.5px}
h2{font-size:17pt;color:var(--navy);margin:34px 0 10px;padding-bottom:6px;border-bottom:2px solid var(--blue);break-after:avoid}
h3{font-size:13pt;color:var(--navy);margin:24px 0 6px;break-after:avoid}
h4{font-size:11.5pt;color:var(--blue);margin:20px 0 4px;break-after:avoid}
p{margin:8px 0}
a{color:var(--blue)}
hr{border:0;border-top:1px solid var(--line);margin:26px 0}
table{border-collapse:collapse;width:100%;margin:12px 0 16px;font-size:9.4pt;break-inside:auto}
th{background:var(--navy);color:#fff;text-align:left;padding:6px 9px;font-weight:600}
td{padding:6px 9px;border-bottom:1px solid var(--line);vertical-align:top}
tr:nth-child(even) td{background:var(--soft)}
tr{break-inside:avoid}
code{font:9.2pt Consolas,"Cascadia Mono",monospace;background:#eef2f9;padding:1px 4px;border-radius:3px}
pre{background:#0c0e14;color:#d7dae0;padding:12px 14px;border-radius:6px;overflow:auto;font-size:8.8pt;line-height:1.45;break-inside:avoid}
pre code{background:none;color:inherit;padding:0;font-size:inherit}
img{max-width:100%;max-height:228mm;width:auto;height:auto;display:block;margin:14px auto 4px;border:1px solid var(--line);border-radius:4px;break-inside:avoid}
p>em:only-child{display:block;text-align:center;color:var(--muted);font-size:9pt;margin:0 0 16px}
h2{break-before:page}
h1+p,h1+h2{break-before:auto}
.cover{padding:60px 0 20px}
@media print{body{max-width:none;padding:0}a{color:inherit;text-decoration:none}}
"""


def main():
    with io.open(SRC, encoding="utf-8") as fh:
        text = fh.read()
    body = markdown.markdown(text, extensions=["tables", "fenced_code", "toc", "sane_lists"],
                             extension_configs={"toc": {"permalink": False}})
    html = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>FunnelStream: project documentation</title><style>%s</style></head><body>%s</body></html>' % (CSS, body))
    with io.open(HTML, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(html)
    print("html", os.path.getsize(HTML))
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    "--print-to-pdf=" + PDF, "--virtual-time-budget=20000", "file:///" + HTML.replace("\\", "/")],
                   capture_output=True)
    print("pdf", os.path.getsize(PDF) if os.path.exists(PDF) else "missing")


if __name__ == "__main__":
    main()
