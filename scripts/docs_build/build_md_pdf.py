"""Build an .html and .pdf next to a Markdown file in docs/, in the page style of the project documentation.

    .venv\Scripts\python scripts/docs_build/build_md_pdf.py DEFENSE-QA "FunnelStream defense questions"
    .venv\Scripts\python scripts/docs_build/build_md_pdf.py DASHBOARD-GUIDE "FunnelStream dashboard guide"

The first argument is the file name in docs/ without .md, the second is the page footer text.
Pass "rtl" as a third argument for right-to-left text such as Arabic:

    .venv\Scripts\python scripts/docs_build/build_md_pdf.py DEFENSE-QA-AR "FunnelStream defense Q&A (Arabic)" rtl
"""
import io
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "pylibs"))
import markdown  # noqa: E402
import build_doc  # noqa: E402  (reuses its CSS and Chrome path)

EXTRA = """
h2{break-before:auto;font-size:15pt;margin-top:26px}
h1{font-size:24pt}
h3{break-before:auto;margin-top:22px}
@page{size:A4;margin:16mm 14mm 18mm 14mm;@bottom-center{content:"%s   |   page " counter(page);font:9pt Segoe UI,Arial;color:#5b6578}}
"""


RTL_CSS = """
body{direction:rtl;text-align:right;font-family:"Segoe UI",Tahoma,Arial,sans-serif;line-height:1.9}
ul,ol{padding-right:24px;padding-left:0}
code,pre{direction:ltr;unicode-bidi:embed;text-align:left}
"""


def main(name, footer, rtl=False):
    src = os.path.join(build_doc.DOCS, name + ".md")
    html_path = os.path.join(build_doc.DOCS, name + ".html")
    pdf_path = os.path.join(build_doc.DOCS, name + ".pdf")
    with io.open(src, encoding="utf-8") as fh:
        body = markdown.markdown(fh.read(), extensions=["tables", "fenced_code", "sane_lists"])
    html = ('<!doctype html><html lang="%s" dir="%s"><head><meta charset="utf-8"><title>%s</title><style>%s%s%s</style></head><body>%s</body></html>'
            % ("ar" if rtl else "en", "rtl" if rtl else "ltr", footer, build_doc.CSS, EXTRA % footer, RTL_CSS if rtl else "", body))
    with io.open(html_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(html)
    subprocess.run([build_doc.CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--print-to-pdf=" + pdf_path,
                    "--virtual-time-budget=20000", "file:///" + html_path.replace("\\", "/")], capture_output=True)
    print("pdf", os.path.getsize(pdf_path) if os.path.exists(pdf_path) else "missing")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], len(sys.argv) > 3 and sys.argv[3] == "rtl")
