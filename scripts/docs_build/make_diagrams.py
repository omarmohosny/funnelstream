"""Render the documentation diagrams: Mermaid in headless Chrome, then trim the white margin with Pillow."""
import os
import subprocess
import sys

from PIL import Image, ImageChops

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "docs", "images", "diagrams")
TMP = os.path.join(os.environ["TEMP"], "fs_diagrams")
CHROME = os.environ.get("CHROME", r"C:\Program Files\Google\Chrome\Application\chrome.exe")
os.makedirs(OUT, exist_ok=True)
os.makedirs(TMP, exist_ok=True)

THEME = """%%{init: {'theme':'base','themeVariables':{'fontFamily':'Segoe UI, Arial','fontSize':'15px',
'primaryColor':'#e8f0fe','primaryBorderColor':'#1f4e9c','primaryTextColor':'#0d1b3d','lineColor':'#44546a',
'secondaryColor':'#fff4e0','tertiaryColor':'#eef7ee','clusterBkg':'#f7f9fc','clusterBorder':'#9fb3d1'}}}%%
"""

DIAGRAMS = {}

DIAGRAMS["architecture"] = (1400, 1500, """flowchart TB
  subgraph SRC["1. Sources (simulated)"]
    direction LR
    SIM["Simulator<br/>app events, orders, delivery,<br/>stock, injected noise"]
    ADS["Mock Meta and Google<br/>Ads API"]
  end
  subgraph KAF["2. Kafka, 4 topics"]
    direction LR
    T1["app.events"] ~~~ T2["orders"] ~~~ T3["delivery.events"] ~~~ T4["inventory.events"]
  end
  subgraph LAKE["3. Local Delta lake (Spark in Docker)"]
    direction LR
    BR["Bronze<br/>raw, append only"] --> SI["Silver<br/>validated, deduplicated,<br/>MERGE on key"]
    SI --> QU["silver_quarantine<br/>bad rows with a reason"]
  end
  subgraph LIVE["4a. Live lane: seconds to a minute"]
    direction LR
    SINK["Live sink<br/>after each Silver batch"] --> PG[("Postgres<br/>live and health tables")] --> GF["Grafana<br/>6 dashboards, 8 alert rules"]
  end
  subgraph BATCH["4b. Batch lane: daily"]
    direction LR
    PUB["Publish Silver<br/>allow-listed columns"] --> DBX[("Databricks<br/>Silver mirror and Gold<br/>12 tables, 66 checks")] --> PBI["Power BI<br/>7 pages, 103 measures"]
  end
  AF["Airflow DAG<br/>ads_yesterday_report"]
  MART["Daily marts in Postgres<br/>inventory, express, D-1 marketing"]
  SIM --> KAF
  KAF --> BR
  ADS --> AF --> DBX
  SI --> SINK
  SI --> PUB
  DBX --> MART
  PG --> MART
  MART --> PBI
""")

DIAGRAMS["layers"] = (1700, 900, """flowchart TB
  subgraph B["Bronze: raw, never changed"]
    b1["bronze_app_events"]
    b2["bronze_orders"]
    b3["bronze_delivery_events"]
    b4["bronze_inventory_events"]
  end
  subgraph S["Silver: trusted, one row per key"]
    s1["silver_events"]
    s2["silver_orders / silver_order_status<br/>silver_order_items"]
    s3["silver_stock_movements<br/>silver_stock_snapshots"]
    s4["silver_express_orders"]
    s5["silver_customers / silver_products<br/>silver_campaign_snapshots"]
    s6["silver_quarantine"]
  end
  subgraph G["Gold: business ready (Databricks)"]
    g1["dim_customer, dim_product<br/>(SCD2), dim_campaign, dim_date"]
    g2["fact_orders, fact_order_lines<br/>fact_attribution"]
    g3["agg_orders_daily, agg_funnel_daily<br/>agg_campaign_daily, agg_city_product_daily"]
    g4["customer_360 (rec_product: empty slot)"]
  end
  b1 --> s1
  b2 --> s2
  b3 --> s2
  b4 --> s3
  b2 --> s4
  b1 & b2 & b3 & b4 -.->|bad rows| s6
  s1 & s2 & s5 --> g1
  s2 --> g2
  s1 & s2 & s5 --> g3
  s1 & s2 --> g4
""")

DIAGRAMS["lifecycle"] = (1100, 1500, """stateDiagram-v2
  direction TB
  [*] --> order_created
  order_created --> payment_authorised: prepaid only
  order_created --> order_packed: cash on delivery
  payment_authorised --> order_packed
  order_packed --> order_shipped
  order_created --> order_cancelled: before shipment
  payment_authorised --> order_cancelled: before shipment
  order_packed --> order_cancelled: before shipment
  order_shipped --> out_for_delivery
  out_for_delivery --> delivered
  out_for_delivery --> delivery_failed
  delivery_failed --> out_for_delivery: retry
  delivery_failed --> order_returned: last attempt failed
  delivered --> order_completed
  order_completed --> [*]
  order_returned --> [*]
  order_cancelled --> [*]
""")

DIAGRAMS["star"] = (1500, 1000, """flowchart TB
  subgraph DIM["Four conformed dimensions"]
    direction LR
    D1["Date<br/>date_key"] ~~~ D2["Campaign<br/>campaign_id"] ~~~ D3["Store<br/>store_id"] ~~~ D4["Product<br/>product_id"]
  end
  subgraph GF["Facts from Databricks Gold"]
    F1["Orders Daily, Orders<br/>Campaign Daily, Funnel Daily<br/>City Product Daily, Order Lines"]
    F2["Customer, Customer 360<br/>(own columns, no relationship)"]
  end
  subgraph PF["Facts from Postgres marts"]
    F3["Stock Now<br/>Inventory Daily<br/>OOS Demand Daily"]
    F4["Express Daily<br/>Marketing D1 Campaign<br/>Marketing D1 Product"]
  end
  D1 -->|date_key / report_date| F1
  D2 -->|campaign_id| F1
  D1 -->|report_date| F3
  D3 -->|store_id| F3
  D4 -->|product_id| F3
  D2 -->|campaign_id| F4
  D3 -->|store_id| F4
  D4 -->|product_id| F4
  D1 -->|report_date| F4
""")

DIAGRAMS["inventory"] = (1200, 1900, """flowchart TB
  SIM["Simulator<br/>StockLedger, hero stock-out,<br/>express orders"] --> K["Kafka<br/>inventory.events"]
  K --> BR["Bronze<br/>bronze_inventory_events"]
  BR --> SM["Silver<br/>stock_movements<br/>stock_snapshots"]
  SM --> SINK["Live sink<br/>full recompute of stock"]
  SINK --> NOW["live_stock_now<br/>live_stock_store_minute"]
  SINK --> DEM["live_oos_demand_minute<br/>flag: out of stock at write time"]
  SINK --> EXP["live_express_orders"]
  NOW & DEM & EXP --> OPS["Grafana, operations<br/>live inventory, Express SLA,<br/>Live stock monitor"]
  OPS --> AL["Alerts<br/>fs-oos-demand, fs-express-sla,<br/>fs-express-ontime, fs-stock-outs"]
  DEM --> D1["Daily jobs<br/>build_inventory_daily<br/>build_d1_marketing"]
  DBX["Databricks<br/>agg_campaign_daily<br/>spend, ROAS, CPA"] --> D1
  D1 --> MK["Marketing D-1, inventory daily<br/>exposed spend = spend x<br/>out-of-stock clicks / served clicks"]
""")

DIAGRAMS["failure"] = (1500, 1100, """sequenceDiagram
  autonumber
  participant G as Generator
  participant K as Kafka
  participant P as Pipeline (Spark)
  participant D as Postgres
  participant A as Grafana alert
  G->>K: events keep arriving
  K->>P: Bronze then Silver micro-batches
  P->>D: live tables and heartbeat
  Note over P: docker kill (real crash)
  G->>K: events keep arriving
  D-->>A: heartbeat age grows, no new minute
  A-->>A: fs-silver-silent fires in 2 to 3 minutes
  Note over P: restart with the same checkpoints
  K->>P: backlog from the last committed offsets
  P->>D: tables catch up, heartbeat returns
  A-->>A: rule resolves
  P->>P: check_duplicates: NO DUPLICATES
""")

DIAGRAMS["quality"] = (1200, 1800, """flowchart TB
  E["Event from Kafka"] --> V{"Contract valid?<br/>known type and version,<br/>required fields, price above 0,<br/>not in the future"}
  V -->|no| Q["silver_quarantine<br/>error_code and detail"]
  V -->|yes| N["Normalise<br/>UTC time, city and district names,<br/>UTM values, numbers"]
  N --> DD{"Already seen?<br/>MERGE on event_id"}
  DD -->|yes| X["Ignored: replay is safe"]
  DD -->|no| S["Silver table"]
  S --> L{"Late by more than<br/>10 minutes?"}
  L -->|yes| LF["is_late = true<br/>kept, left out of live metrics"]
  L -->|no| OK["Counted in live metrics"]
  S --> BT{"Bot session?"}
  BT -->|yes| BF["is_bot = true<br/>left out of funnel and attribution"]
""")

DIAGRAMS["docker"] = (1500, 800, """flowchart TB
  subgraph H["Windows laptop, Docker Desktop (about 8 GB)"]
    subgraph U["Always on"]
      K["kafka 1 GB"]
      P["postgres 512 MB"]
      G["grafana 512 MB"]
    end
    subgraph S["Started for the pipeline"]
      SP["spark 4 GB<br/>one container runs Bronze and Silver"]
      SY["sync 1.5 GB<br/>publish, Gold, duplicate check"]
    end
    AF["airflow 2 GB (profile, off by default)"]
  end
  HOST["Host: Python 3.11 venv<br/>simulator, host tests, scripts"] --> K
  SP --> K
  SP --> P
  G --> P
  SY --> DB["Databricks workspace<br/>(cloud)"]
  PBI["Power BI Desktop"] --> DB
  PBI --> P
""")


def trim(path, pad=24):
    im = Image.open(path).convert("RGB")
    bg = Image.new("RGB", im.size, (255, 255, 255))
    box = ImageChops.difference(im, bg).getbbox()
    if box:
        box = (max(box[0] - pad, 0), max(box[1] - pad, 0), min(box[2] + pad, im.width), min(box[3] + pad, im.height))
        im = im.crop(box)
    im.save(path)


def main(only=None):
    for name, (w, h, src) in DIAGRAMS.items():
        if only and name not in only:
            continue
        html = os.path.join(TMP, name + ".html")
        with open(html, "w", encoding="utf-8") as fh:
            fh.write("""<!doctype html><meta charset="utf-8"><body style="margin:0;background:#fff">
<pre class="mermaid">%s%s</pre>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<script>mermaid.initialize({startOnLoad:true,securityLevel:'loose'});</script></body>""" % (THEME, src))
        png = os.path.join(OUT, name + ".png")
        subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
                        "--window-size=%d,%d" % (w, h), "--virtual-time-budget=20000", "--screenshot=" + png,
                        "file:///" + html.replace("\\", "/")], capture_output=True)
        trim(png)
        print(name, Image.open(png).size)


if __name__ == "__main__":
    main(sys.argv[1:] or None)
