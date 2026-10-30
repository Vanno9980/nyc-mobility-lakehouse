# Databricks notebook source

import re
import shutil
import uuid

from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pyarrow.parquet as pq

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "nyc_mobility_av")
dbutils.widgets.text("start_month", "2025-01")
dbutils.widgets.text("max_files", "1")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
start_month = dbutils.widgets.get("start_month")
max_files = int(dbutils.widgets.get("max_files"))

if not catalog or not schema:
    raise ValueError("catalog e schema sono obbligatori")

if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", start_month):
    raise ValueError("start_month deve essere nel formato YYYY-MM")

if max_files < 1:
    raise ValueError("max_files deve essere maggiore di zero")

volume_root = Path(f"/Volumes/{catalog}/{schema}/landing")

landing_directory = volume_root / "yellow"

# Fuori dalla cartella osservata da Auto Loader.
staging_directory = volume_root / "_staging"

landing_directory.mkdir(parents=True, exist_ok=True)
staging_directory.mkdir(parents=True, exist_ok=True)

# COMMAND ----------

SOURCE_PAGE = (
    "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
)

ALLOWED_HOST = "d37ci6vzurychx.cloudfront.net"

FILE_PATTERN = re.compile(
    r"/trip-data/yellow_tripdata_(\d{4}-(?:0[1-9]|1[0-2]))\.parquet"
)


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for key, value in attrs:
                if key == "href" and value:
                    self.links.append(value)


def open_url(url):
    request = Request(
        url,
        headers={"User-Agent": "nyc-mobility-lakehouse/0.1"}
    )
    return urlopen(request, timeout=120)


def validate_parquet(path):
    # Verifica che il footer e lo schema Parquet siano leggibili.
    # Non esegue una scansione completa di tutti i record.
    with path.open("rb") as handle:
        metadata = pq.read_metadata(handle)

    if metadata.num_rows == 0:
        raise ValueError(f"Parquet vuoto: {path.name}")

    return metadata.num_rows

# COMMAND ----------

# Discovery dei file pubblicati.
with open_url(SOURCE_PAGE) as response:
    html = response.read().decode("utf-8")

parser = LinkParser()
parser.feed(html)

available = {}

for link in parser.links:
    url = urljoin(SOURCE_PAGE, link)
    parsed = urlparse(url)
    match = FILE_PATTERN.fullmatch(parsed.path)

    if (
        parsed.scheme == "https"
        and parsed.hostname == ALLOWED_HOST
        and match
    ):
        month = match.group(1)

        if month >= start_month:
            # Indicizzazione per mese: elimina eventuali link duplicati.
            available[month] = url

if not available:
    raise RuntimeError(
        "Nessun file trovato: controllare la pagina sorgente "
        "e il parametro start_month"
    )

# COMMAND ----------

# Incrementale: controlla i file già presenti.
pending = []
existing_count = 0

for month, url in sorted(available.items()):
    filename = f"yellow_tripdata_{month}.parquet"
    destination = landing_directory / filename

    if destination.exists():
        # Se un'esecuzione precedente ha lasciato un file incompleto,
        # fallisce prima di avviare Auto Loader.
        validate_parquet(destination)
        existing_count += 1
    else:
        pending.append((month, url, destination))

selected = pending[:max_files]

print(f"File disponibili dalla data iniziale: {len(available)}")
print(f"File già presenti: {existing_count}")
print(f"File mancanti: {len(pending)}")
print(f"File da scaricare in questa esecuzione: {len(selected)}")

# COMMAND ----------

for month, url, destination in selected:
    temporary = staging_directory / f"{uuid.uuid4().hex}.part"
    publishing = False

    print(f"Download: {destination.name}")

    try:
        with open_url(url) as response:
            expected_size = response.headers.get("Content-Length")

            with temporary.open("wb") as output:
                shutil.copyfileobj(
                    response,
                    output,
                    length=1024 * 1024
                )

        actual_size = temporary.stat().st_size

        if (
            expected_size is not None
            and actual_size != int(expected_size)
        ):
            raise ValueError(
                f"Download incompleto: {actual_size} byte, "
                f"attesi {expected_size}"
            )

        row_count = validate_parquet(temporary)

        # Non sovrascrive deliberatamente file già acquisiti.
        if destination.exists():
            raise RuntimeError(
                f"Il file è comparso durante il download: {destination}"
            )

        publishing = True
        shutil.copyfile(temporary, destination)

        if destination.stat().st_size != actual_size:
            raise ValueError("Copia nella landing incompleta")

        validate_parquet(destination)

        print(
            f"Completato: {destination.name} | "
            f"{actual_size / 1024**2:.2f} MiB | "
            f"{row_count:,} record"
        )

    except Exception:
        # Rimuove una pubblicazione incompleta in caso di errore gestito.
        if publishing:
            destination.unlink(missing_ok=True)
        raise

    finally:
        temporary.unlink(missing_ok=True)

print("Downloader completato")