import os
import re
from pathlib import Path
from zipfile import ZipFile

import log_config
from config import ACCOUNT_STATMENTS_PATH, BASE_PATH, DATA_PATH, EXPORT_PATH, TAX_YEAR

log = log_config.getLogger(__name__)
IGNORE_FILES = [".gitkeep"]
filepaths: list[Path] = []


def append_files(basedir: Path, filenames: list[str]) -> None:
    for filename in filenames:
        filepaths.append(Path(basedir, filename))


# Account statements
log.debug("Archive account statements")
account_statements = [
    f for f in os.listdir(ACCOUNT_STATMENTS_PATH) if f not in IGNORE_FILES
]
log.debug("Found: %s", ", ".join(account_statements))
append_files(ACCOUNT_STATMENTS_PATH, account_statements)

# Price database
log.debug("Archive price database")
price_databases = [
    f for f in os.listdir(DATA_PATH) if f.endswith(".db") and f not in IGNORE_FILES
]
log.debug("Found: %s", ", ".join(price_databases))
append_files(DATA_PATH, price_databases)

# Evaluation and log file
log.debug("Archive latest evaluation and log file")
eval_regex = re.compile(str(TAX_YEAR) + r"\_rev\d{3}\.csv")
evaluation = max((f for f in os.listdir(EXPORT_PATH) if eval_regex.match(f)))
log_file = evaluation.removesuffix(".csv") + ".log"
log.debug("Found: %s", ", ".join((evaluation, log_file)))
append_files(EXPORT_PATH, [evaluation, log_file])


archive_filepath = Path(
    EXPORT_PATH, f"CoinTaxman - Crypto Tax Evaluation - {TAX_YEAR}.zip"
)
log.debug("Zip files to %s", archive_filepath)
with ZipFile(archive_filepath, "w") as zip_file:
    for filepath in filepaths:
        zip_file.write(filepath, os.path.relpath(filepath, BASE_PATH))
