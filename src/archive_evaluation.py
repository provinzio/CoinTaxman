import os
import re
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import log_config
from config import (
    ACCOUNT_STATMENTS_PATH,
    BASE_PATH,
    CONFIG_FILE,
    DATA_PATH,
    EXPORT_PATH,
    TAX_YEAR,
)

log = log_config.getLogger(__name__)
IGNORE_FILES = [".gitkeep"]
filepaths: dict[Path, str] = {}


def append_files(
    basedir: Path, filenames: list[str], zip_filepaths: Optional[list[str]] = None
) -> None:
    """Save filepaths to the global filepaths dictionary for later output.

    Args:
        basedir (Path): Directory where the files are.
        filenames (list[str]): Name of the files including the extension.
        zip_filepaths (Optional[list[str]], optional): Alternative relative filepaths
                for the moved files in the output zip. Defaults to None (old relpaths
                will be kept).
    """
    new_filepaths = [Path(basedir, filename) for filename in filenames]

    if zip_filepaths is None:
        zip_filepaths = [
            os.path.relpath(filepath, BASE_PATH) for filepath in new_filepaths
        ]

    assert len(new_filepaths) == len(zip_filepaths)
    filepaths.update(dict(zip(new_filepaths, zip_filepaths)))


def append_file(filepath: Path) -> None:
    """Append a single file to the global filepaths dictionary.

    Args:
        filepath (Path): Filepath of the to be appended file.
    """
    filepaths[filepath] = os.path.relpath(filepath, BASE_PATH)


# Account statements
# Archive all files in the account statements folder.
log.debug("Archive account statements")
account_statements = [
    f for f in os.listdir(ACCOUNT_STATMENTS_PATH) if f not in IGNORE_FILES
]
log.debug("Found: %s", ", ".join(account_statements))
append_files(ACCOUNT_STATMENTS_PATH, account_statements)

# Price database
# Archive all .db-files in the data folder.
log.debug("Archive price database")
price_databases = [
    f for f in os.listdir(DATA_PATH) if f.endswith(".db") and f not in IGNORE_FILES
]
log.debug("Found: %s", ", ".join(price_databases))
append_files(DATA_PATH, price_databases)

# Evaluation and log file
# Save the evaluation with the highest number and the corresponding log file.
log.debug("Archive latest evaluation and log file")
eval_regex = re.compile(str(TAX_YEAR) + r"\_rev\d{3}\.xlsx")
evaluation = max((f for f in os.listdir(EXPORT_PATH) if eval_regex.match(f)))
log_file = evaluation.removesuffix(".xlsx") + ".log"
log.debug("Found: %s", ", ".join((evaluation, log_file)))
append_files(
    EXPORT_PATH,
    [evaluation, log_file],
    [f"CoinTaxman - Crypto Tax Report - {TAX_YEAR}.xlsx", "CoinTaxman.log"],
)

# Config file
log.debug("Archive config file")
if CONFIG_FILE.is_file():
    log.debug("Found: %s", CONFIG_FILE)
    append_file(CONFIG_FILE)

# Name of the archive
archive_filepath = Path(
    EXPORT_PATH, f"CoinTaxman - Crypto Tax Evaluation - {TAX_YEAR}.zip"
)
# Zip all files into the archive.
log.debug("Zip files to %s", archive_filepath)
with ZipFile(archive_filepath, "w") as zip_file:
    for filepath, zip_filepath in filepaths.items():
        zip_file.write(filepath, zip_filepath)
