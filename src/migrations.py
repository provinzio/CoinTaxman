import configparser
import dataclasses
import datetime
import decimal

from pathlib import Path


BASE_PATH = Path(__file__).parent.parent.absolute()
MIGRATIONS_FILE = BASE_PATH / "migrations.ini"


@dataclasses.dataclass(frozen=True)
class TokenMigrationConfig:
    source_coin: str
    target_coin: str
    ratio: decimal.Decimal
    effective_from: datetime.datetime
    max_delay: datetime.timedelta


def load_token_migrations() -> tuple[TokenMigrationConfig, ...]:
    config = configparser.ConfigParser()
    config.read(MIGRATIONS_FILE)

    migrations = []

    for section in config.sections():
        migrations.append(
            TokenMigrationConfig(
                source_coin=config[section]["source_coin"].upper(),
                target_coin=config[section]["target_coin"].upper(),
                ratio=decimal.Decimal(config[section]["ratio"]),
                effective_from=datetime.datetime.fromisoformat(
                    config[section]["effective_from"]
                ),
                max_delay=datetime.timedelta(
                    hours=config[section].getint(
                        "max_delay_hours",
                        fallback=72,
                    )
                ),
            )
        )

    return tuple(migrations)


TOKEN_MIGRATIONS = load_token_migrations()