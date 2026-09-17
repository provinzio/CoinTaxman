# CoinTaxman
# Copyright (C) 2021  Carsten Docktor <https://github.com/provinzio>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import configparser
import dataclasses
import datetime
import decimal

import log_config
from config import BASE_PATH

log = log_config.getLogger(__name__)

MIGRATIONS_FILE = BASE_PATH / "migrations.ini"


@dataclasses.dataclass(frozen=True)
class TokenMigrationConfig:
    source_coin: str
    target_coin: str
    ratio: decimal.Decimal
    effective_from: datetime.datetime
    max_delay: datetime.timedelta


def load_token_migrations() -> tuple[TokenMigrationConfig, ...]:
    if not MIGRATIONS_FILE.exists():
        raise ValueError(
            f"Token migration configuration file not found: {MIGRATIONS_FILE}"
        )

    config = configparser.ConfigParser()
    config.read(MIGRATIONS_FILE)

    migrations = []

    # Reject duplicate source_coin definitions while loading migrations.ini
    seen_source_coins = set()

    for section in config.sections():
        try:
            for required_key in (
                "source_coin",
                "target_coin",
                "ratio",
                "effective_from",
                "max_delay_hours",
            ):
                if required_key not in config[section]:
                    raise ValueError(f"{required_key} is required")

            source_coin = config[section]["source_coin"].upper()
            target_coin = config[section]["target_coin"].upper()
            ratio = decimal.Decimal(config[section]["ratio"])
            max_delay_hours_raw = config[section]["max_delay_hours"]
            max_delay_hours = int(max_delay_hours_raw)
            effective_from_raw = config[section]["effective_from"]

            if not source_coin:
                raise ValueError(f"{section}: source_coin is required")

            if not target_coin:
                raise ValueError(f"{section}: target_coin is required")

            if source_coin == target_coin:
                raise ValueError(
                    f"{section}: source_coin and target_coin must be different"
                )

            if source_coin in seen_source_coins:
                raise ValueError(
                    f"duplicate source_coin {source_coin!r} in migrations.ini"
                )

            if ratio <= 0:
                raise ValueError(f"{section}: ratio must be greater than 0")

            if ratio != ratio.to_integral_value():
                raise ValueError(f"{section}: ratio must be an integer")

            if max_delay_hours <= 0:
                raise ValueError(f"{section}: max_delay_hours must be greater than 0")

            if effective_from_raw.endswith("Z"):
                effective_from_raw = effective_from_raw[:-1] + "+00:00"

            effective_from = datetime.datetime.fromisoformat(effective_from_raw)

            if effective_from.tzinfo is None:
                log.warning(
                    "%s: effective_from has no timezone; assuming UTC",
                    section,
                )
                effective_from = effective_from.replace(tzinfo=datetime.timezone.utc)

            seen_source_coins.add(source_coin)

            migrations.append(
                TokenMigrationConfig(
                    source_coin,
                    target_coin,
                    ratio,
                    effective_from,
                    max_delay=datetime.timedelta(hours=max_delay_hours),
                )
            )

        except Exception as exc:
            raise ValueError(
                f"Invalid token migration configuration in "
                f"{MIGRATIONS_FILE}, section [{section}]: {exc}"
            ) from exc

    return tuple(migrations)
