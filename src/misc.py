# CoinTaxman
# Copyright (C) 2021  Carsten Docktor

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

import collections
import datetime
import random
import time
from typing import Optional, Tuple, Union

import core


def xint(i: int) -> Optional[int]:
    return None if i is None else int(i)


def to_ms_timestamp(d: datetime.datetime) -> int:
    """Return timestamp in milliseconds.

    Args:
        d (datetime.datetime)

    Returns:
        int: Timestamp in milliseconds.
    """
    return int(d.timestamp() * 1000)


def get_offset_timestamps(utc_time: datetime.datetime, offset: datetime.timedelta) -> Tuple[int, int]:
    """Return timestamps in milliseconds `offset/2` before/after `utc_time`.

    Args:
        utc_time (datetime.datetime)
        offset (datetime.timedelta)

    Returns:
        Tuple[int, int]: Timestamps in milliseconds.
    """
    start = utc_time - offset/2
    end = utc_time + offset/2
    return to_ms_timestamp(start), to_ms_timestamp(end)


def group_by(l: list[object], key: str) -> dict[str, list[object]]:
    """Group a list of objects by `key`.

    Args:
        l (list[object])
        key (str)

    Returns:
        dict[str, list[object]]: Dict with different `key`as keys.
    """
    d = collections.defaultdict(list)
    for e in l:
        d[getattr(e, key)].append(e)
    return dict(d)


__delayed = {}


def delayed(func):
    """Randomly delay calls to the same function."""
    def wrapper(*args, **kwargs):
        global __delayed
        if delayed := __delayed.get(id(func)):
            delayed_for = (delayed - datetime.datetime.now()).total_seconds()
            if delayed_for > 0:
                time.sleep(delayed_for)

        ret = func(*args, **kwargs)

        delay = random.uniform(0.2, 2)
        delay = datetime.timedelta(seconds=delay)
        __delayed[id(func)] = datetime.datetime.now() + delay

        return ret
    return wrapper


def is_fiat(symbol: Union[str, core.Fiat]) -> bool:
    """Check if `symbol` is a fiat.

    Args:
        fiat (str): Currency Symbol.

    Returns:
        bool: True if `symbol` is fiat. False otherwise.
    """
    return isinstance(symbol, core.Fiat) or symbol in core.Fiat.__members__
