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

import collections
import datetime
import decimal
import random
import re
import subprocess
import time
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    SupportsFloat,
    SupportsInt,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import core

F = TypeVar("F", bound=Callable[..., Any])
L = TypeVar("L", bound=list[Any])


def xint(x: Union[None, str, SupportsInt]) -> Optional[int]:
    return None if x is None or x == "" else int(x)


def xfloat(x: Union[None, str, SupportsFloat]) -> Optional[float]:
    return None if x is None or x == "" else float(x)


def xdecimal(
    x: Union[None, str, int, float, decimal.Decimal]
) -> Optional[decimal.Decimal]:
    """Convert to decimal, but make sure, that empty values return as None.

    Integer and floats are converted to strings first to receive
    "real" Decimal values.

    Args:
        x (Union[None, str, int, float, decimal.Decimal])

    Returns:
        Optional[decimal.Decimal]
    """
    if isinstance(x, (int, float)):
        x = str(x)
    assert x is None or isinstance(x, str) or isinstance(x, decimal.Decimal)
    return None if x is None or x == "" else decimal.Decimal(x)


def cdecimal(x: Union[None, str, int, float, decimal.Decimal]) -> decimal.Decimal:
    """Convert to decimal and change None to 0.

    See xdecimal for further informations.

    Args:
        x (Union[None, str, int, float, decimal.Decimal])

    Returns:
        decimal.Decimal
    """
    dec = xdecimal(x)
    return decimal.Decimal() if dec is None else dec


def dsum(__iterable: Iterable[decimal.Decimal]) -> decimal.Decimal:
    """Builtin sum function, which always returns a decimal.Decimal.

    Args:
        __iterable (Iterable[decimal.Decimal])

    Returns:
        decimal.Decimal
    """
    return decimal.Decimal(sum(__iterable))


def force_decimal(x: Union[None, str, int, float, decimal.Decimal]) -> decimal.Decimal:
    """Convert to decimal, but make sure, that empty values raise an error.

    See `xdecimal` for further informations.

    Args:
        x (Union[None, str, int, float, decimal.Decimal])

    Raises:
        ValueError: The given argument can not be parsed accordingly.

    Returns:
        decimal.Decimal
    """
    d = xdecimal(x)
    if isinstance(d, decimal.Decimal):
        return d
    else:
        raise ValueError(f"Could not parse `{d}` to decimal")


def reciprocal(d: decimal.Decimal) -> decimal.Decimal:
    return decimal.Decimal() if d == 0 else decimal.Decimal(1) / d


def to_ms_timestamp(d: datetime.datetime) -> int:
    """Return timestamp in milliseconds.

    Args:
        d (datetime.datetime)

    Returns:
        int: Timestamp in milliseconds.
    """
    return int(d.timestamp() * 1000)


def to_ns_timestamp(d: datetime.datetime) -> int:
    """Return timestamp in nanoseconds.

    Args:
        d (datetime.datetime)

    Returns:
        int: Timestamp in nanoseconds.
    """
    return int(d.timestamp() * 1000000000)


def to_decimal_timestamp(d: datetime.datetime) -> decimal.Decimal:
    return decimal.Decimal(d.timestamp())


def get_offset_timestamps(
    utc_time: datetime.datetime,
    offset: datetime.timedelta,
) -> Tuple[int, int]:
    """Return timestamps in milliseconds `offset/2` before/after `utc_time`.

    Args:
        utc_time (datetime.datetime)
        offset (datetime.timedelta)

    Returns:
        Tuple[int, int]: Timestamps in milliseconds.
    """
    start = utc_time - offset / 2
    end = utc_time + offset / 2
    return to_ms_timestamp(start), to_ms_timestamp(end)


def to_iso_timestamp(d: datetime.datetime) -> str:
    """Return timestamp as ISO8601 timestamp.

    Args:
        d (datetime.datetime)

    Returns:
        str: ISO8601 timestamp.
    """
    return d.isoformat().replace("+00:00", "Z")


def parse_iso_timestamp(d: str) -> datetime.datetime:
    """Parse a ISO8601 timestamp, return a datetime object.

    Args:
        d (str) A string in ISO8601 format

    Returns:
        datetime.datetime: The datetime object representing the string.
    """
    # make RFC3339 timestamp ISO 8601 parseable
    if d[-1] == "Z":
        d = d[:-1] + "+00:00"

    # timezone information is already taken care of with this
    return datetime.datetime.fromisoformat(d)


def parse_iso_timestamp_to_decimal_timestamp(d: str) -> decimal.Decimal:
    return to_decimal_timestamp(datetime.datetime.fromisoformat(d))


def group_by(lst: L, key: Union[str, list[str]]) -> dict[Any, L]:
    """Group a list of objects by `key`.

    Args:
        lst (L)
        key (Union[str, list[str]])

    Returns:
        dict[Any, L]: Dict with different `key`as keys.
    """
    d = collections.defaultdict(list)
    if isinstance(key, str):
        for e in lst:
            d[getattr(e, key)].append(e)
    elif isinstance(key, list):
        assert all(isinstance(k, str) for k in key)
        for e in lst:
            d[tuple(getattr(e, k) for k in key)].append(e)
    else:
        raise TypeError
    return dict(d)


T = TypeVar("T")


def sort_by_order_and_key(
    order: list[Type[T]],
    list_: list[T],
    keys: Optional[list[str]] = None,
) -> list[T]:
    """Sort a list by list of existing types and arbitrary keys/members.

    If the type is missing in order list. The entry will be placed first.

    Args:
        order (list[Type[T]]): List with types in correct order.
        operations (list[T]): List with entries to be sorted.
        keys (list[str], optional): List of members which will be considered
                                    when sorting. Defaults to None.

    Returns:
        list[T]: Sorted entries by `order` and specific keys.
    """

    def key_function(op: T) -> tuple:
        try:
            idx = order.index(type(op))
        except ValueError:
            idx = 0
        return tuple(([getattr(op, key) for key in keys] if keys else []) + [idx])

    return sorted(list_, key=key_function)


__delayed: dict[int, datetime.datetime] = {}


def delayed(func: F) -> F:
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

    return cast(F, wrapper)


def is_fiat(symbol: Union[str, core.Fiat]) -> bool:
    """Check if `symbol` is a fiat currency.

    Args:
        fiat (Union[str, core.Fiat]): Currency Symbol.

    Returns:
        bool: True if `symbol` is a fiat currency. False otherwise.
    """
    return isinstance(symbol, core.Fiat) or symbol in core.Fiat.__members__


def get_next_file_path(
    path: Path, base_filename: str, extensions: Union[str, list[str]]
) -> Path:
    """Looking for the next free filename in format {base_filename}_revXXX.

    The revision number starts with 001 and will always be +1 from the highest
    existing revision.

    Args:
        path (Path)
        base_filename (str)
        extension (Union[str, list[str]])

    Raises:
        AssertitionError: When {base_filename}_rev999.{extension}
                          already exists.

    Returns:
        Path: Path to next free file.
    """
    i = 1
    extensions = [extensions] if isinstance(extensions, str) else extensions
    extension = extensions[0]
    regex = re.compile(base_filename + r"_rev(\d{3}).(" + "|".join(extensions) + ")")

    for p in path.iterdir():
        if p.is_file():
            if m := regex.match(p.name):
                j = int(m.group(1)) + 1
                if j > i:
                    i = j

    assert i < 1000

    file_path = Path(path, f"{base_filename}_rev{i:03d}.{extension}")
    assert not file_path.exists()
    return file_path


def get_current_commit_hash(default: Optional[str] = None) -> str:
    try:
        output = subprocess.check_output(["git", "rev-parse", "HEAD"])
        commit = output.decode()
        commit = commit.strip()
        return commit
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        if default is None:
            raise RuntimeError("Unable to determine commit hash") from e
        return default


def not_none(v: Optional[T]) -> T:
    if v is None:
        raise ValueError()
    return v


def column_num_to_string(n: int) -> str:
    # References: https://stackoverflow.com/a/63013258/8979290
    n, rem = divmod(n - 1, 26)
    char = chr(65 + rem)
    if n:
        return column_num_to_string(n) + char
    else:
        return char


def column_string_to_num(s: str) -> int:
    # References: https://stackoverflow.com/a/63013258/8979290
    n = ord(s[-1]) - 64
    if s[:-1]:
        return 26 * (column_string_to_num(s[:-1])) + n
    else:
        return n
