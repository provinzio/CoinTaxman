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

import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(None)

# Entferne die handler des basic loggers.
for handler in log.handlers:
    log.removeHandler(handler)

# Handler
ch = logging.StreamHandler()

# Formatter
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")

ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

# Disable urllib debug messages
logging.getLogger("urllib3").propagate = False
