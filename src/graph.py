import collections
import logging
import time
from typing import Optional

import ccxt

log = logging.getLogger(__name__)


class PricePath:
    def __init__(
        self,
        exchanges: Optional[list[str]] = None,
        gdict: Optional[dict] = None,
        cache: Optional[dict] = None,
    ):
        if exchanges is None:
            exchanges = []
        if gdict is None:
            gdict = {}
        if cache is None:
            cache = {}

        self.gdict = gdict
        self.cache = cache

        # Saves the priority for a certain path so that bad paths can be skipped.
        self.priority: collections.defaultdict[str, int] = collections.defaultdict(int)
        allpairs: list[tuple[str, str, str, str]] = []

        for exchange_id in exchanges:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class()
            markets = exchange.fetch_markets()
            assert isinstance(markets, list)

            if exchange.has["fetchOHLCV"]:
                allpairs.extend(
                    [(i["base"], i["quote"], exchange_id, i["symbol"]) for i in markets]
                )
            else:
                logging.warning(
                    f"{exchange.name} does not support fetch ohlcv. "
                    f"Ignoring exchange and {len(markets)} pairs."
                )

        # Remove duplicate pairs.
        # TODO It might be faster to create it directly as set.
        #      Is it even necessary to convert it to a list?
        allpairs = list(set(allpairs))
        # print("Total Pairs to check:", len(allpairs))

        # Sorting by `symbol` to have the same result on every run due to the set.
        allpairs.sort(key=lambda x: x[3])

        for base, quote, exchange, symbol in allpairs:
            self.addVertex(base)
            self.addVertex(quote)
            self.addEdge(
                base, quote, {"exchange": exchange, "symbol": symbol, "inverted": False}
            )
            self.addEdge(
                quote, base, {"exchange": exchange, "symbol": symbol, "inverted": True}
            )

    def edges(self):
        return self.findedges()

    # Find the distinct list of edges

    def findedges(self):
        edgename = []
        for vrtx in self.gdict:
            for nxtvrtx in self.gdict[vrtx]:
                if {nxtvrtx, vrtx} not in edgename:
                    edgename.append({vrtx, nxtvrtx})
        return edgename

    def getVertices(self):
        return list(self.gdict.keys())

    # Add the vertex as a key
    def addVertex(self, vrtx):
        if vrtx not in self.gdict:
            self.gdict[vrtx] = []

    def addEdge(self, vrtx1, vrtx2, data):
        if vrtx1 in self.gdict:
            self.gdict[vrtx1].append((vrtx2, data))
        else:
            self.gdict[vrtx1] = [vrtx2]

    def _getpath(self, start, stop, maxdepth, depth=0):
        """
        a recursive function for finding all possible paths between to edges
        """
        paths = []
        if (edges := self.gdict.get(start)) and maxdepth > depth:
            for edge in edges:
                if depth == 0 and edge[0] == stop:
                    paths.append([edge])
                elif edge[0] == stop:
                    paths.append(edge)
                else:
                    path = self._getpath(edge[0], stop, maxdepth, depth=depth + 1)
                    if len(path) and path is not None:
                        for p in path:
                            if p[0] == stop:
                                newpath = [edge]
                                newpath.append(p)
                                paths.append(newpath)
        return paths

    def change_prio(self, key, value):
        ke = "-".join(key)
        self.priority[ke] += value

    def getpath(
        self, start, stop, starttime=0, stoptime=0, preferredexchange=None, maxdepth=3
    ):
        def comb_sort_key(path):
            """
            Sorting function which is used to prioritize paths by:
            (in order of magnitude)
            - smallest length -> +1 per element
            - preferred exchange -> +1 per exchange which is not preferred
            - priority -> +0.5 per unfinished execution of path
            - volume (if known) -> 1/sum(avg_vol per pair)
            - volume (if not known) -> 1 -> always smaller if volume is known
            """
            if preferredexchange:
                # prioritze pairs with the preferred exchange
                volume = 1
                volumenew = 0
                priority = self.priority.get(
                    "-".join([a[1]["symbol"] for a in path]), 0
                )
                xl = (a if (a := check_cache(pair)) else None for pair in path)
                for c in xl:
                    if c and c[0]:
                        if c[1][1]["stoptime"] == 0:
                            break
                        elif c[1][1]["avg_vol"] != 0:
                            # is very much off because volume is not in the same
                            # currency something for later
                            volumenew += c[1][1]["avg_vol"]

                    else:
                        break
                else:
                    volume = 1 / volumenew
                return (
                    len(path)
                    + sum(
                        [
                            0 if pair[1]["exchange"] == preferredexchange else 1
                            for pair in path
                        ]
                    )
                    + volume
                    + priority
                )
            else:
                return len(path)

        def check_cache(pair):
            """
            checking if the start and stoptime of a pair is already known
            or if it needs to be downloaded
            """
            if pair[1].get("starttime") or pair[1].get("stoptime"):
                return True, pair
            if cacheres := self.cache.get(pair[1]["exchange"] + pair[1]["symbol"]):
                pair[1]["starttime"] = cacheres[0]
                pair[1]["stoptime"] = cacheres[1]
                pair[1]["avg_vol"] = cacheres[2]
                return True, pair
            return False, pair

        def get_active_timeframe(path, starttimestamp=0, stoptimestamp=-1):
            rangeinms = 0
            timeframe = int(6.048e8)  # week in ms
            if starttimestamp == 0:
                starttimestamp = 1325372400 * 1000
            if stoptimestamp == -1:
                stoptimestamp = time.time_ns() // 1_000_000  # get cur time in ms
            starttimestamp -= timeframe  # to handle edge cases
            if stoptimestamp > starttimestamp:
                rangeinms = stoptimestamp - starttimestamp
            else:
                rangeinms = 0  # maybe throw error

            # add one candle to the end to ensure the needed
            # timeslot is in the requested candles
            rangeincandles = int(rangeinms / timeframe) + 1

            # todo: cache already used pairs
            globalstarttime = 0
            globalstoptime = 0
            for i in range(len(path)):
                cached, path[i] = check_cache(path[i])
                if not cached:
                    exchange_class = getattr(ccxt, path[i][1]["exchange"])
                    exchange = exchange_class()
                    # TODO maybe a more elaborate ratelimit wich removes execution
                    # time to from the ratelimit
                    time.sleep(exchange.rateLimit / 1000)
                    timeframeexchange = exchange.timeframes.get("1w")
                    if (
                        timeframeexchange
                    ):  # this must be handled better maybe choose timeframe dynamically
                        # maybe cache this per pair
                        ohlcv = exchange.fetch_ohlcv(
                            path[i][1]["symbol"], "1w", starttimestamp, rangeincandles
                        )
                    else:
                        ohlcv = []  # do not check fail later
                    if len(ohlcv) > 1:
                        # (candle ends after the date + timeframe)
                        path[i][1]["stoptime"] = ohlcv[-1][0] + timeframe
                        path[i][1]["avg_vol"] = sum([vol[-1] for vol in ohlcv]) / len(
                            ohlcv
                        )  # avg vol in curr
                        path[i][1]["starttime"] = ohlcv[0][0]
                        if (
                            path[i][1]["stoptime"] < globalstoptime
                            or globalstoptime == 0
                        ):
                            globalstoptime = path[i][1]["stoptime"]
                        if path[i][1]["starttime"] > globalstarttime:
                            globalstarttime = path[i][1]["starttime"]
                    else:
                        path[i][1]["stoptime"] = 0
                        path[i][1]["starttime"] = 0
                        path[i][1]["avg_vol"] = 0
                    self.cache[path[i][1]["exchange"] + path[i][1]["symbol"]] = (
                        path[i][1]["starttime"],
                        path[i][1]["stoptime"],
                        path[i][1]["avg_vol"],
                    )
                else:

                    if (
                        path[i][1]["stoptime"] < globalstoptime or globalstoptime == 0
                    ) and path[i][1]["stoptime"] != 0:
                        globalstoptime = path[i][1]["stoptime"]
                    if path[i][1]["starttime"] > globalstarttime:
                        globalstarttime = path[i][1]["starttime"]
                    ohlcv = []
            return (globalstarttime, globalstoptime), path

        # get all possible paths which are no longer than 4 pairs long
        paths = self._getpath(start, stop, maxdepth)
        # sort by path length to get minimal conversion chain to reduce error
        paths = sorted(paths, key=comb_sort_key)
        # get timeframe in which a path is viable
        for path in paths:
            timest, newpath = get_active_timeframe(path)
            # this is implemented as a generator (hence the yield) to reduce
            # the amount of computing needed. if the first path fails the next is used
            if starttime == 0 and stoptime == 0:
                yield timest, newpath
            elif starttime == 0:
                if stoptime < timest[1]:
                    yield timest, newpath
            elif stoptime == 0:
                if starttime > timest[0]:
                    yield timest, newpath
            # The most ideal situation is if the timerange of the path is known
            # and larger than the needed timerange
            else:
                if stoptime < timest[1] and starttime > timest[0]:
                    yield timest, newpath


if __name__ == "__main__":
    g = PricePath(exchanges=["binance", "coinbasepro"])
    start = "IOTA"
    to = "EUR"
    preferredexchange = "binance"
    path = g.getpath(start, to, maxdepth=2, preferredexchange=preferredexchange)
    # debug only in actual use we would iterate over
    # the path object fetching new paths as needed
    path = list(path)
    print(len(path))
