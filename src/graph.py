import collections
import logging
import time
import config
from typing import Optional

import ccxt

log = logging.getLogger(__name__)


class RateLimit:
    exchangedict = {}

    def limit(self, exchange):
        if lastcall := self.exchangedict.get(exchange.id):
            now = time.time()
            delay = exchange.rateLimit / 1000
            if exchange.name == "Kraken":
                delay += 2  # the reported ratelimit gets exceeded sometimes
            timepassed = now - lastcall
            if (waitfor := delay - timepassed) > 0:
                time.sleep(waitfor + 0.5)
            self.exchangedict[exchange.id] = time.time()
        else:
            self.exchangedict[exchange.id] = time.time()


class PricePath:
    def __init__(
        self,
        exchanges: Optional[list[str]] = None,
        gdict: Optional[dict] = None,
        cache: Optional[dict] = None,
    ):
        if exchanges is None:
            exchanges = list(config.EXCHANGES)
        if gdict is None:
            gdict = {}
        if cache is None:
            cache = {}

        self.gdict = gdict
        self.cache = cache
        self.RateLimit = RateLimit()

        # Saves the priority for a certain path so that bad paths can be skipped.
        self.priority: collections.defaultdict[str, int] = collections.defaultdict(int)
        allpairs: set(tuple[str, str, str, str]) = set()

        for exchange_id in exchanges:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class()
            markets = exchange.fetch_markets()
            assert isinstance(markets, list)

            if exchange.has["fetchOHLCV"]:
                toadd = [
                    (i["base"], i["quote"], exchange_id, i["symbol"]) for i in markets
                ]
                for pair in toadd:
                    allpairs.add(pair)
            else:
                logging.warning(
                    f"{exchange.name} does not support fetch ohlcv. "
                    f"Ignoring exchange and {len(markets)} pairs."
                )

        # Remove duplicate pairs.
        # TODO It might be faster to create it directly as set.
        #      Is it even necessary to convert it to a list?
        # allpairs = list(set(allpairs))
        allpairs = list(allpairs)
        # print("Total Pairs to check:", len(allpairs))

        # Sorting by `symbol` to have the same result on every run due to the set.
        allpairs.sort(key=lambda x: x[3])

        for base, quote, exchange, symbol in allpairs:
            self.add_Vertex(base)
            self.add_Vertex(quote)
            self.add_Edge(
                base, quote, {"exchange": exchange, "symbol": symbol, "inverted": False}
            )
            self.add_Edge(
                quote, base, {"exchange": exchange, "symbol": symbol, "inverted": True}
            )

    def edges(self):
        return self.find_edges()

    # Find the distinct list of edges

    def find_edges(self):
        edgename = []
        for vrtx in self.gdict:
            for nxtvrtx in self.gdict[vrtx]:
                if {nxtvrtx, vrtx} not in edgename:
                    edgename.append({vrtx, nxtvrtx})
        return edgename

    def get_Vertices(self):
        return list(self.gdict.keys())

    # Add the vertex as a key
    def add_Vertex(self, vrtx):
        if vrtx not in self.gdict:
            self.gdict[vrtx] = []

    def add_Edge(self, vrtx1, vrtx2, data):
        if vrtx1 in self.gdict:
            self.gdict[vrtx1].append((vrtx2, data))
        else:
            self.gdict[vrtx1] = [
                (vrtx2, data),
            ]

    def _get_path(self, start, stop, maxdepth, depth=0):
        """
        a recursive function for finding all possible paths between to vertices
        """
        paths = []
        if (edges := self.gdict.get(start)) and maxdepth > depth:
            for edge in edges:  # list of edges starting from the start vertice
                if depth == 0 and edge[0] == stop:
                    paths.append([edge])
                elif edge[0] == stop:
                    paths.append(edge)
                else:
                    path = self._get_path(edge[0], stop, maxdepth, depth=depth + 1)
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

    def get_path(
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
            # prioritze pairs with the preferred exchange
            volume = 1
            volumenew = 1
            priority = self.priority.get("-".join([a[1]["symbol"] for a in path]), 0)
            pathlis = (a if (a := check_cache(pair)) else None for pair in path)
            for possiblepath in pathlis:
                if possiblepath and possiblepath[0]:
                    if possiblepath[1][1]["stoptime"] == 0:
                        break
                    elif possiblepath[1][1]["avg_vol"] != 0:
                        # is very much off because volume is not in the same
                        # currency something for later
                        # volumenew*= volume of next thing in path (needs to be fixed for inverted paths)
                        volumenew *= possiblepath[1][1]["avg_vol"]

                else:
                    break
            else:
                volume = 1 / volumenew
            temppriority = volume + priority

            if preferredexchange:

                return (
                    len(path)
                    + sum(
                        [
                            0 if pair[1]["exchange"] == preferredexchange else 1
                            for pair in path
                        ]
                    )
                    + temppriority
                )
            else:
                return len(path) + temppriority

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

                    self.RateLimit.limit(exchange)
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
        paths = self._get_path(start, stop, maxdepth)
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
    path = g.get_path(start, to, maxdepth=2, preferredexchange=preferredexchange)
    # debug only in actual use we would iterate over
    # the path object fetching new paths as needed
    path = list(path)
    print(len(path))
