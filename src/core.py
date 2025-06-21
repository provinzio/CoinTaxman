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

from enum import Enum, auto


class Country(Enum):
    GERMANY = auto()


class Principle(Enum):
    FIFO = auto()
    LIFO = auto()


class Fiat(Enum):
    """Symbols taken from https://www.xe.com/iso4217.php at 14.02.2021."""

    AED = "United Arab Emirates Dirham"
    AFN = "Afghanistan Afghani"
    ALL = "Albania Lek"
    AMD = "Armenia Dram"
    ANG = "Netherlands Antilles Guilder"
    AOA = "Angola Kwanza"
    ARS = "Argentina Peso"
    AUD = "Australia Dollar"
    AWG = "Aruba Guilder"
    AZN = "Azerbaijan Manat"
    BAM = "Bosnia and Herzegovina Convertible Mark"
    BBD = "Barbados Dollar"
    BDT = "Bangladesh Taka"
    BGN = "Bulgaria Lev"
    BHD = "Bahrain Dinar"
    BIF = "Burundi Franc"
    BMD = "Bermuda Dollar"
    BND = "Brunei Darussalam Dollar"
    BOB = "Bolivia Bolíviano"
    BRL = "Brazil Real"
    BSD = "Bahamas Dollar"
    BTN = "Bhutan Ngultrum"
    BWP = "Botswana Pula"
    BYN = "Belarus Ruble"
    BZD = "Belize Dollar"
    CAD = "Canada Dollar"
    CDF = "Congo/Kinshasa Franc"
    CHF = "Switzerland Franc"
    CLP = "Chile Peso"
    CNY = "China Yuan Renminbi"
    COP = "Colombia Peso"
    CRC = "Costa Rica Colon"
    CUC = "Cuba Convertible Peso"
    CUP = "Cuba Peso"
    CVE = "Cape Verde Escudo"
    CZK = "Czech Republic Koruna"
    DJF = "Djibouti Franc"
    DKK = "Denmark Krone"
    DOP = "Dominican Republic Peso"
    DZD = "Algeria Dinar"
    EGP = "Egypt Pound"
    ERN = "Eritrea Nakfa"
    ETB = "Ethiopia Birr"
    EUR = "Euro Member Countries"
    FJD = "Fiji Dollar"
    FKP = "Falkland Islands (Malvinas) Pound"
    GBP = "United Kingdom Pound"
    GEL = "Georgia Lari"
    GGP = "Guernsey Pound"
    GHS = "Ghana Cedi"
    GIP = "Gibraltar Pound"
    GMD = "Gambia Dalasi"
    GNF = "Guinea Franc"
    GTQ = "Guatemala Quetzal"
    GYD = "Guyana Dollar"
    HKD = "Hong Kong Dollar"
    HNL = "Honduras Lempira"
    HRK = "Croatia Kuna"
    HTG = "Haiti Gourde"
    HUF = "Hungary Forint"
    IDR = "Indonesia Rupiah"
    ILS = "Israel Shekel"
    IMP = "Isle of Man Pound"
    INR = "India Rupee"
    IQD = "Iraq Dinar"
    IRR = "Iran Rial"
    ISK = "Iceland Krona"
    JEP = "Jersey Pound"
    JMD = "Jamaica Dollar"
    JOD = "Jordan Dinar"
    JPY = "Japan Yen"
    KES = "Kenya Shilling"
    KGS = "Kyrgyzstan Som"
    KHR = "Cambodia Riel"
    KMF = "Comorian Franc"
    KPW = "Korea (North) Won"
    KRW = "Korea (South) Won"
    KWD = "Kuwait Dinar"
    KYD = "Cayman Islands Dollar"
    KZT = "Kazakhstan Tenge"
    LAK = "Laos Kip"
    LBP = "Lebanon Pound"
    LKR = "Sri Lanka Rupee"
    LRD = "Liberia Dollar"
    LSL = "Lesotho Loti"
    LYD = "Libya Dinar"
    MAD = "Morocco Dirham"
    MDL = "Moldova Leu"
    MGA = "Madagascar Ariary"
    MKD = "Macedonia Denar"
    MMK = "Myanmar (Burma) Kyat"
    MNT = "Mongolia Tughrik"
    MOP = "Macau Pataca"
    MRU = "Mauritania Ouguiya"
    MUR = "Mauritius Rupee"
    MVR = "Maldives (Maldive Islands) Rufiyaa"
    MWK = "Malawi Kwacha"
    MXN = "Mexico Peso"
    MYR = "Malaysia Ringgit"
    MZN = "Mozambique Metical"
    NAD = "Namibia Dollar"
    NGN = "Nigeria Naira"
    NIO = "Nicaragua Cordoba"
    NOK = "Norway Krone"
    NPR = "Nepal Rupee"
    NZD = "New Zealand Dollar"
    OMR = "Oman Rial"
    PAB = "Panama Balboa"
    PEN = "Peru Sol"
    PGK = "Papua New Guinea Kina"
    PHP = "Philippines Peso"
    PKR = "Pakistan Rupee"
    PLN = "Poland Zloty"
    PYG = "Paraguay Guarani"
    QAR = "Qatar Riyal"
    RON = "Romania Leu"
    RSD = "Serbia Dinar"
    RUB = "Russia Ruble"
    RWF = "Rwanda Franc"
    SAR = "Saudi Arabia Riyal"
    SBD = "Solomon Islands Dollar"
    SCR = "Seychelles Rupee"
    SDG = "Sudan Pound"
    SEK = "Sweden Krona"
    SGD = "Singapore Dollar"
    SHP = "Saint Helena Pound"
    SLL = "Sierra Leone Leone"
    SOS = "Somalia Shilling"
    # SPL* = "Seborga Luigino"
    SRD = "Suriname Dollar"
    STN = "São Tomé and Príncipe Dobra"
    SVC = "El Salvador Colon"
    SYP = "Syria Pound"
    SZL = "eSwatini Lilangeni"
    THB = "Thailand Baht"
    TJS = "Tajikistan Somoni"
    TMT = "Turkmenistan Manat"
    TND = "Tunisia Dinar"
    TOP = "Tonga Pa'anga"
    TRY = "Turkey Lira"
    TTD = "Trinidad and Tobago Dollar"
    TVD = "Tuvalu Dollar"
    TWD = "Taiwan New Dollar"
    TZS = "Tanzania Shilling"
    UAH = "Ukraine Hryvnia"
    UGX = "Uganda Shilling"
    USD = "United States Dollar"
    UYU = "Uruguay Peso"
    UZS = "Uzbekistan Som"
    VEF = "Venezuela Bolívar"
    VND = "Viet Nam Dong"
    VUV = "Vanuatu Vatu"
    WST = "Samoa Tala"
    XAF = "Communauté Financière Africaine (BEAC) CFA Franc BEAC"
    XCD = "East Caribbean Dollar"
    XDR = "International Monetary Fund (IMF) Special Drawing Rights"
    XOF = "Communauté Financière Africaine (BCEAO) Franc"
    XPF = "Comptoirs Français du Pacifique (CFP) Franc"
    YER = "Yemen Rial"
    ZAR = "South Africa Rand"
    ZMW = "Zambia Kwacha"
    ZWD = "Zimbabwe Dollar"


# Kraken's "dirty" asset codes don't seem to follow any system.
# Only "clean" names are used in CoinTaxman internally.
#     kraken_asset_map:
#         Converts Kraken "dirty" asset codes to "clean" asset names
#         (e.g. ZEUR   -> EUR)
#     kraken_pair_map:
#         Converts clean fiat / clean crypto pairs to "dirty" API asset pairs
#         (e.g. ETHEUR -> XETHZEUR)
# Analyzed using asset pairs API data:
# https://api.kraken.com/0/public/AssetPairs (retrieved at 2022-01-02)
kraken_asset_map = {
    # Fiat:
    "CHF": "CHF",
    "ZAUD": "AUD",
    "ZCAD": "CAD",
    "ZEUR": "EUR",
    "EUR.HOLD": "EUR",
    "ZGBP": "GBP",
    "ZJPY": "JPY",
    "ZUSD": "USD",
    # Crypto:
    "BTC": "XBT",
    "XETC": "ETC",
    "XETH": "ETH",
    "XLTC": "LTC",
    "XMLN": "MLN",
    "XREP": "REP",
    "XXBT": "XBT",
    "XXDG": "XDG",
    "XXLM": "XLM",
    "XXMR": "XMR",
    "XXRP": "XRP",
    "XZEC": "ZEC",
}

# Only these asset pairs violate the rule
# "clean name" + "clean name" = "asset pair"
kraken_pair_map = {
    "USDTUSD": "USDTZUSD",
    "ETCETH": "XETCXETH",
    "ETCXBT": "XETCXXBT",
    "ETCEUR": "XETCZEUR",
    "ETCUSD": "XETCZUSD",
    "ETH2ETH": "ETH2.SETH",
    "ETH2EUR": "XETHZEUR",
    "ETH2USD": "XETHZUSD",
    "ETHXBT": "XETHXXBT",
    "ETHCAD": "XETHZCAD",
    "ETHEUR": "XETHZEUR",
    "ETHGBP": "XETHZGBP",
    "ETHJPY": "XETHZJPY",
    "ETHUSD": "XETHZUSD",
    "LTCXBT": "XLTCXXBT",
    "LTCEUR": "XLTCZEUR",
    "LTCJPY": "XLTCZJPY",
    "LTCUSD": "XLTCZUSD",
    "MLNETH": "XMLNXETH",
    "MLNXBT": "XMLNXXBT",
    "MLNEUR": "XMLNZEUR",
    "MLNUSD": "XMLNZUSD",
    "REPETH": "XREPXETH",
    "REPXBT": "XREPXXBT",
    "REPEUR": "XREPZEUR",
    "REPUSD": "XREPZUSD",
    "XBTCAD": "XXBTZCAD",
    "XBTEUR": "XXBTZEUR",
    "XBTGBP": "XXBTZGBP",
    "XBTJPY": "XXBTZJPY",
    "XBTUSD": "XXBTZUSD",
    "XDGXBT": "XXDGXXBT",
    "XLMXBT": "XXLMXXBT",
    "XLMAUD": "XXLMZAUD",
    "XLMEUR": "XXLMZEUR",
    "XLMGBP": "XXLMZGBP",
    "XLMUSD": "XXLMZUSD",
    "XMRXBT": "XXMRXXBT",
    "XMREUR": "XXMRZEUR",
    "XMRUSD": "XXMRZUSD",
    "XRPXBT": "XXRPXXBT",
    "XRPCAD": "XXRPZCAD",
    "XRPEUR": "XXRPZEUR",
    "XRPJPY": "XXRPZJPY",
    "XRPUSD": "XXRPZUSD",
    "ZECXBT": "XZECXXBT",
    "ZECEUR": "XZECZEUR",
    "ZECUSD": "XZECZUSD",
    "EURUSD": "ZEURZUSD",
    "GBPUSD": "ZGBPZUSD",
    "USDCAD": "ZUSDZCAD",
    "USDJPY": "ZUSDZJPY",
}
