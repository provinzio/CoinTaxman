**PULL REQUESTS are appreciated.**

#### English

CoinTaxman helps you to bring your income from crypto trading, lending, ... into your tax declaration.
At the moment CoinTaxman only covers my area of ​​application.
Pull requests and requests via issues are welcome (see `Key notes for users` for more information).

#### German - Deutsch

CoinTaxman hilft dir dabei deine Einkünfte aus dem Krypto-Handel/-Verleih/... in die Steuererklärung zu bringen.
Momentan deckt der CoinTaxman nur meinen Anwendungsbereich ab.
Pull Requests und Anfragen über Issues sind gerne gesehen (siehe `Key notes for users` für weitere Informationen).

# **Disclaimer: use at your own risk**

### Currently supported countries
- Germany

### Currently supported exchanges
- [Binance](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Binance)
- [Bitpanda Pro](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Bitpanda-Pro)
- [coinbase (pro)](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-coinbase)
- [Kraken](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Kraken)

It is also possible to import a custom transaction history file.
See [here](https://github.com/provinzio/CoinTaxman/wiki/Custom-import-format) for more informations.
The format is based on the awesome `bittytax_conv`-tool from [BittyTax](https://github.com/BittyTax/BittyTax).

### Requirements

- Python 3.9
- See `requirements.txt` for the required modules.
Quick and easy installation can be done with `pip`.
> `pip install -r requirements.txt`

### Usage

1. Adjust `src/config.ini` to your liking
2. Add all your account statements in `account_statements/`
2. Run `python "src/main.py"`

If not all your exchanges are supported, you can not (directly) calculate your taxes with this tool.
You can use the custom import format [here](https://github.com/provinzio/CoinTaxman/wiki/Custom-import-format) to make it happen.

Have a look at our [Wiki](https://github.com/provinzio/CoinTaxman/wiki) for more information on how to obtain the account statement for your exchange.

#### Makefile

The Makefile offers multiple useful commands to quickly update the requirements, run the script, create a docker container, clean your code...

If you have `make` installed, you can use e.g. `make run` to execute the script.
Please have a look in the [Makefile](https://github.com/provinzio/CoinTaxman/blob/main/Makefile) for more information.

#### Run as docker container

Thanks to [jhoogstraat](https://github.com/jhoogstraat), you can also run CoinTaxman as a docker container.
The image is hosted on dockerhub: [jeppy/cointaxman:latest](https://hub.docker.com/r/jeppy/cointaxman).

### Supporting the development

Please consider supporting the development of this tool by either using my [Binance referral link](https://www.binance.com/en/register?ref=DS7C3HPD) if you want to create an account there or by donating to one of the adresses below.

BTC: `1AZMdztmZ8yZFDb5sdbWNp1wPT8gvvigwp`

ETH: `0x4868d10b8bc347b374300a7b924e3ffdf937ea0f`

### Key notes for users

### Requesting a new country

I would like to extend my tool for international usage.
However, the chances are high that I am unfamiliar with the local conditions neither able to speak the language.
I would not want to program the logic for something I can not look up properly.
Because in general and especially when it comes to state affairs, I prefer to look at the primary source.

I am happy to implement your input or support you with the implementation.
You can help by specifying detailed information about the taxation of crypto currency in the requested country.
Information I require are for example
- Country fiat (well that is easy... I might be able to figure it out by myself)
- Taxation of crypto sells
- Taxation from crypto lending
- Taxation of airdrops
- Are there special periods after which the sell is tax free?
- ...

Not every aspect has to be implemented directly.
We are free to start by implementing the stuff you need for your tax declaration.

I am looking forward to your [issue](https://github.com/provinzio/CoinTaxman/issues) or pull request.
Your country was already requested?
Hit the thumbs up button of that issue or participate in the process.

### Requesting a new exchange

The crypto world is huge and so is the amount of available exchanges out there.
I would love to make this tool as useful as possible for as many of you.

I am happy to implement your input or support you with the implementation.
Please provide an example account statement for the requested exchange or some other kind of documentation.

Are you already familiar with the API of that exchange or know some other way to request historical prices for that exchange?
Share your knowledge.

I am looking forward to your [issue](https://github.com/provinzio/CoinTaxman/issues) or pull request.
Your exchange was already requested?
Hit the thumbs up button of that issue or participate in the process.

### Key notes for developers

#### Adding a new country

- Add your country  to the Country enum in `src/core.py`
- Extend `src/config.py` to fit your tax regulation
- Add a country specific tax evaluation function in `src/taxman.py` like `Taxman._evaluate_taxation_GERMANY`
- Depending on your specific tax regulation, you might need to add additional functionality
- Update the README with a documentation about the taxation guidelines in your country

#### Adding a new exchange

- Add a read-in function like `Book._read_binance` in `src/book.py`
- Add a way to retrieve price data from this exchange in `src/price_data.py` like `PriceData._get_price_binance`
- Setup a wiki entry on how to retrieve the account statement and other useful information, which I can copy paste in our [Wiki](https://github.com/provinzio/CoinTaxman/wiki)

#### Managing python module dependencies

We use `pip-compile` from `pip-tools` to manage our python requirements, which separates "real" dependencies (e.g. dependencies/requirements.in) from all dependencies with there sub-dependencies (see requirements.txt).
To update the dependencies, please update the files in the dependencies folder and run `make build-deps` or the corresponding commands by hand (see Makefile).

# Ideas on crypto-taxation

The taxation of cryptocurrency is probably not yet regulated down to the smallest detail in every country. Often there still seem to be ambiguities or gray areas.
This section should help you to dig deeper into the subject.
Feel free to commit details about the taxation in your country.

## Taxation in Germany

Nach langer Unklarheit über die Besteuerung von Kryptowährung wurde am 10.05.2022 durch das Bundesministerium für Finanzen (BMF) [ein Schreiben](https://www.bundesfinanzministerium.de/Content/DE/Downloads/BMF_Schreiben/Steuerarten/Einkommensteuer/2022-05-09-einzelfragen-zur-ertragsteuerrechtlichen-behandlung-von-virtuellen-waehrungen-und-von-sonstigen-token.html) mit rechtsverbindlichen Vorgaben zur Versteuerung veröffentlicht.

Die ursprünglich hier stehenden Vermutungen und Interpretationen meinerseits habe ich aus der aktuellen `README.md` entfernt.
Für Interessierte findet sich das in der Versionshistorie.
Dieses Tool richtet sich nach bestem Wissen nach dem BMF Schreiben.

An dieser Stelle sei explizit erwähnt, dass ich trotzdem keine Haftung für die Anwendung dieses Programms übernehme.
Es ist weder sichergestellt, dass ich es aktuell noch nutze (falls ich das Repo in Zukunft nicht mehr aktiv pflege), noch ob die tatsächliche Umsetzung des Programms gesetzlich zulässig ist.
Issues und Pull Requests sind gerne gesehen.

Weitere Besonderheiten die sich so nicht im BMF-Schreiben wiederfinden, sind im folgenden aufgelistet.


### Binance Referral Rewards (Referral System)

Bei Binance gibt es die Möglichkeit, andere Personen über einen Link zu werben.
Bei jeder Transaktion der Person erhält man einen kleinen Anteil derer Gebühren als Reward gutgeschrieben.
Die Einkünfte durch diese Rewards werden durch CoinTaxman als Einkünfte aus sonstigen Leistungen ausgewiesen und damit wie eine übliche Kunden-werben-Kunden-Prämie erfasst.
