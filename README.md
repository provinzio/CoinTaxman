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

### Future- / Margin-Trading

#### Unterscheidung Veräußerungsgeschäft / Termingeschäft

> Gewinne aus Future-Trades stellen in der Regel Einkünfte aus Kapitalvermögen dar und unterliegen damit der Kapitalertragsteuer. Maßgebend für die steuerliche Beurteilung ist allerdings weniger die von der Börse gewählte Begrifflichkeit, sondern vielmehr die konkrete Ausgestaltung des angebotenen Finanzprodukts. Im Einzelfall kann deshalb unter Umständen auch bei Futures ein privates Veräußerungsgeschäft gemäß § 23 EStG vorliegen, das zu einer Besteuerung nach dem persönlichen Einkommensteuersatz führt. Im Kern kommt es für die Abgrenzung darauf an, ob das Geschäft wie beim Spot Trading auf die Lieferung einer Kryptowährung abzielt (dann ist § 23 EStG einschlägig) oder ob die Lieferung lediglich einen Differenzausgleich darstellt (dann liegen Kapitaleinkünfte gemäß § 23 Abs. 2 Satz. 1 Nr. 3 EStG vor). [...]
> 
> Parallel dazu lassen sich die Überlegungen auf das Margin Trading übertragen, weshalb Gewinne aus Margin Trades immer nur dann unter Kapitaleinkünfte (§ 20 EStG) fallen, wenn keine Lieferung einer Kryptowährung, sondern ein Differenzausgleich durchgeführt wird. Kommt es hingegen zu einer Lieferung einer Kryptowährung, liegt ein privates Veräußerungsgeschäft gemäß § 23 Abs. 1 Satz 1 Nr. 2 EStG vor.
> 
> [...]
> 
> Stammen die erhaltenen Bitcoins aus einer Auszahlung resultierend aus einem Differenzausgleich, ist anschließend eine steuerfreie Veräußerung möglich. Da die Gewinne in Bitcoin bereits nach Maßgabe der Kapitalertragsteuer gemäß § 20 EStG versteuert wurden, greift auch keine Jahresfrist.
> 
> [...]
> 
> Solange die Position offen ist, muss diese auch nicht versteuert werden. Erst ab dem Zeitpunkt, in dem Investoren tatsächlich Einnahmen zugeflossen sind, findet die Besteuerung statt (sog. Zufluss-/Abflussprinzip).
> 
> [...]
> 
> Entstandene Gebühren fallen unter die sog. Werbungskosten. Da in den meisten Fällen beim Future Trading Kapitaleinkünfte vorliegen, sind die Werbungskosten bereits durch den Pauschbetrag in Höhe von 801 Euro (bzw. 1.602 Euro für verheiratete Paare) abgegolten. Die Gebühren können deshalb nicht gesondert steuerlich geltend gemacht werden, um die Steuerlast zu mindern.

[Quelle](https://winheller.com/blog/besteuerung-future-margin-trading/)
[Wörtlich zitiert vom 18.02.2022]

#### Fallbeispiel

> Person A schließt mit Person B einen Vertrag, der A das Recht einräumt, in Zukunft einen BTC von B zu erhalten, der momentan 35.000 Euro wert ist. Wird der Kontrakt fällig und A erhält von B den BTC, liegt ein Fall von § 23 Abs. 1 Satz 1 Nr. 2 EStG vor. Das heißt für B, dass er die Veräußerung des BTC mit seinem persönlichen Einkommensteuersatz versteuern muss. Für A bedeutet das hingegen, dass eine Anschaffung vorliegt und damit die Jahresfrist gilt.
> 
> Treffen A und B hingegen im oben geschilderten Fall die Vereinbarung, dass A am Ende des Vertrages die Wahl hat, ob er einen BTC bekommt oder alternativ den Gegenwert der Differenz zum aktuellen Kurs, dann liegt ein Termingeschäft gem. § 20 Abs. 2 Satz 1 Nr. 3 EStG vor. Das gilt auch dann, wenn die Differenz in BTC gezahlt wird. Steigt der Kurs von BTC am Ende des Kontrakts auf 37.000 Euro an, erhält A den Gegenwert der Differenz (37.000 Euro – 35.000 Euro = 2.000 Euro) in BTC. Der Gewinn muss von A pauschal mit 25 Prozent Kapitalertragsteuer versteuert werden, die Jahresfrist aus § 23 EStG greift hingegen nicht. Gegebenenfalls liegt bei B ein privates Veräußerungsgeschäft i.S.v. § 23 EStG vor.

[Quelle](https://hub.accointing.com/crypto-tax-regulations/germany/tax-break-germany-derivate-und-futures-winheller)
[Wörtlich zitiert vom 18.02.2022]

#### Werbungskosten für Termingeschäfte

> Ab dem VZ 2009 ist als Werbungskosten ein Betrag von 801 € bzw. 1 602 € bei Zusammenveranlagung abzuziehen (Sparer-Pauschbetrag, § 20 Abs. 9 EStG); der Abzug der tatsächlichen Werbungskosten ist ausgeschlossen. Die früheren Regelungen zum Werbungskosten Pauschbetrag und Sparer-Freibetrag wurden mit Einführung der Abgeltungsteuer aufgehoben.
> 
> [...]
> 
> In folgenden Fällen sind die Kosten auch ab 2009 weiterhin abzugsfähig:
> - Veräußerungskosten und Kosten in Zusammenhang mit Termingeschäften werden bei der Veräußerungsgewinnermittlung nach § 20 Abs. 4 EStG berücksichtigt.
> 
> [...]
> 

[Quelle](https://datenbank.nwb.de/Dokument/97088/)
[Wörtlich zitiert vom 18.02.2022]

#### Verrechnung von Verlusten aus Termingeschäften

> Während es vor dem 01.01.2021 möglich war, Verluste aus Termingeschäften uneingeschränkt mit den Einkünften aus Kapitalvermögen zu verrechnen, ist dies aufgrund des neu eingeführten § 20 Abs. 6 Satz 5 EStG seit 2021 nicht mehr ohne Weiteres möglich:
> 1. Verluste dürfen nur noch mit Gewinnen aus Termingeschäften und mit Erträgen aus Stillhaltergeschäften verrechnet werden.
> 2. Außerdem ist die Verlustverrechnung auf 20.000 Euro jährlich begrenzt.
> 
> Zwar können die nicht verrechneten Verluste in die Folgejahre vorgetragen werden. Aber auch dann ist eine Verlustverrechnung der Höhe nach auf 20.000 Euro pro Jahr begrenzt. Das führt faktisch zu einer Mindestbesteuerung von Gewinnen.

[Quelle](https://www.winheller.com/bankrecht-finanzrecht/bitcointrading/bitcoinundsteuer/verlustverrechnung.html)
[Wörtlich zitiert vom 18.02.2022]

#### Zusammenfassung

Zusammenfassung der Besteuerung des Margin-Tradings in meinen Worten:
- Gewinne/Verluste werden besteuert, sobald die Margin-Positionen ausgeglichen bzw. geschlossen werden
- Wird eine Margin-Position ausgeglichen ("settled"), d.h. die Kryptowährung wird zu Vertragsende zum Startpreis ge-/verkauft, liegt ein privates Veräußerungsgeschäft vor
  - Für private Veräußerungsgeschäfte gelten die oben angeführten Regeln, inklusive der einjährigen Haltefrist
- Wird eine Margin-Position geschlossen ("closed", Differenzausgleich), liegt ein Termingeschäft vor
  - Die Gewinne bzw. Verluste fallen unter Kapitaleinkünfte (§ 20 EStG)
  - Erhaltene Kryptowährung aus Differenzausgleichen kann steuerfrei veräußert werden
  - Es gibt keine einjährige Haltefrist
  - Gebühren können nur abgezogen werden, wenn der Freibetrag von 801 / 1602 Euro bereits ausgeschöpft wird
  - Die Verlustrechnung ist auf 20.000 Euro jährlich begrenzt und darf nicht mit Gewinnen aus privaten Veräußerungsgeschäften verrechnet werden
- Steht es dem Investor bis zum Ende offen, ob eine Margin-Position ausgeglichen ("settled") oder geschlossen ("closed") werden kann, liegt automatisch ein Termingeschäft vor und es gelten die gleichen Regelungen wie für geschlossene Positionen. Dies trifft für folgende Börsen zu:
  - Kraken