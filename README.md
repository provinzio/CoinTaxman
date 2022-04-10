**PULL REQUESTS are appreciated.**

#### English

CoinTaxman helps you to bring your income from crypto trading, lending, ... into your tax declaration.
At the moment CoinTaxman only covers my area of ​​application.
Pull requests and requests via issues are welcome (see `Key notes for users` for more information).

#### German - Deutsch

CoinTaxman hilft dir dabei deine Einkünfte aus dem Krypto-Handel/-Verleih/... in die Steuererklärung zu bringen.
Momentan deckt der CoinTaxman nur meinen Anwendungsbereich ab.
Pull Requests und Anfragen über Issues sind gerne gesehen (siehe `Key notes for users` für weitere Informationen).

**Disclaimer: use at your own risk**

### Currently supported countries
- Germany

### Currently supported exchanges
- [Binance](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Binance)
- [Bitpanda Pro](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Bitpanda-Pro)
- [coinbase (pro)](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-coinbase)
- [Kraken](https://github.com/provinzio/CoinTaxman/wiki/Exchange:-Kraken)

### Requirements

- Python 3.9
- See `requirements.txt` for the required modules.
Quick and easy installation can be done with `pip`.
> `pip install -r requirements.txt`

### Usage

1. Adjust `src/config.py` to your liking
2. Add account statements from supported exchanges in `account_statements/`
2. Run `python "src/main.py"`

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

I am looking forward to your [issue](https://github.com/provinzio/CoinTaxman/issues).
Your country was already requested?
Hit the thumbs up button of that issue or participate in the process.

### Requesting a new exchange

The crypto world is huge and so is the amount of available exchanges out there.
I would love to make this tool as useful as possible for as many of you.

I am happy to implement your input or support you with the implementation.
Please provide an example account statement for the requested exchange or some other kind of documentation.

Are you already familiar with the API of that exchange or know some other way to request historical prices for that exchange?
Share your knowledge.

I am looking forward to your [issue](https://github.com/provinzio/CoinTaxman/issues).
Your exchange was already requested?
Hit the thumbs up button of that issue or participate in the process.

### Key notes for developers

#### Adding a new country

- Add your country  to the Country enum in `src/core.py`
- Extend `src/config.py` to fit your tax regulation
- Add a country specific tax evaluation function in `src/taxman.py` like `Taxman._evaluate_taxation_GERMANY`
- Depending on your specific tax regulation, you might need to add additional functionality and might want to add or edit the enums in `src/core.py`
- Update the README with a documentation about the taxation guidelines in your country

#### Adding a new exchange

- Add a read-in function like `Book._read_binance` in `src/book.py`
- Add a way to retrieve price data from this exchange in `src/price_data.py` like `PriceData._get_price_binance`
- Setup a wiki entry on how to retrieve the account statement and other useful information, which I can copy paste in our [Wiki](https://github.com/provinzio/CoinTaxman/wiki)

# Ideas on crypto-taxation

The taxation of cryptocurrency is probably not yet regulated down to the smallest detail in every country. Often there still seem to be ambiguities or gray areas.
This section should help you to dig deeper into the subject.
Feel free to commit details about the taxation in your country.

## Taxation in Germany

Meine Interpretation rund um die Besteuerung von Kryptowährung in Deutschland wird durch die Texte von den [Rechtsanwälten und Steuerberatern WINHELLER](https://www.winheller.com/) sehr gut artikuliert.
Meine kurzen Zusammenfassungen am Ende von jedem Abschnitt werden durch einen ausführlicheren Text von [WINHELLER](https://www.winheller.com/) ergänzt.

An dieser Stelle sei explizit erwähnt, dass dies meine Interpretation ist. Es ist weder sichergestellt, dass ich aktuell noch nach diesen praktiziere (falls ich das Repo in Zukunft nicht mehr aktiv pflege), noch ob diese Art der Versteuerung gesetzlich zulässig ist.
Meine Interpretation steht gerne zur Debatte.

### Allgemein

> Kryptowährungen sind kein gesetzliches Zahlungsmittel. Vielmehr werden sie – zumindest im Ertragsteuerrecht – als immaterielle Wirtschaftsgüter betrachtet. 
>
> Wird der An- und Verkauf von Kryptowährungen als Privatperson unternommen, sind § 22 Nr. 2, § 23 Abs. 1 Nr. 2 EStG einschlägig. Es handelt sich hierbei um ein privates Veräußerungsgeschäft von „anderen Wirtschaftsgütern“. Gemäß § 23 Abs. 3 Satz 1 EStG ist der Gewinn oder Verlust der Unterschied zwischen Veräußerungspreis einerseits und den Anschaffungs- und Werbungskosten andererseits. Es muss also nur der Anschaffungspreis vom Veräußerungspreis abgezogen werden. Die Gebühren beim Handel auf den Börsen sind Werbungskosten und damit abzugsfähig.
>
> In § 23 Abs. 3 Satz 5 EStG ist zudem eine Freigrenze von 600 € vorgesehen, bis zu deren Erreichen alle privaten Veräußerungsgeschäfte des Veranlagungszeitraums steuerfrei bleiben. Wird die Grenze überschritten, muss allerdings der gesamte Betrag ab dem ersten Euro versteuert werden. Die Einkommensteuer fällt dabei nicht erst beim Umtausch von Kryptowährungen in Euro oder eine andere Fremdwährung an, sondern bereits bei einem Tausch in eine beliebige andere Kryptowährung oder auch beim Kauf von Waren oder Dienstleistungen mit einer solchen. Vergeht aber zwischen Anschaffung und Veräußerung mehr als ein Jahr (ggf. zehn Jahre nach § 23 Abs. 1 Nr. 2 Satz 4 EStG), greift die Haltefrist des § 23 Abs. 1 Nr. 2 Satz 1 EStG. In diesen Fällen ist der gesamte Veräußerungsgewinn nicht steuerbar.  
>
> Zur Bestimmung der Anschaffungskosten und des Veräußerungsgewinns sowie zur Bestimmung der Einhaltung der Haltefrist wird in der Regel die sogenannte FIFO-Methode aus § 23 Abs. 1 Nr. 2 Satz 3 EStG herangezogen. Zwar schreibt das Gesetz diese First-In-First-Out-Methode nicht für Kryptowährungen vor, in der Praxis wird sie aber weitgehend angewendet. Es werden allerdings auch andere Meinungen vertreten und eine Berechnung nach der LIFO-Methode oder – zur Bestimmung der Anschaffungskosten – nach Durchschnittswerten vorgeschlagen.

[Quelle](https://www.winheller.com/bankrecht-finanzrecht/bitcointrading/bitcoinundsteuer/besteuerung-kryprowaehrungen.html)
[Wörtlich zitiert vom 14.02.2021]

Zusammenfassung in meinen Worten:
- Kryptowährung sind immaterielle Wirtschaftsgüter.
- Der Verkauf innerhalb eines Jahres gilt als privates Veräußerungsgeschäft und ist als Sonstiges Einkommen zu versteuern (Freigrenze 600 €).
- Der Tausch von Kryptowährung wird ebenfalls versteuert.
- Gebühren zum Handel sind steuerlich abzugsfähig.
- Es kann einmalig entschieden werden, ob nach FIFO oder LIFO versteuert werden soll.

Weitere Grundsätze, die oben nicht angesprochen wurden:
- Versteuerung erfolgt separat getrennt nach Depots (Wallets, Exchanges, etc.) [cryptotax](https://cryptotax.io/fifo-oder-lifo-bitcoin-gewinnermittlung/).

### Airdrops

> Im Rahmen eines Airdrops erhält der Nutzer Kryptowährungen, ohne diese angeschafft oder eine sonstige Leistung hierfür erbracht zu haben. Die Kryptowährungen werden nicht aus dem Rechtskreis eines Dritten auf den Nutzer übertragen. Vielmehr beginnen sie ihre „Existenz“ überhaupt erst in dessen Vermögen. Die Kryptowährung entsteht direkt in den Wallets der Nutzer, wobei die Wallets bestimmte Kriterien erfüllen müssen. Airdrops ähneln insofern einem Lottogewinn oder einem Zufallsfund (sog. Windfall Profits).
>
> Mangels Anschaffungsvorgangs kommt bei einer anschließenden Veräußerung eine Besteuerung nach § 23 Abs. 1 Nr. 2 Einkommensteuergesetz (EStG) nicht in Betracht. Mangels Leistungserbringung seitens des Nutzers liegen auch keine sonstigen Einkünfte i.S.d. § 22 Nr. 3 EStG vor. Damit ist der Verkauf von Airdrops steuerfrei.

[Quelle](https://www.winheller.com/bankrecht-finanzrecht/bitcointrading/bitcoinundsteuer/besteuerung-airdrops.html)
[Wörtlich zitiert vom 14.02.2021]

Zusammenfassung in meinen Worten:
- Erhalt und Verkauf von Airdrops ist steuerfrei.

### Coin Lending

> Handelt es sich bei den durch Krypto-Lending erhaltenen Zinserträgen um Einkünfte aus sonstigen Leistungen gem. § 22 Nr. 3 EStG, so gilt eine Freigrenze von 256 Euro. Beträge darüber werden mit dem perönlichen Einkommensteuersatz von 18 bis 45 % versteuert. Außerdem wäre die spätere Veräußerung gem. § 23 Abs. 1 Nr. 2 EStG der durch das Lending erlangten Kryptowährung mangels Anschaffungsvorgangs nicht steuerbar.
>
> In Deutschland ist die Besteuerung der durch das Krypto-Lending erhaltenen Zinsen jedoch nicht abschließend geklärt. Zum einem wird diskutiert, ob es sich dabei um Kapitaleinkünfte gem. § 20 Abs. 1 Nr. 7 EStG handelt, da es sich bei der Hingabe der Kryptowährung um ein klassisches, verzinsliches Darlehen handelt. Anderseits wird von Finanzämtern immer wieder angenommen, dass es sich bei den erzielten Zinserträgen durch Lending um Einkünfte aus sonstigen Leistungen gem. § 22 Nr. 3 EStG handelt.
>
> Die erhaltene Kryptowährung in Form von Zinsen ist im Zeitpunkt des Zuflusses zu bewerten. Es handele sich deshalb nicht um Kapitaleinkünfte, da die Hingabe der Kryptowährung gerade keine Hingabe von Kapital, sondern vielmehr eine Sachleistung darstelle. Begründet wird dies damit, dass sich eine Kapitalforderung auf eine Geldleistung beziehen muss, nicht aber auf eine Sachleistung, wie es bei Kryptowährungen der Fall ist.
>
> Kontrovers diskutiert wird auch, ob der Verleih einer Kryptowährung zu einer Verlängerung der Haltefrist nach § 23 Abs. 1 Nr. 2 Satz 4 EStG führt. Eine Verlängerung der Haltefrist tritt danach nur dann ein, wenn ein Wirtschaftsgut
> - nach dem 31.12.08 angeschafft wurde,
> - als Einkunftsquelle genutzt wird und
> - damit Einkünfte erzielt werden.
> Unter Nutzung als Einkunftsquelle ist zu verstehen, dass die betroffenen Wirtschaftsgüter eine eigenständige Erwerbsgrundlage bilden. Maßgeblich ist also die Frage, ob mit der Kryptowährung Einkünften erzielt werden.
> 
> Beim Lending werden jedoch in der Regel keine Einkünfte aus dem Wirtschaftsgut (der Kryptowährung), sondern aus dem Verleihgeschäft erzielt (als Ertrag aus der Forderung). Weil in diesen Fällen kein Missbrauch vorliegt, kann es bei der Haltefrist von einem Jahr bleiben. Auch das Bayrische Landesamt für Steuern hat bestätigt, dass die erhaltenen Zinsen nicht Ausfluss des „anderen Wirtschaftsgutes Fremdwährungsguthaben“, sondern vielmehr Ausfluss der eigentlichen Kapitalforderungen sind.

[Quelle](https://www.winheller.com/bankrecht-finanzrecht/bitcointrading/bitcoinundsteuer/besteuerung-lending.html)
[Wörtlich zitiert vom 14.02.2021]

Zusammenfassung in meinen Worten:
- Erhaltene Kryptowährung durch Coin Lending wird im Zeitpunkt des Zuflusses als Einkunft aus sonstigen Leistungen versteuert (Freigrenze 256 €).
- Der Verkauf ist nicht steuerbar.
- Coin Lending beeinflusst nicht die Haltefrist der verliehenen Coins.

### Staking

> Ebenso [wie beim Coin Lending] verhält es sich bei Kryptowährungen, die für Staking oder Masternodes genutzt werden. Nutzer müssen bei proof-of-stake-basierten Kryptowährungen oder beim Betreiben von Masternodes einen bestimmten Teil ihrer Kryptowährung der Verfügungsmacht entziehen und dem Netzwerk als Sicherheit bereitstellen. Die Sicherheit des Netzwerkes wird dadurch gewährleistet, dass regelwidriges Verhalten den dem Verlust der Sicherheitsleistung (Kryptowährung) zur Folge hat. Auch in diesen Fällen werden keine Einkünfte aus dem Wirtschaftsgut selbst, sondern für das Blockieren der Verfügungsmacht, also als Ertrag aus der Forderung, erzielt. Auch hier bleibt es bei der Haltefrist von einem Jahr.

[Quelle](https://www.winheller.com/bankrecht-finanzrecht/bitcointrading/bitcoinundsteuer/besteuerung-von-staking.html)
[Wörtlich zitiert vom 19.02.2021]

Zusammenfassung:
- siehe Coin Lending

### Kommission (Referral System)

Wenn man denkt, dass man den Steuerdschungel endlich durchquert hat, kommt Binance mit seinem Referral System daher.
Über das Werbungs-System erhält man einen prozentualen Anteil an den Trading-Gebühren der Geworbenen auf sein Konto gutgeschrieben.
(lebenslang, logischerweise in BTC.)
Es ist also keine typische Kunden-werben-Kunden-Prämie sondern eher eine Kommission und damit bin ich mir unsicher, wie das einzuordnen ist.

Für das Erste handhabe ich es wie eine Kunden-werben-Kunden-Prämie in Form eines Sachwerts.
Sprich, die BTC werden zum Zeitpunkt des Erhalts in ihren EUR-Gegenwert umgerechnet und den Einkünften aus sonstigen Leistungen hinzugefügt.
Aufgrund eines fehlenden steuerlichen Anschaffungsvorgangs ist eine Veräußerung steuerfrei.

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