FROM python:3.9.7-alpine

WORKDIR /CoinTaxman

RUN addgroup -S cointaxman && adduser -S cointaxman -G cointaxman -h /CoinTaxman

RUN apk update \
    && apk add git make

USER cointaxman

COPY --chown=cointaxman requirements.txt Makefile ./

RUN make install

COPY --chown=cointaxman src ./src

CMD ["python", "src/main.py"]