# bitfinex-scraper

A Bitfinex scraping bot that pulls and saves market data into an InfluxDB time series database.

## Overview

The program scrapes the [candlestick charts](https://en.wikipedia.org/wiki/Candlestick_chart) values for predefined trading pairs from the [Bitfinex API 2.0](https://docs.bitfinex.com/v2/reference).  
In a first time, it pulls and saves the past historical market data (defaults to the last 24 hours) with the REST API.  
In a second time, it pulls and saves the real time market data with the Websocket API.

## Installation

This project has been tested with Python 3.7.0 and InfluxDB 1.6.0.

- [Python](https://www.python.org)
- [InfluxDB](https://docs.influxdata.com/influxdb)

Here is a full list of items to install before going further:

- [influxdb](https://pypi.org/project/influxdb)
- [requests](https://pypi.org/project/requests)
- [websockets](https://pypi.org/project/websockets)
- [pendulum](https://pypi.org/project/pendulum)

To install all of the requirements at once:

`pip install -r requirements.txt`

## Configuration via environment variables

__Bitfinex Websocket API__

- `BITFINEX_PAIRS`: a list of comma-separated pairs you want to subscribe, e.g. `"trade:5m:tBTCUSD,trade:5m:tETHUSD,trade:15m:tXMRUSD"`

__InfluxDB__

- `INFLUXDB_HOST`: hostname to connect to InfluxDB
- `INFLUXDB_PORT`: port to connect to InfluxDB __(defaults to 8086)__
- `INFLUXDB_USE_SSL`: use https instead of http to connect to InfluxDB __(defaults to False)__
- `INFLUXDB_VERIFY_SSL`: verify SSL certificates for HTTPS requests __(defaults to False)__
- `INFLUXDB_USERNAME`: username used to connect to InfluxDB
- `INFLUXDB_PASSWORD`: password used to connect to InfluxDB
- `INFLUXDB_DATABASE`: database name

## Usage

To start scraping market data:

`python scraper.py`

## Credits

This project was largely inspired by [bitfinex-crawler](https://github.com/JasperZ/bitfinex-crawler).
