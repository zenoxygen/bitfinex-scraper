#!/usr/bin/env python3

import os
import time
import asyncio
import websockets
import pendulum

from influxdb import InfluxDBClient

import bitfinex

insertion_template = [{
    'measurement': 'candles',
    'time': '',
    'fields': {
        'open': 0.0,
        'low': 0.0,
        'high': 0.0,
        'close': 0.0,
        'volume': 0.0
    },
    'tags': {
        'pair': ''
    }
}]

def connect(influxdb_params):
    """
    Connect to InfluxDB.
    """
    host = influxdb_params[0]
    port = influxdb_params[1]
    username = influxdb_params[2]
    password = influxdb_params[3]
    database = influxdb_params[4]
    ssl = influxdb_params[5]
    verify_ssl = influxdb_params[6]

    try:
        print('Connection to InfluxDB...')
        client = InfluxDBClient(host, port, username, password, database, ssl, verify_ssl)
    except:
        print('Could not connect to InfluxDB.')
        return None
    else:
        print('Connection to InfluxDB successfully established.')
        return client


def disconnect(client):
    """
    Disconnect from InfluxDB.
    """
    if client is not None:
        client.close()
        print('Connection to InfluxDB properly closed.')
        time.sleep(3)
    else:
        print('Could not close connection to InfluxDB.')


def stream_fields_to_dict(stream_fields):
    """
    Write data into InfluxDB.
    """
    insertion_template[0]['time'] = stream_fields[0]
    insertion_template[0]['fields']['open'] = stream_fields[1]
    insertion_template[0]['fields']['low'] = stream_fields[2]
    insertion_template[0]['fields']['high'] = stream_fields[3]
    insertion_template[0]['fields']['close'] = stream_fields[4]
    insertion_template[0]['fields']['volume'] = stream_fields[5]
    insertion_template[0]['tags']['pair'] = stream_fields[6]

    # Return True is operation is successful
    return insertion_template


def save_past_market_data(influxdb_params, bitfinex_pairs, start_date, end_date):
    """
    Save past market data into InfluxDB.
    """
    client = connect(influxdb_params)

    if client is not None:

        print('Retrieve historical market data from Bitfinex (can take several minutes)...')

        # For each pair to retrieve
        for pair in bitfinex_pairs:

            # Settings to retrieve data
            timeframe = pair.split(':')[1]
            pair = pair.split(':')[2]

            # Get candles between two dates (json format)
            candles = bitfinex.get_market_data_by_date(pair, timeframe, start_date, end_date)

            print('Successfully retrieved {nb_candles} candles ({pair}) from {start} to {end}.'\
                  .format(nb_candles=len(candles), pair=pair, start=start_date, end=end_date))

            # Write time series into InfluxDB
            for candle in candles:
                while True:
                    points = stream_fields_to_dict(candle)
                    try:
                        if client.write_points(points) is True:
                            break
                    except:
                        print('Could not write to InfluxDB.')
                        disconnect(client)
                        client = connect(influxdb_params)

        disconnect(client)


async def save_real_time_market_data(influxdb_params, queue):
    """
    Save real time market data into InfluxDB.
    """
    client = connect(influxdb_params)

    if client is not None:
        while True:
            stream_fields = await queue.get()

            while True:
                points = stream_fields_to_dict(stream_fields)
                try:
                    if client.write_points(points) is True:
                        break
                except:
                    print('Could not write to InfluxDB.')
                    disconnect(client)
                    client = connect(influxdb_params)


if __name__ == '__main__':
    bitfinex_pairs = os.getenv('BITFINEX_PAIRS', None)
    influxdb_host = os.getenv('INFLUXDB_HOST', None)
    influxdb_port = os.getenv('INFLUXDB_PORT', 8086)
    influxdb_use_ssl = os.getenv('INFLUXDB_USE_SSL', False)
    influxdb_verify_ssl = os.getenv('INFLUXDB_VERIFY_SSL', False)
    influxdb_username = os.getenv('INFLUXDB_USERNAME', None)
    influxdb_password = os.getenv('INFLUXDB_PASSWORD', None)
    influxdb_database = os.getenv('INFLUXDB_DATABASE', None)

    if not bitfinex_pairs:
        print('Environment variable BITFINEX_PAIRS must be set.')
        exit(1)

    if not (influxdb_host and influxdb_database and influxdb_username and influxdb_password):
        print('Environment variables INFLUXDB_HOST, INFLUXDB_DATABASE,\
              INFLUXDB_USERNAME and INFLUXDB_PASSWORD must be set.')
        exit(1)

    bitfinex_pairs = bitfinex_pairs.split(',')

    influxdb_params = (influxdb_host, influxdb_port, influxdb_username, influxdb_password,\
                       influxdb_database, influxdb_use_ssl, influxdb_verify_ssl)

    # Get past historical market data
    now = pendulum.now().set(tz='Europe/Paris')
    start_date = now.subtract(days=1).to_atom_string()
    end_date = now.to_atom_string()
    save_past_market_data(influxdb_params, bitfinex_pairs, start_date, end_date)

    # Get real time market data
    event_loop = asyncio.get_event_loop()
    queue = asyncio.Queue(loop=event_loop)
    event_loop.create_task(bitfinex.get_real_time_market_data(bitfinex_pairs, queue))
    event_loop.create_task(save_real_time_market_data(influxdb_params, queue))
    event_loop.run_forever()
