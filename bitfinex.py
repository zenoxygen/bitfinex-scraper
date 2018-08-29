import json
import time
import asyncio
import websockets
import requests
import pendulum

SUPPORTED_API_VERSION = '2'

HTTP_API_URI = 'https://api.bitfinex.com/v2/'
WEBSOCKETS_API_URI = 'wss://api.bitfinex.com/ws/2'

ERROR_CODE_UNKNOWN_EVENT = 10000
ERROR_CODE_UNKNOWN_PAIR = 10001
ERROR_CODE_SUBSCRIPTION_FAILED = 10300
ERROR_CODE_ALREADY_SUBSCRIBED = 10301
ERROR_CODE_UNKNOWN_CHANNEL = 10302
ERROR_CODE_CHANNEL_LIMIT = 10305
ERROR_CODE_RATE_LIMIT = 11010

INFO_CODE_RECONNECT = 20051
INFO_CODE_START_MAINTENANCE = 20060
INFO_CODE_END_MAINTENANCE = 20061

configuration_template = {
    'event': 'conf',
    'flags': 'flags'
}

candles_subscription_template = {
    'event': 'subscribe',
    'channel': 'candles',
    'key': ''
}

#
# Bitfinex candles stream fields (Websocket API Version 2.0)
# https://docs.bitfinex.com/v2/reference#ws-public-candle
#
# *------------------------------------------------------------------------*
# | Fields    | Type      | Description                                    |
# |------------------------------------------------------------------------|
# | MTS       | int       | Millisecond time stamp                         |
# | OPEN      | float     | First execution during the time frame          |
# | CLOSE     | float     | Last execution during the time frame           |
# | HIGH      | float     | Highest execution during the time frame        |
# | LOW       | float     | Lowest execution during the timeframe          |
# | VOLUME    | float     | Quantity of symbol traded within the timeframe |
# *------------------------------------------------------------------------*
#

def get_market_data_by_date(pair, timeframe, start_date, end_date):
    """
    Get historial OHLCV market data for specific pair and timeframe between to date.
    """
    # Convert string to datetime
    start_dt = pendulum.from_format(start_date, 'YYYY-MM-DDTHH:mm:ssZ')
    end_dt = pendulum.from_format(end_date, 'YYYY-MM-DDTHH:mm:ssZ')

    # Substract timeframe to end datetime
    end_dt = end_dt.subtract(minutes=5)

    # Convert datetime to timestamp (in millisecond, so multiplied by 1000)
    start_dt = start_dt.int_timestamp * 1000
    end_dt = end_dt.int_timestamp * 1000

    # Create an empty list that will contain all candles
    market_data = []

    # Request settings
    limit = 576 # Number of requests to cover on 48h (with timeframe=5m)

    # Loop between two dates
    while (start_dt < end_dt):

        # Build url
        url = HTTP_API_URI + 'candles/trade:{timeframe}:{pair}/hist?limit={limit}&start={start_dt}&sort=1'\
            .format(timeframe=timeframe, pair=pair, limit=limit, start_dt=start_dt)

        # Request API
        json_response = requests.get(url)
        response = json.loads(json_response.text)
        time.sleep(1)

        if 'error' in response:
            # Check rate limit
            if response[1] == ERROR_CODE_RATE_LIMIT:
                print('Error: reached the limit number of requests. Wait 120 seconds...')
                time.sleep(120)
                continue
            # Check platform status
            elif response[1] == ERROR_CODE_START_MAINTENANCE:
                print('Error: platform is in maintenance. Forced to stop all requests.')
                break
        else:
            # Keep previous start datetime
            prev_start_dt = start_dt

            # Get last timestamp of request (in second, so divided by 1000)
            last_dt = int(response[::-1][0][0]) // 1000
            last_dt = pendulum.from_timestamp(last_dt)
            # Put it as new start datetime (in millisecond, so multiplied by 1000)
            start_dt = last_dt.int_timestamp * 1000

            # Get stream fields
            for point in list(response):

                # Convert timestamp to human readable format
                timestamp = pendulum.from_timestamp(int(point[0]) // 1000)
                # Convert timestamp to appropriate timezone
                timestamp = timestamp.in_tz('Europe/Paris')

                timestamp = timestamp.to_atom_string()
                open_price = float(point[1])
                low_price = float(point[2])
                high_price = float(point[3])
                close_price = float(point[4])
                volume = float(point[5])
                pair = str(pair)

                # Add new candle as tuple containing stream fields
                candle = (timestamp, open_price, low_price, high_price, close_price, volume, pair)
                market_data.append(candle)

    return market_data


async def connect():
    """
    Connect to Bitfinex Websocket server.
    """
    print('Connection to Bitfinex...')

    try:
        ws = await websockets.connect(WEBSOCKETS_API_URI)
    except:
        print('Could not connect to Bitfinex.')
        return None
    else:
        try:
            json_response = await ws.recv()
            response = json.loads(json_response)
            if str(response['version']) == SUPPORTED_API_VERSION:
                print('Connection to Bitfinex successfully established.')
                return ws
        except:
            print('Could not connect to Bitfinex.')
            if 'version' in response:
                print('API version {} is not supported.'.format(response['version']))
            if 'platform' in response:
                if response['platform']['status'] == 0:
                    print('Error: platform is in maintenance. Forced to stop all requests.')
            disconnect(ws)
            return None


async def configure(ws):
    """
    Configure connection to Bitfinex Websocket server.
    """
    configuration_template['flags'] = 32

    try:
        await ws.send(json.dumps(configuration_template))
        json_response = await ws.recv()
        response = json.loads(json_response)
        if str(response['event']) == 'conf' and str(response['status']) == 'OK':
            return True
        else:
            print('Could not configure connection to Bitfinex.')
            return False
    except:
            print('Could not configure connection to Bitfinex.')
            return False


async def disconnect(ws):
    """
    Disconnect from Bitfinex Websocket server.
    """
    if ws is not None:
        ws.close()
        print('Connection to Bitfinex properly closed.')
        time.sleep(3)
    else:
        print('Could not close connection to Bitfinex.')


async def fetch_data_from_channels(ws, queue, subscribed_channels):
    """
    Fetch data from subscribed channels.
    """
    while True:
        try:
            # Receive updates upon any change
            response = await asyncio.wait_for(ws.recv(), timeout=1)
            if 'event' in response:
                # Check info message
                if response['event'] == 'info':
                    if response['code'] == INFO_CODE_RECONNECT:
                        print('Info: please reconnect to websocket server.')
                    elif response['code'] == INFO_CODE_START_MAINTENANCE:
                        print('Info: websocket server is entering in maintenance mode.')
                    elif response['code'] == INFO_CODE_END_MAINTENANCE:
                        print('Info: maintenance ended.')
                    break
        except:
            try:
                # Test connection
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=1)
            except:
                print('Disconnected from Bitfinex.')
                break
        else:
            # Parse response to get channel id and update message
            response_splitted = response.replace('[', '').replace(']', '').replace('\'','').replace('"', '').split(',')
            channel_id = int(response_splitted[0])

            if channel_id in subscribed_channels:
                # Check heartbeat message (hb)
                if response_splitted[1] != 'hb':
                    timestamp = str(response_splitted[1])
                    open_price = float(response_splitted[2])
                    low_price = float(response_splitted[3])
                    high_price = float(response_splitted[4])
                    close_price = float(response_splitted[5])
                    volume = float(response_splitted[6])

                    # Convert timestamp to appropriate timezone
                    timestamp = pendulum.parse(timestamp).in_tz('Europe/Paris')
                    timestamp = timestamp.to_atom_string()

                    # Put on queue a new candle as tuple containing stream fields
                    await queue.put((timestamp, open_price, low_price, high_price,\
                                     close_price, volume, subscribed_channels[channel_id]))


async def subscribe_to_channels(ws, bitfinex_pairs):
    """
    Subscribe to public channels (only candles supported).
    """
    subscribed_channels = {}

    for pair in bitfinex_pairs:
        candles_subscription_template['key'] = pair

        try:
            # Send subscription message
            await ws.send(json.dumps(candles_subscription_template))
            json_response = await ws.recv()
            response = json.loads(json_response)

            # Check subscription response
            if 'event' in response:

                if response['event'] == 'subscribed':
                    # Receive initial snapshot but we don't use it
                    json_response = await ws.recv()
                    channel_id = int(response['chanId'])
                    subscribed_channels[channel_id] = pair.split(":")[2]

                if response['event'] == 'error':
                    if response['code'] == ERROR_CODE_UNKNOWN_EVENT:
                        print('Error: unknown event.')
                    if response['code'] == ERROR_CODE_UNKNOWN_PAIR:
                        print('Error: unknown pair.')
                    elif response['code'] == ERROR_CODE_SUBSCRIPTION_FAILED:
                        print('Error: subscription failed.')
                    elif response['code'] == ERROR_CODE_ALREADY_SUBSCRIBED:
                        print('Error: already subscribed.')
                    elif response['code'] == ERROR_CODE_UNKNOWN_CHANNEL:
                        print('Error: unknown channel.')
                    elif response['code'] == ERROR_CODE_CHANNEL_LIMIT:
                        print('Error: reached limit of open channels.')
                    break
        except:
            break

    print('Successfully subscribed to following channels: {}'.format(subscribed_channels))

    return subscribed_channels


async def get_real_time_market_data(bitfinex_pairs, queue):
    """
    Get historical OHLCV market data for specific pairs in real time.
    """
    print('Get real time market data...')

    while True:
        # Connect to server
        ws = await connect()

        if ws is not None:
            if await configure(ws):
                # Subscribe to channels
                subscribed_channels = await subscribe_to_channels(ws, bitfinex_pairs)
                if len(subscribed_channels) == len(bitfinex_pairs):
                    # Fetch data from subscribed channels
                    await fetch_data_from_channels(ws, queue, subscribed_channels)

            await disconnect(ws)
