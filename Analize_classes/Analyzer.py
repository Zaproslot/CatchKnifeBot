from binance import AsyncClient
import pandas as pd
import logging


class Analyzer:
    """ This class contains a various methods for analyzing a trading information. """
    __logger = logging.getLogger(__name__)

    @staticmethod
    async def get_all_futures(client) -> list:
        """
        This method finds symbol names from client.futures_exchange_info()
        @param client: AsyncClient
        @return: the list with the futures names
        """
        req = await client.futures_exchange_info()
        df = pd.DataFrame(req['symbols'])
        df = df[df.symbol.str.contains('USDT')]
        futures = list(df['symbol'])
        return futures

    @staticmethod
    async def __get_last_candles(client: AsyncClient, symbol: str, tf: str) -> pd.DataFrame:
        """
        Return a dataframe with last candles.
        @param client: async client
        @param symbol: instrument
        @param tf: timeframe
        @return: last candle
        """
        start_str = Analyzer.__get_start_param(tf=tf)

        while True:
            try:
                req = await client.futures_historical_klines(symbol, tf, start_str)
                last_candle = pd.DataFrame(req)
                last_candle = last_candle.iloc[:, :6]
                last_candle.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
                last_candle = last_candle.astype(float)
                return last_candle
            except pd.errors.DataError as e:
                Analyzer.__logger.error(f'Pandas error. Wrong request during getting last candles by symbol {symbol}')

    @staticmethod
    def kline_tf_to_int_minutes(tf: str) -> int:
        """
        Converting the AsyncClient.KLINE_INTERVAL to the int value.
        @param tf: timeframe
        @return: timeframe in an integer value
        """
        match tf:
            case AsyncClient.KLINE_INTERVAL_1MINUTE:
                return 1
            case AsyncClient.KLINE_INTERVAL_5MINUTE:
                return 5
            case AsyncClient.KLINE_INTERVAL_15MINUTE:
                return 15
            case _:
                return 0

    @staticmethod
    def __get_start_param(tf: str) -> str:
        """
        Returns how many candles needs to return.
        @param tf: timeframe
        @return: time period for getting candles
        """
        match tf:
            case AsyncClient.KLINE_INTERVAL_1MINUTE:
                return '50m UTC'
            case AsyncClient.KLINE_INTERVAL_5MINUTE:
                return '4h UTC'
            case AsyncClient.KLINE_INTERVAL_15MINUTE:
                return '12h UTC'
            case _:
                return '1d UTC'

    @staticmethod
    async def get_last_bear_candle_params(client: AsyncClient, symbol: str, tf: str) -> dict:
        """
        Returns a dictionary with the open of the last bearish
        candle and the swing's max volume from theDateFrame.
        @param client: async client
        @param symbol: instrument
        @param tf: timeframe
        @return: a dictionary with the last candle parameters
        """
        last_candles = await Analyzer.__get_last_candles(client, symbol, tf)
        last_bear_candle = last_candles[last_candles['Open'] > last_candles['Close']].tail(1)
        low_price = last_bear_candle['Low'].values[0]
        volume = last_bear_candle['Volume'].values[0]
        time = last_bear_candle['Time'].values[0]
        swing_max_volume = last_candles[last_candles['Time'].values >= time]['Volume'].values.max()
        params = {'last_bear_candle_low': low_price,
                  'last_bear_candle_volume': volume,
                  'last_bear_candle_time': time,
                  'swing_max_volume': swing_max_volume}
        return params

    @staticmethod
    def check_rollback(last_bear_candle_low: float, cur_high: float, cur_price: float, rollback: float) -> bool:
        """
        Checking the rollback after the pump was found
        @param last_bear_candle_low: the low of the last bear candle
        @param cur_high: current high
        @param cur_price: current price
        @param rollback: the rollback from the current high price in percents
        @return: a rollback is found or not
        """
        swing_height = cur_high - last_bear_candle_low
        target_price = cur_high - (swing_height * rollback * 0.01)
        if cur_price <= target_price:
            Analyzer.__logger.info(f'{cur_price = }, {target_price = }')
        return True if cur_price <= target_price else False

    @staticmethod
    async def get_last_candle_params(client: AsyncClient, symbol: str, tf: str):

        while True:
            try:
                last_kline = await client.futures_historical_klines(symbol, tf, '3min ago UTC')
                df = pd.DataFrame(last_kline)
                df = df.iloc[:, :6]
                df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
                return df.iloc[1].astype(float)
            except pd.errors.DataError as e:
                Analyzer.__logger.error(
                    f'Pandas error. Wrong data in the dataframe during getting the last candle parameters')
