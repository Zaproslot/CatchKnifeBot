import time
import logging
import pandas as pd
from Analize_classes import Analyzer
from Trade_classes import TradeProcessor
from binance import AsyncClient, BinanceSocketManager
from keys import api_key, api_secret
from enums import SWING_TRADE, CATCH_KNIVES
import config
import asyncio


class MainProcessor:
    """Makes an analysis and trade in the async mode."""

    def __init__(self):
        self.tasks = None
        self.f_symbols = None
        self.bm = None
        self.client = None
        self.trade_processor = None
        logging.basicConfig(
            filename='log/log.log',
            encoding='utf-8',
            format='%(asctime)s %(levelname)s %(name)s %(funcName)s -> : %(message)s',
            filemode='a',
            level=logging.INFO
        )
        self.logger = logging.getLogger(__name__)
        self.analyzer = Analyzer()
        self.opened_position = {}

    async def __swing_trade(self, symbol: str):
        """This is the task of the swing trade mode for one instrument."""
        control_time = 0
        current_high = -1.0
        is_pump = False
        is_volumes = False
        printed = False

        position_is_open = False
        stop_loss = 0.0
        take_profit = 0.0
        tf_ms = self.analyzer.kline_tf_to_int_minutes(config.tf) * 60

        if config.trade_mode == SWING_TRADE:
            async with self.bm.aggtrade_futures_socket(symbol=symbol) as ts:
                self.logger.info(f'{symbol} initialized.')
                while True:  # Main loop
                    cur_time = time.time()
                    req = await ts.recv()

                    try:
                        df = pd.DataFrame(req)
                    except ValueError as e:
                        self.logger.error(f'Error during reading agg trades {symbol} {req = }')
                        continue
                    current_price = float(df[df.index == 'p']['data'].values[0])

                    if current_high < current_price:  # Rewrite the high of the pump
                        current_high = current_price

                    if not position_is_open:  #

                        if cur_time >= control_time:  # Working is only at the calculated period

                            is_pump = False
                            is_volumes = False
                            control_time = cur_time - (cur_time % tf_ms) + tf_ms
                            last_bear_candle_data = await self.analyzer.get_last_bear_candle_params(
                                self.client,
                                symbol,
                                config.tf)

                            pump_lvl = last_bear_candle_data['last_bear_candle_low'] * (1 + (config.pump_height * 0.01))  # Pump high
                            coeff_volumes = last_bear_candle_data['swing_max_volume'] / last_bear_candle_data[
                                'last_bear_candle_volume']  # The coefficient of the volumes' different

                            if coeff_volumes >= config.coeff_volumes:  #
                                is_volumes = True

                        if current_high >= pump_lvl and not is_pump:
                            is_pump = True

                        if is_pump and is_volumes:  # You can open a deal
                            if not printed:
                                msg = f'\nThe pump at the symbol {symbol} is found.\n' \
                                      f'Start pump price: {last_bear_candle_data["last_bear_candle_low"]}\n' \
                                      f'Pump high: {current_high}\n' \
                                      f'Start pump time: {last_bear_candle_data["last_bear_candle_time"]}\n\n'

                                self.logger.info(msg)
                                printed = True

                            stop_loss = current_price * (1 + (config.stop_loss * 0.01))
                            take_profit = current_price * (1 - (config.take_profit * 0.01))

                            is_rollback = self.analyzer.check_rollback(last_bear_candle_data['last_bear_candle_low'],
                                                                       current_high,
                                                                       current_price,
                                                                       config.pump_rollback)
                            if is_rollback:
                                self.logger.info(
                                    f'Open a deal {symbol}. {current_price = } {stop_loss = } {take_profit = }')
                                if await self.trade_processor.deal_by_market(symbol,
                                                                             config.risk_usdt_on_deal,
                                                                             current_price,
                                                                             stop_loss,
                                                                             take_profit,
                                                                             SWING_TRADE):
                                    self.logger.info(f'Position {symbol} is opened. ')
                                    position_is_open = True
                                    is_pump = False
                                    is_volumes = False
                                    printed = False
                                    current_high = -1.0

                    else:  # If there's an opened position
                        if current_price >= stop_loss:  # closing by SL
                            reason = 'stop loss'
                            if await self.trade_processor.close_by_market(symbol,
                                                                          current_price,
                                                                          stop_loss,
                                                                          take_profit,
                                                                          SWING_TRADE,
                                                                          reason):
                                self.logger.info(f'The deal {symbol} closed by stop_loss. {current_price = } {stop_loss = } {take_profit = }')
                                position_is_open = False
                        elif current_price <= take_profit:  # closing by TP
                            reason = 'take profit'
                            if await self.trade_processor.close_by_market(symbol,
                                                                          current_price,
                                                                          stop_loss,
                                                                          take_profit,
                                                                          SWING_TRADE,
                                                                          reason):
                                self.logger.info(f'The deal {symbol} closed by take profit. {current_price = } {stop_loss = } {take_profit = }')
                                position_is_open = False

    async def __catch_knives(self, symbol: str):
        """This is the task of the catch knives mode for one instrument."""
        control_time = 0
        prev_volume = 0
        tf = AsyncClient.KLINE_INTERVAL_1MINUTE
        printed = False
        price_in_diap = False
        volume_control = False
        pump_control = False
        now_time = 0
        future_time = 0
        up_diapason_board = 0.0
        dn_diapason_board = 0.0
        tf_sec = self.analyzer.kline_tf_to_int_minutes(config.tf) * 60

        position_is_open = False
        stop_loss = 0.0
        take_profit = 0.0

        if config.trade_mode == CATCH_KNIVES:
            async with self.bm.kline_futures_socket(symbol=symbol) as ts:
                self.logger.info(f'{symbol} initialized.')
                while True:
                    cur_time = time.time()
                    req = await ts.recv()

                    try:
                        df = pd.DataFrame(req)['k']
                    except KeyError as e:
                        self.logger.error(f'Error during reading kline {symbol} {req = }')
                        continue

                    open_price = float(df.o)
                    high_price = float(df.h)
                    low_price = float(df.l)
                    close_price = float(df.c)
                    volume = float(df.v)
                    cdl_time = float(df.t)

                    if not position_is_open:
                        if cur_time >= control_time:
                            volume_control = False
                            pump_control = False
                            now_time = 0
                            future_time = 0
                            up_diapason_board = 0
                            dn_diapason_board = 0

                            control_time = cur_time - (cur_time % tf_sec) + tf_sec
                            prev_cdl = await self.analyzer.get_last_candle_params(self.client, symbol, tf)
                            prev_volume = prev_cdl.Volume
                            prev_high = prev_cdl.High

                        pump_control_price = prev_high * (1 + (config.pump_height * 0.01))
                        if volume > 0 and not volume_control:
                            volume_control = True if prev_volume // volume >= config.coeff_volumes else False
                        pump_control = True if not pump_control and high_price >= pump_control_price else False

                        if volume_control and pump_control and open_price < close_price and close_price > prev_high :  # Looking for a pump
                            if not printed:
                                msg = f'\nThe pump at the symbol {symbol} is found.\n' \
                                      f'Start pump price: {low_price}\n' \
                                      f'Pump high: {high_price}\n' \
                                      f'Start pump time: {cdl_time}\n\n'
                                self.logger.info(msg)

                                printed = True
                            # Init stop zone's params
                            if now_time == 0:
                                now_time = time.time()
                                future_time = now_time + config.stop_diap_time
                                diap = close_price * (0.01 * config.stop_diap)
                                up_diapason_board = close_price + (diap * 0.5)
                                dn_diapason_board = close_price - (diap * 0.5)
                                self.logger.info(f'New stop diapason for {symbol} is calculated.')
                            # The price is in the diapason for a pointed time
                            if cur_time >= future_time and up_diapason_board >= close_price:
                                if close_price >= dn_diapason_board:
                                    self.logger.info(f'Diapason is good {symbol}.')
                                    price_in_diap = True
                                    now_time = 0
                                    future_time = 0
                            # The price went from the diapason
                            elif cur_time < future_time and (
                                    close_price > up_diapason_board or close_price < dn_diapason_board):
                                self.logger.info(f'Diapason is broken {symbol}')
                                price_in_diap = False
                                now_time = 0
                                future_time = 0

                            if price_in_diap:
                                stop_loss = close_price * (1 + (config.stop_loss * 0.01))
                                take_profit = close_price * (1 - (config.take_profit * 0.01))
                                self.logger.info(f'Open a deal {symbol}. {close_price = } {stop_loss = } {take_profit = }')
                                if await self.trade_processor.deal_by_market(symbol,
                                                                             config.risk_usdt_on_deal,
                                                                             close_price,
                                                                             stop_loss,
                                                                             take_profit,
                                                                             CATCH_KNIVES):
                                    position_is_open = True
                                    printed = False
                                    price_in_diap = False
                                    now_time = 0
                                    future_time = 0

                    else:  # If there's an opened position
                        if close_price >= stop_loss:
                            reason = 'stop loss'
                            if await self.trade_processor.close_by_market(symbol,
                                                                          close_price,
                                                                          stop_loss,
                                                                          take_profit,
                                                                          SWING_TRADE,
                                                                          reason):
                                self.logger.info(f'The deal {symbol} closed by stop_loss. {close_price = } {stop_loss = } {take_profit = }')
                                position_is_open = False
                        elif close_price <= take_profit:
                            reason = 'take profit'
                            if await self.trade_processor.close_by_market(symbol,
                                                                          close_price,
                                                                          stop_loss,
                                                                          take_profit,
                                                                          SWING_TRADE,
                                                                          reason):
                                self.logger.info(f'The deal {symbol} closed by take profit. {close_price = } {stop_loss = } {take_profit = }')
                                position_is_open = False

    async def run(self):
        """This method needs to run in the asyncio loop."""
        self.client = await AsyncClient.create(api_key=api_key, api_secret=api_secret)
        self.bm = BinanceSocketManager(self.client)
        self.trade_processor = TradeProcessor(client=self.client)
        self.f_symbols = await Analyzer.get_all_futures(client=self.client)
        self.tasks = []

        tf_in_min = self.analyzer.kline_tf_to_int_minutes(config.tf)

        if tf_in_min < 1:
            self.logger.error('Wrong timeframe!!!')
            await self.client.close_connection()

        for symbol in self.f_symbols:
            if config.trade_mode == SWING_TRADE:
                self.tasks.append(asyncio.create_task(self.__swing_trade(symbol=symbol)))
            elif config.trade_mode == CATCH_KNIVES:
                self.tasks.append(asyncio.create_task(self.__catch_knives(symbol=symbol)))
        self.logger.info(f'All tasks created.')
        for task in self.tasks:
            await task

    async def close_connection(self):
        """Close the current async client"""
        await self.client.close_connection()
