import time
from binance import AsyncClient
import pandas as pd
import config
from binance.enums import *
import json
import logging


class TradeProcessor:

    def __init__(self, client: AsyncClient):
        self.client = client
        self.open_position = {}
        self.logger = logging.getLogger(__name__)

    async def __check_open_position(self, symbol: str) -> list:
        """
        Looking open positions by the instrument and return a list with the instrument and the quantity.
        @param symbol: instrument
        @return: a list with the symbol and the quantity of opened positions
        """
        self.logger.info(f'Check open positions by {symbol} parameters........ ')
        try:
            req = await self.client.futures_account(symbol=symbol)
            f_acc_info = json.dumps(req)
            f_acc_list_list = json.loads(f_acc_info)['positions']
            df = pd.DataFrame(f_acc_list_list)
            qty = float(df[df['symbol'] == symbol]['positionAmt'].values)
            return [symbol, qty]
        except pd.errors.DataError as e:
            self.logger.error(f'Pandas error during checking open position.{e}')

    async def deal_by_market(self, symbol: str,
                             qty_usdt: float,
                             cur_price: float,
                             stop_loss: float,
                             take_profit: float,
                             mode_trade: str,
                             side=SIDE_SELL) -> bool:
        """
        Open a deal by market.
        @param symbol: instrument
        @param qty_usdt: lot in USDT
        @param cur_price: current price
        @param stop_loss: stop loss
        @param take_profit: take profit
        @param mode_trade: type of trade
        @param side: deal side
        @return: position is opened or not
        """
        self.logger.info(f'Opening deal by {symbol}........ ')
        trading_conditions, balance = await self.__check_trading_conditions(qty_usdt=qty_usdt, symbol=symbol)
        self.logger.info(f'Trading conditions are checked {symbol}')
        if trading_conditions:
            qty = await self.__calc_lot(symbol, qty_usdt, cur_price, stop_loss, take_profit, balance)
            self.logger.info(f'Lot calculated {symbol}')
            reason = 'deal open'
            self.logger.info(f'Opening deal at symbol {symbol}. quantity:{qty}')
            if qty > 0:
                sell_deal_req = await self.client.futures_create_order(symbol=symbol,
                                                                       side=side,
                                                                       type=ORDER_TYPE_MARKET,
                                                                       quantity=qty)
                self.logger.info(f'Position by {symbol} is open. Info about the opened position:\n{sell_deal_req}]\n')
                open_deal = await self.__check_open_position(symbol)
                if abs(open_deal[1]) > 0:
                    self.open_position[open_deal[0]] = open_deal[1]
                    msg = f'\nThe position list has position by {symbol}\n' \
                          f'Symbol: {symbol}\nPrice: {cur_price}\nSL: {stop_loss}\nTP: {take_profit}\n' \
                          f'Quantity: {qty}\nTrade mode: {mode_trade}\nReason: {reason}\n\n'
                    self.logger.info(msg)
                    return True
                else:
                    self.logger.info(f'The deal at {symbol} is not open.')
                    return False
            else:
                self.logger.info(f'Wrong {symbol} quantity.')
                return False

    async def close_by_market(self, symbol: str,
                              cur_price: float,
                              stop_loss: float,
                              take_profit: float,
                              mode_trade: str,
                              reason: str,
                              side=SIDE_BUY) -> bool:
        """
        Close a deal by market.
        @param symbol: instrument
        @param stop_loss: stop loss
        @param take_profit: take profit
        @param mode_trade: type of trade
        @param reason: close reason
        @return: position is closed or not
        """
        qty = abs(self.open_position[symbol])

        if qty > 0:
            close_req = await self.client.futures_create_order(symbol=symbol,
                                                               side=side,
                                                               type=ORDER_TYPE_MARKET,
                                                               quantity=qty)

            time.sleep(0.1)

            self.logger.info(f'Info about the closed position by {symbol}:\n{close_req}]\n')
            closed_deal = await self.__check_open_position(symbol)
            if closed_deal[1] == 0:
                self.open_position[closed_deal[0]] = closed_deal[1]
                msg = f'The deal at {symbol} is closed.\n' \
                      f'Symbol: {symbol}\nPrice: {cur_price}\nSL: {stop_loss}\nTP: {take_profit}\n' \
                      f'Quantity: {qty}\nTrade mode: {mode_trade}\nReason: {reason}\n\n'
                self.logger.info(msg)
                return True
            else:
                self.logger.info(f'The deal at {symbol} is not closed.')
                return False
        else:
            self.logger.info(f'Wrong quantity for closing the deal {symbol}.')
            return False

    async def __calc_lot(self, symbol: str,
                         qty_usdt: float,
                         cur_price: float,
                         stop_loss: float,
                         take_profit: float,
                         money: float) -> float:
        """
        Calculating a quantity of the volume for the deal.
        @param symbol: instrument
        @param qty_usdt: lot in USDT
        @param cur_price: current price
        @param stop_loss: stop loss
        @param take_profit: take profit
        @param money: balance
        @return: a trade volume of the instrument
        """
        self.logger.info(f'Calculate lot for {symbol}........ ')
        min_usdt = 5  # minimal lot in USDT
        max_qty, min_qty, step, tick_size = await self.__get_qty_params(symbol)
        min_vol = (((min_usdt / take_profit) // min_qty) + min_qty)
        vol = qty_usdt / (stop_loss - cur_price)
        max_balance_qty = ((money * config.shoulder / cur_price * (config.depo_load * 0.01)) // step) * step

        if vol < min_vol:
            vol = min_qty
        elif vol > max_qty:
            vol = max_qty
        elif vol > max_balance_qty:
            vol = max_balance_qty
        else:
            vol = (vol // step) * step
        return vol

    async def __get_qty_params(self, symbol: str):
        """Getting the max-min quantity and the step for the instrument.
        @param symbol: instrument
        @return: the instrument's trade parameters
        """
        self.logger.info(f'Getting {symbol} parameters........ ')
        while True:
            try:
                req = await self.client.futures_exchange_info()

                df = pd.DataFrame(req['symbols'])
                df = df[df.symbol.str.contains(symbol)]
                lst = df.filters.tolist()[0][2]
                tick_size = df.filters.tolist()[0][0]
                params = [lst['maxQty'], lst['minQty'], lst['stepSize'], tick_size['tickSize']]
                self.logger.info(params)
                params = tuple(map(float, params))
                return params
            except pd.errors.DataError as e:
                self.logger.error(f'Pandas error. Wrong request during getting futures exchange info. {e}')

    async def __check_trading_conditions(self, qty_usdt: float, symbol: str) -> tuple:
        """Check the account status and return the conditions and the balance
        @param qty_usdt: lot size in USDT
        @param symbol: instrument
        @return: trading enabled or disabled, and current balance
        """
        self.logger.info(f'Check {symbol} conditions........ ')
        while True:
            try:
                req = await self.client.futures_account()
                can_trade = bool(req['canTrade'])
                money = float(req['totalMarginBalance'])
                margin = float(req['totalMaintMargin'])
                break
            except pd.errors.DataError as e:
                self.logger.error(f'Pandas error. Wrong request during getting futures account info. {e}')

        if not can_trade:
            self.logger.info('Trading is blocked.')
            return False, money
        elif qty_usdt > money:
            self.logger.info('Insufficient funds.')
            return False, money
        elif margin > 0:
            self.logger.info(f'Margin is no null')
            load_percent = round(margin / (money * 0.01), 2)
            self.logger.info(f'{load_percent = } {money/load_percent = } {config.depo_load = }')
            if load_percent > config.depo_load:
                self.logger.info(f'The deposit is overloaded {money = } {margin = } {load_percent}')
                return False, money
            else:
                self.logger.info(f'Trading conditions by {symbol} are checked. OK.')
                return True, money
        else:
            self.logger.info(f'Trading conditions by {symbol} are checked. OK.')
            return True, money
