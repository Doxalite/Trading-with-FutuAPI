import time
from moomoo import *
import os
from dotenv import load_dotenv

load_dotenv()

# Global variables
OPEND_ADDRESS = '127.0.0.1'
OPEND_PORT = 11111

TRADING_ENVIRONMENT = TrdEnv.SIMULATE
TRADING_MARKET = Market.HK
TRADING_PWD = os.getenv('TRADING_PWD')
TRADING_SECURITY = 'HK.00700'
TRADING_PERIOD = KLType.K_1M

# Strategy variables
POSITION_OPEN_TIME = None
FIRST_CANDLE_HIGH = None
LAST_CANDLE_CLOSE = None

# Initialize Futu API
quote_ctx = OpenQuoteContext(host=OPEND_ADDRESS, port=OPEND_PORT)
trade_ctx = OpenHKTradeContext(host=OPEND_ADDRESS, port=OPEND_PORT) # Use HK trade context  

# check if it is normal trading time
def is_normal_trading_time(code):
    ret, data = quote_ctx.get_market_state([code])
    if ret != RET_OK:
        print(f"Failed to get market state: {data}")
        return False
    market_state = data.iloc[0]['market_state']
    '''
    MarketState.MORNING            HK and A-share morning
    MarketState.AFTERNOON          HK and A-share afternoon, US opening hours
    MarketState.FUTURE_DAY_OPEN    HK, SG, JP futures day market open
    MarketState.FUTURE_OPEN        US futures open
    MarketState.FUTURE_BREAK_OVER  Trading hours of U.S. futures after break
    MarketState.NIGHT_OPEN         HK, SG, JP futures night market open
    '''
    if market_state == MarketState.MORNING or \
                market_state == MarketState.AFTERNOON or \
                market_state == MarketState.FUTURE_DAY_OPEN  or \
                market_state == MarketState.FUTURE_OPEN  or \
                market_state == MarketState.FUTURE_BREAK_OVER  or \
                market_state == MarketState.NIGHT_OPEN:
        return True
    print('It is not regular trading hours.')
    return False

# Get positions
def get_holding_position(code):
    holding_position = 0
    ret, data = trade_ctx.position_list_query(code=code, trd_env=TRADING_ENVIRONMENT)
    if ret != RET_OK:
        print('Get holding position failed: ', data)
        return None
    else:
        for qty in data['qty'].values.tolist():
            holding_position += qty
        print('[Holding Position Status] The holding position quantity of {} is: {}'.format(TRADING_SECURITY, holding_position))
    return holding_position

# check if 3 previous candles are red
def is_three_previous_candles_red(code):
    ret, data = quote_ctx.get_cur_kline(code, 4, TRADING_PERIOD)
    if ret != RET_OK:
        print(f"Failed to get klines: {data}")
        return (False, None)
    for i in range(3):
        candle = data.iloc[i]
        if candle['open'] <= candle['close']:
            return (False, None)
    first_candle_high = data.iloc[0]['high']
    last_candle_close = data.iloc[2]['close']
    return (True, [first_candle_high, last_candle_close])

# get best ask and bid prices
def get_ask_and_bid(code):
    ret, data = quote_ctx.get_order_book(code, num=1)
    if ret != RET_OK:
        print('Get order book failed: ', data)
        return None, None
    return data['Ask'][0][0], data['Bid'][0][0]

# Function to place a buy order
def place_buy_order(code, quantity = 100):
    # get order book data
    ask, bid = get_ask_and_bid(code)

    # check if buying power is sufficient
    if is_valid_quantity(code, quantity, ask):
        # place order
        ret, data = trade_ctx.place_order(price=ask, qty=quantity, code=code, trd_side=TrdSide.BUY, 
                                          order_type=OrderType.NORMAL, trd_env=TRADING_ENVIRONMENT,
                                          remark = 'three_red_candles_strategy')
        if ret != RET_OK:
            print(f"Failed to place buy order: {data}")
    else:
        print('Insufficient buying power to place buy order.')
        
# Function to place a sell order
def place_sell_order(code, quantity):
    # get order book data
    ask, bid = get_ask_and_bid(code)

    # Check quantity
    if quantity == 0:
        print('Invalid order quantity.')
        return False

    # Close position
    ret, data = trade_ctx.place_order(price=bid, qty=quantity, code=code, trd_side=TrdSide.SELL,
                   order_type=OrderType.NORMAL, trd_env=TRADING_ENVIRONMENT, remark='three_red_candles_strategy')
    if ret != RET_OK:
        print('Close position failed: ', data)
        return False
    return True

# Function to place a limit order (buy or sell)
def place_limit_order(code, quantity, price, trd_side):
    # Check quantity
    if quantity == 0:
        print('Invalid order quantity.')
        return False

    # Place limit order
    ret, data = trade_ctx.place_order(price=price, qty=quantity, code=code, trd_side=trd_side,
                   order_type=OrderType.LIMIT, trd_env=TRADING_ENVIRONMENT, remark='three_red_candles_strategy')
    if ret != RET_OK:
        print('Place limit order failed: ', data)
        return False
    return True

# Function to place a stop order (sell when price drops to stop price)
def place_stop_order(code, quantity, price):
    # Check quantity
    if quantity == 0:
        print('Invalid order quantity.')
        return False

    # Place stop order
    ret, data = trade_ctx.place_order(price=price, qty=quantity, code=code, trd_side=TrdSide.SELL,
                   order_type=OrderType.STOP, trd_env=TRADING_ENVIRONMENT, remark='three_red_candles_strategy')
    if ret != RET_OK:
        print('Place stop order failed: ', data)
        return False
    return True

# check if buying power is sufficient for quantity to buy
def is_valid_quantity(code, quantity, price):
    ret, data = trade_ctx.acctradinginfo_query(order_type=OrderType.NORMAL, code=code, price=price,
                                                   trd_env=TRADING_ENVIRONMENT)
    if ret != RET_OK:
        print('Get max long/short quantity failed: ', data)
        return False
    max_can_buy = data['max_cash_buy'][0]
    max_can_sell = data['max_sell_short'][0]
    if quantity > 0:
        return quantity < max_can_buy
    elif quantity < 0:
        return abs(quantity) < max_can_sell
    else:
        return False

# Show order status
def show_order_status(data):
    order_status = data['order_status'][0]
    order_info = dict()
    order_info['Code'] = data['code'][0]
    order_info['Price'] = data['price'][0]
    order_info['TradeSide'] = data['trd_side'][0]
    order_info['Quantity'] = data['qty'][0]
    print('[OrderStatus]', order_status, order_info)

# Show fill status
def show_fill_status(data):
    fill_status = data['status'][0]
    fill_info = dict()
    fill_info['Code'] = data['code'][0]
    fill_info['Price'] = data['price'][0]
    fill_info['TradeSide'] = data['trd_side'][0]
    fill_info['Quantity'] = data['qty'][0]
    print('[FillStatus]', fill_status, fill_info)

# strategy initialization. Run once when the strategy starts
def on_init():
    # unlock trade (no need for sim trade)
    print('************************ Strategy start ************************')
    return True

# Run once for each tick
def on_tick():
    pass

# Run once for each new candlestick
def on_bar_open():
    print('************************** Bar Open ****************************')
    global POSITION_OPEN_TIME
    global FIRST_CANDLE_HIGH
    global LAST_CANDLE_CLOSE 

    # trade only during normal trading hours
    if not is_normal_trading_time(TRADING_SECURITY):
        print('It is not regular trading hours.')
        return
    
    run_strategy, data = is_three_previous_candles_red(TRADING_SECURITY)
    if run_strategy:
        FIRST_CANDLE_HIGH, LAST_CANDLE_CLOSE = data

    # get positions
    holding_position = get_holding_position(TRADING_SECURITY)

    # trading signals
    if holding_position == 0:
        if run_strategy:
            print('Signal: Buy')
            place_buy_order(TRADING_SECURITY, 100)
            POSITION_OPEN_TIME = time.time()
        else:
            print(f'No trading signal at time {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}.')
    elif holding_position > 0:
        # close after 3 minutes
        if POSITION_OPEN_TIME is not None and (time.time() - POSITION_OPEN_TIME) >= 180:
            print('Position held for 3 minutes, signal: Sell')
            # cancel all pending orders
            ret, orders = trade_ctx.order_list_query(code=TRADING_SECURITY, trd_env=TRADING_ENVIRONMENT)
            orders
            if ret == RET_OK:
                for order_id in orders['order_id'].values.tolist():
                    trade_ctx.modify_order(modify_order_op=ModifyOrderOp.CANCEL, order_id=order_id, price=0, qty=0, trd_env=TRADING_ENVIRONMENT)
            place_sell_order(TRADING_SECURITY, holding_position)
            POSITION_OPEN_TIME = None

# Run once when an order is filled
def on_fill(data):
    if data['code'][0] == TRADING_SECURITY:
        show_fill_status(data)

        # place stop loss order once buy order is filled
        if data['status'][0] == 'OK' and data['trd_side'][0] == TrdSide.BUY:
            profit_price = data['price'][0] + (FIRST_CANDLE_HIGH - LAST_CANDLE_CLOSE)
            stop_price = data['price'][0] - (FIRST_CANDLE_HIGH - LAST_CANDLE_CLOSE)
            print(f'Placing take profit limit sell order at {profit_price} and stop loss limit sell order at {stop_price}')
            place_limit_order(TRADING_SECURITY, data['qty'][0], profit_price, TrdSide.SELL)
            place_stop_order(TRADING_SECURITY, data['qty'][0], stop_price)
        # cancel pending orders once sell order is filled
        elif data['status'][0] == 'OK' and data['trd_side'][0] == TrdSide.SELL:
            # cancel all pending orders
            ret, orders = trade_ctx.order_list_query(code=TRADING_SECURITY, trd_env=TRADING_ENVIRONMENT)
            if ret == RET_OK:
                for order_id in orders['order_id'].values.tolist():
                    trade_ctx.modify_order(modify_order_op=ModifyOrderOp.CANCEL, order_id=order_id, price=0, qty=0, trd_env=TRADING_ENVIRONMENT)
            global POSITION_OPEN_TIME
            POSITION_OPEN_TIME = None
        

# Run once when the status of an order changes
def on_order_status(data):
    if data['code'][0] == TRADING_SECURITY:
        show_order_status(data)

############################### Framework code, which can be ignored ###############################
class OnTickClass(TickerHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        on_tick()


class OnBarClass(CurKlineHandlerBase):
    last_time = None
    def on_recv_rsp(self, rsp_pb):
        ret_code, data = super(OnBarClass, self).on_recv_rsp(rsp_pb)
        if ret_code == RET_OK:
            cur_time = data['time_key'][0]
            if cur_time != self.last_time and data['k_type'][0] == TRADING_PERIOD:
                if self.last_time is not None:
                    on_bar_open()
                self.last_time = cur_time


class OnOrderClass(TradeOrderHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret, data = super(OnOrderClass, self).on_recv_rsp(rsp_pb)
        if ret == RET_OK:
            on_order_status(data)


class OnFillClass(TradeDealHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret, data = super(OnFillClass, self).on_recv_rsp(rsp_pb)
        if ret == RET_OK:
            on_fill(data)


# Main loop
if __name__ == "__main__":
    # Strategy initialization
    if not on_init():
        print('Strategy initialization failed, exit script!')
        quote_ctx.close()
        trade_ctx.close()
    else:
        # Set up callback functions
        quote_ctx.set_handler(OnTickClass())
        quote_ctx.set_handler(OnBarClass())
        trade_ctx.set_handler(OnOrderClass())
        trade_ctx.set_handler(OnFillClass())
        # Subscribe tick-by-tick, candlestick and order book of the underlying trading security
        quote_ctx.subscribe(code_list=[TRADING_SECURITY], subtype_list=[SubType.TICKER, SubType.ORDER_BOOK, TRADING_PERIOD])

    # set up backstopping handler

