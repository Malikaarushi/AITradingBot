import logging
import json
from kiteconnect import KiteTicker
from config import getUserConfig
from zerodha import getAccessToken
from instruments import getInstrumentDataBySymbol, getInstrumentDataByToken
import pandas as pd
import datetime
#from pandas.io.json import json_normalize
import plotly.graph_objects as go
import plotly.io as pio
from openpyxl import Workbook

ticker = None

def startTicker():
  userConfig = getUserConfig()
  accessToken = getAccessToken()
  if accessToken == None:
    logging.error('startTicker: Cannot start ticker as accessToken is empty')
    return
  
  global ticker
  ticker = KiteTicker(userConfig['apiKey'], accessToken)
  ticker.on_connect = onConnect
  ticker.on_close = onDisconnect
  ticker.on_error = onError
  ticker.on_reconnect = onReconnect
  ticker.on_noreconnect = onMaxReconnectsAttempt
  ticker.on_ticks = onNewTicks
  ticker.on_order_update = onOrderUpdate

  logging.info('Ticker: Going to connect..')
  ticker.connect(threaded=True)

def registerSymbols(symbols):
  tokens = []
  for symbol in symbols:
    isd = getInstrumentDataBySymbol(symbol)
    token = isd['instrument_token']
    logging.info('registerSymbol: %s token = %s', symbol, token)
    tokens.append(token)

  logging.info('Subscribing tokens %s', tokens)
  ticker.subscribe(tokens)
  #subscribe in full mode
  ticker.set_mode(ticker.MODE_FULL,tokens)

def stopTicker():
  logging.info('Ticker: stopping..')
  ticker.close(1000, "Manual close")

ticks_storage = pd.DataFrame()
timeframe = 1
temp = {}

def onNewTicks(ws, ticks):
  #symbols=[]
  #logging.info('New ticks received %s', ticks)
  writer=pd.ExcelWriter('Tick.xlsx', engine='openpyxl')
  for tick in ticks:
    isd = getInstrumentDataByToken(tick['instrument_token'])
    symbol = isd['tradingsymbol']
    logging.info('Tick: %s CMP = %f', symbol, tick['last_price'])
    #symbols.append(symbol)
      # wb= Workbook()
      # ws=wb.active
      # with pd.ExcelWriter('Tick.xlsx', engine="openpyxl") as writer:
        # writer.book=wb
        # writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)
    timestamp = str(datetime.datetime.now())
    temp[tick['timestamp']] = tick
    df = pd.DataFrame.from_dict(temp, orient='index')
    df1 = df['last_price'].resample(str(timeframe)+'Min').ohlc()
    df1['Symbol']=symbol
    tick_df=pd.DataFrame(df1)
    tick_df = tick_df.append(tick_df) # append to existing
    print(tick_df)
    tick_df.to_excel(writer, sheet_name=symbol, index=True)

  writer.save()  
    # # Now only keep columns that you want
    # # timestamp = str(datetime.datetime.now())
    # # df['timestamp']=timestamp
    # # df = df[['timestamp','ohlc.open','ohlc.high', 'ohlc.low','ohlc.close']]

def plotTicker():
  filename= pd.read_excel('Tick.xlsx')
  candlestick = go.Candlestick(
                              x=filename.index,
                              open=filename['open'],
                              high=filename['high'],
                              low=filename['low'],
                              close=filename['close'],
                              increasing_line_color= 'blue', decreasing_line_color= 'orange'
                              )
  fig = go.Figure(data=[candlestick])
  fig.update_layout(
    width=800, height=600,
    title="OHLC data",
    yaxis_title=filename['Symbol']
  )
  fig.show()  
  pio.write_html(fig, file='indexplot.html', auto_open=True)

def onConnect(ws, response):
  logging.info('Ticker connection successful.')

def onDisconnect(ws, code, reason):
  logging.error('Ticker got disconnected. code = %d, reason = %s', code, reason)

def onError(ws, code, reason):
  logging.error('Ticker errored out. code = %d, reason = %s', code, reason)

def onReconnect(ws, attemptsCount):
  logging.warn('Ticker reconnecting.. attemptsCount = %d', attemptsCount)

def onMaxReconnectsAttempt(ws):
  logging.error('Ticker max auto reconnects attempted and giving up..')

def onOrderUpdate(ws, data):
  logging.info('Ticker: order update %s', data)




