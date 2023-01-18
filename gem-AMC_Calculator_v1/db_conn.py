import mysql.connector
import configparser
import traceback
import time
import datetime


config = configparser.ConfigParser()
config.read('config.ini')

def myconn():
	try:
		conn = mysql.connector.connect(
		host=config.get('Databases','host'),
		user=config.get('Databases','user'),
		password=config.get('Databases','password'),
		database=config.get('Databases','databases')
		)
	except:
		print('Something went wrong in database connection')
		return None
	return conn

def insert_data(df):

  conn = myconn()
  cursor = conn.cursor(buffered=True)
  for data in df:

    try:
      t_time = datetime.datetime.strptime(data['expiryDate'],  '%d-%b-%Y').strftime('%Y-%m-%d')
      current_date_time =  datetime.date.today().strftime('%Y-%m-%d')
      
      try:
        cursor.execute('''
          INSERT INTO Security_master (security_type, market_ticker, Expiration_date,strike_price,underlying_ticker,underlying_exp_date,ticker_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
              ''',
              (data['sec_type'],
              data['mkt_ticker'],
              t_time,
              data['strikePrice'],
              data['underlying_ticker'],
              t_time,
              data['ticker_type']
            ))
                
      except:
        pass
      query = "SELECT master_id FROM Security_master WHERE (market_ticker='%s' and Expiration_date='%s' and strike_price='%s' and ticker_type = '%s')"%(str(data['mkt_ticker']),str(t_time),str(data['strikePrice']),str(data['ticker_type']))
      cursor.execute(query)
      MasterId = cursor.fetchall()
      try:
        cursor.execute('''
          INSERT INTO MetaData (ticker_name, field_name, frequency,source,master_id)
            VALUES (%s,%s,%s,%s,%s)
              ''',
              (data['ticker_name'],
              data['field'],
              data['frequency'],
              data['source'],
              MasterId[0][0]
            ))
      except:
        pass
      try:
        cursor.execute("SELECT meta_id FROM MetaData WHERE ticker_name='%s' "%str(data['ticker_name']))
        MetaId = cursor.fetchall()
        cursor.execute('''
          INSERT INTO Data (asof_date, value, meta_id)
            VALUES (%s,%s,%s)
              ''',
              (current_date_time,
              data['value'],
              MetaId[0][0]
            ))
      except:
        pass
    except Exception:
      print(traceback.format_exc())

  conn.commit()
  time.sleep(.0001)
  conn.close()
