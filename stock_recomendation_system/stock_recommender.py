# %% [markdown]
# # Requirements Setup

# %%
import sqlalchemy
import pymysql
import ta
import pandas as pd
import numpy as np
import yfinance as yf
import os

pymysql.install_as_MySQLdb()

# %% [markdown]
# # Recommender Class

# %%
class Recommender:
    password = os.getenv('PASSWORD')
    engine = sqlalchemy.create_engine(f'mysql://root:{password}@localhost:3306/')
    
    def __init__(self, index):
        self.index = index
        
    def get_tables(self):
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{self.index}'
        """
        df = pd.read_sql(query, self.engine)
        df['Schema'] = self.index
        return df
    
    def get_prices(self):
        prices = []
        for table, schema in zip(self.get_tables().TABLE_NAME, self.get_tables().Schema):
            sql = f'{schema}.`{table}`'
            prices.append(pd.read_sql(f'SELECT Date, Close FROM {sql}', self.engine))
        return prices
    
    def max_date(self):
        query = f"""
        SELECT MAX(Date)
        FROM {self.index}.`{self.get_tables().TABLE_NAME[0]}`
        """
        return pd.read_sql(query, self.engine)
    
    def update_database(self):
        max_date = self.max_date().iloc[0, 0]
        engine = sqlalchemy.create_engine(f'mysql://root:{self.password}@localhost:3306/{self.index}')
        for stock in self.get_tables().TABLE_NAME:
            data = yf.download(stock, start=max_date)
            data = data[data.index > max_date]
            data = data.reset_index()
            data.to_sql(stock, engine, if_exists='append', index=False)
        print(f'{self.index} successfully updated')
    
    def macd_decision(self, df):
        df['MACD_diff'] = ta.trend.macd_diff(df.Close)
        df['Decision_MACD'] = np.where((df.MACD_diff > 0) & (df.MACD_diff.shift(1) < 0),
                                       True, False)
    
    def golden_cross_decision(self, df):
        df['SMA_20'] = ta.trend.sma_indicator(df.Close, window=20)
        df['SMA_50'] = ta.trend.sma_indicator(df.Close, window=50)
        df['Signal'] = np.where(df.SMA_20 > df.SMA_50, True, False)
        df['Decision_GC'] = df.Signal.diff()
    
    def rsi_sma_decision(self, df):
        df['RSI'] = ta.momentum.rsi(df.Close, window=10)
        df['SMA_200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Decision_RSI_SMA'] = np.where((df.Close > df.SMA_200) & (df.RSI < 30),
                                          True, False)
    
    def apply_technics(self):
        prices = self.get_prices()
        for price in prices:
            self.macd_decision(price)
            self.golden_cross_decision(price)
            self.rsi_sma_decision(price)
        return prices
    
    def recommend(self):
        signals = []
        indicators = ['Decision_MACD', 'Decision_GC', 'Decision_RSI_SMA']
        for stock, price in zip(self.get_tables().TABLE_NAME, self.apply_technics()):
            if price.empty is False:
                for indicator in indicators:
                    if price[indicator].iloc[-1] is True:
                        signals.append(f'{indicator} Buying Signal for {stock}')
        if signals == []:
            signals.append('No Buying Signals')
        return signals

# %%
bovespa_instance = Recommender('bovespa')
nasdaq_instance = Recommender('nasdaq')

# %%
bovespa_instance.update_database()
nasdaq_instance.update_database()

# %%



