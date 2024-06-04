'''
data_processing.py

This module provides basic operation to read and calculate the performance of the portfolio.

Functions:
    get_etf_prices(start_date, end_date, exclude_etfs): Returns ETF prices over time.
    get_etf_quantities(start_date, end_date, etfs): Returns ETF quantities over time.
    get_positions_value(etf_prices, etf_quantities): Returns positions value over time.
    get_cash_flow(etf_prices, start_date, end_date, invested_cash, exclude_etfs):
        Returns cash on hand over time.
    get_portfolio_performance(positions_value, cash_flow, start_date, end_date, freq):
        Returns monthly or annualy performance of portfolio.
    get_standard_deviation_of_daily_returns(positions_value, cash_flow):
        Returns the standard deviation of daily returns.
    calculate_standard_deviation(data): Returns the standard deviation of a dataset.
'''

from typing import List, Optional
import math
from pandas import Timestamp, DataFrame, date_range, period_range, read_csv

def get_etf_prices(
    start_date: Timestamp,
    end_date: Timestamp,
    exclude_etfs: Optional[List[str]] = None
) -> DataFrame:
    '''
    Reads ETFs prices from px_etf.csv and fills in the mising dates with forward filling.

    Args:
        start_date: 
            In case of missing data acts as the date from which data should be prefilled.
        end_date: 
            In case of missing data acts as the date until which data should be forward filled.
        exclude_etfs:
            Optional. List of string ETFs to exclude from dataset, by default is empty.

    Returns:
        Returns pandas DataFrame of ETFs prices, where date is the index and ETFs are columns.
    '''
    if exclude_etfs is None:
        exclude_etfs = []
    etf_prices = read_csv('px_etf.csv', parse_dates=['Date'], index_col='Date')
    etf_prices = etf_prices.drop(exclude_etfs, axis=1)
    all_dates = DataFrame(index=date_range(start=start_date, end=end_date, freq='D'))
    etf_prices = etf_prices.reindex(all_dates.index).ffill()

    return etf_prices

def get_etf_quantities(start_date: Timestamp, end_date: Timestamp, etfs: List[str]) -> DataFrame:
    '''
    Reads ETFs quantities from tx_etf.csv and fills in the mising dates with forward filling.

    Args:
        start_date: 
            In case of missing data acts as the date from which data should be prefilled.
        end_date: 
            In case of missing data acts as the date until which data should be forward filled.
        etfs:
            List of string ETFs the dataset should contain.

    Returns:
        Returns pandas DataFrame of ETFs quantities, where date is the index and ETFs are columns.
    '''
    etf_transactions = read_csv('tx_etf.csv', parse_dates=['date'])
    etf_quantities = DataFrame(index=etf_transactions['date'].unique(), columns=etfs)
    prev_date = None
    for _, etf_transaction in etf_transactions.iterrows():
        ticker = etf_transaction['ticker']
        if ticker not in etfs:
            continue
        date = etf_transaction['date']
        qty = etf_transaction['qty']
        order_type = etf_transaction['order']

        if prev_date != date:
            if prev_date is not None:
                etf_quantities.loc[date] = etf_quantities.loc[prev_date]
            else:
                etf_quantities.loc[date] = 0

            prev_date = date

        etf_quantities.at[date, ticker] +=  (qty if order_type == 'BUY' else -qty)

    all_dates = DataFrame(index=date_range(start=start_date, end=end_date, freq='D'))
    etf_quantities = etf_quantities.reindex(all_dates.index).infer_objects(copy=False).ffill()
    etf_quantities = etf_quantities.astype(int)
    return etf_quantities

def get_positions_value(etf_prices: DataFrame, etf_quantities: DataFrame) -> DataFrame:
    '''
    Calculates positions value over time.

    Args:
        etf_prices: 
            The price of ETFS over time.
        etf_quantities: 
            The quantity of ETFS over time.
            
    Returns:
        Returns pandas DataFrame of positions value,
        where date is the index and position value is the column.
    '''
    return DataFrame((etf_prices * etf_quantities).sum(axis=1), columns=["value"])

def get_cash_flow(
    etf_prices: DataFrame,
    start_date: Timestamp,
    end_date: Timestamp,
    invested_cash: float,
    exclude_etfs: Optional[List[str]] = None
) -> DataFrame:
    '''
    Updates the cash value by reading the transactions from tx_etf.csv
    and fills in the mising dates with forward filling.

    Args:
        etf_prices:
            The price of ETFS over time.
        start_date: 
            In case of missing data acts as the date from which data should be prefilled.
        end_date:
            In case of missing data acts as the date until which data should be forward filled.
        invested_cash:
            The starting sum of the investment.
        exclude_etfs:
            Optional. List of string ETFs to exclude from dataset, by default is empty.

    Returns:
        Returns pandas DataFrame of cash flow, 
        where date is the index and the cash value is the column.
    '''
    if exclude_etfs is None:
        exclude_etfs = []
    etf_transactions = read_csv('tx_etf.csv', parse_dates=['date'])
    cash_flows = DataFrame(
        index=date_range(start=start_date, end=end_date, freq='D'),
        columns=["value"]
    )
    current_cash = invested_cash
    for _, etf_transaction in etf_transactions.iterrows():
        ticker = etf_transaction['ticker']
        if ticker in exclude_etfs:
            continue

        date = etf_transaction['date']
        qty = etf_transaction['qty']
        price = etf_prices.at[date, ticker]
        order_type = etf_transaction['order']

        current_cash += price * (-qty if order_type == 'BUY' else qty)
        cash_flows.at[date, "value"] = current_cash

    cash_flows = cash_flows.infer_objects(copy=False).ffill()

    return cash_flows

def get_portfolio_performance(
    positions_value: DataFrame,
    cash_flow: DataFrame,
    start_date: Timestamp,
    end_date: Timestamp,
    freq: str ='Y'
) -> DataFrame:
    '''
    Calculates the portfolio performance.

    Args:
        positions_value:
            The positions value of ETFS over time.
        cash_flow:
            The cash on hand value over time.
        start_date: 
            The starting date of calculating the portfolio performance.
        end_date: 
            The ending date of calculating the portfolio performance.
        freq:
            Accepted values are 'Y' - yearly or 'M' - monthly. By default is 'Y'.
        exclude_etfs:
            List of string ETFs to exclude from dataset.

    Returns:
        Returns pandas DataFrame of portfolio performance in USD and %, 
        where date is the index and portfolio perfoamnce in USD and % are the columns.
        
    Raises: 
        ValueError: Frequency must be either 'M' for monthly portfolio performance 
        or 'Y' for annually portfolio performance
    '''
    if freq not in ['M', 'Y']:
        raise ValueError("Frequency must be either 'M' for monthly portfolio performance "
            "or 'Y' for annually portfolio performance")

    periods = period_range(start=start_date, end=end_date, freq=freq)
    period_tuples = [(period.start_time, period.end_time.floor('D')) for period in periods]
    period_tuples[0] = (start_date, period_tuples[0][1])
    period_tuples[-1] = (period_tuples[-1][0], end_date)

    end_dates = [start_date] + [end_date for _, end_date in period_tuples]
    portfolio_perf = DataFrame(index=end_dates, columns=['USD value', '% value'])
    portfolio_perf.loc[start_date] = 0
    for s_date, e_date in period_tuples:
        start_value = positions_value.at[s_date, 'value'] + cash_flow.at[s_date, 'value']
        end_value = positions_value.at[e_date, 'value'] + cash_flow.at[e_date, 'value']
        portfolio_perf.at[e_date, 'USD value'] = end_value - start_value

        if start_value == 0:
            portfolio_perf.at[e_date, '% value'] = 0
            continue

        portfolio_perf.at[e_date, '% value'] = (end_value - start_value) / start_value * 100

    return portfolio_perf

def get_standard_deviation_of_daily_returns(
    positions_value: DataFrame,
    cash_flow: DataFrame
) -> DataFrame:
    '''
    Calculates the standard deviation of daily returns.

    Args:
        positions_value:
            The positions value of ETFS over time.
        cash_flow:
            The cash on hand value over time.

    Returns:
        Returns the standard deviation of daily returns as a float number.
    '''
    portfolio_values = positions_value + cash_flow
    beginning_values = portfolio_values.shift(1)
    daily_returns = (portfolio_values - beginning_values) / beginning_values * 100

    standard_deviation = calculate_standard_deviation(daily_returns)
    return standard_deviation

def calculate_standard_deviation(data: DataFrame) -> float:
    '''Calculates and returns the standard deviation of a dataset.'''
    mean = data.mean()
    n = len(data)

    deviation_from_mean = mean - data
    deviation_from_mean = deviation_from_mean ** 2

    sum_deviation_from_mean = deviation_from_mean.sum().iloc[0]

    standard_deviation = math.sqrt(sum_deviation_from_mean / (n - 1))

    return standard_deviation
