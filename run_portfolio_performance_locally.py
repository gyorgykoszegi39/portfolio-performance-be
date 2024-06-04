from pandas import read_csv
import matplotlib.pyplot as plt
import utilities.data_processing as dp
import visualization.ploting as dia

if __name__ == "__main__":
    INVESTMENT = 1e6
    etf_prices = read_csv('px_etf.csv', parse_dates=['Date'], index_col='Date')
    START_DATE = etf_prices.index[0]
    END_DATE = etf_prices.index[-1]
    ETFS = etf_prices.columns.values

    etf_prices = dp.get_etf_prices(START_DATE, END_DATE)
    etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, ETFS)
    positions_value = dp.get_positions_value(etf_prices, etf_quantities)
    cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT)
    m_portfolio_perf = dp.get_portfolio_performance(positions_value, cash_flow, START_DATE, END_DATE, 'M')
    y_portfolio_perf = dp.get_portfolio_performance(positions_value, cash_flow, START_DATE, END_DATE, 'Y')

    print("Monthly Portfolio Performance")
    print(m_portfolio_perf.to_string(index=True, justify='left'))
    print("Annual Portfolio Performance")
    print(y_portfolio_perf.to_string(index=True, justify='left'))

    dia.create_plot_data(etf_prices, "ETF Prices Over Time")
    dia.create_plot_data(etf_prices * etf_quantities , "Positions Value per ETF Over Time")
    dia.create_plot_data(positions_value, "Positions Value Over Time")
    dia.create_plot_data(cash_flow, "Cash on Hand Over Time")
    dia.create_plot_data(cash_flow + positions_value, "Combined Cash Flow and Positions Value Over Time")

    m_portfolio_perf_usd = m_portfolio_perf.drop(columns=['% value'])
    m_portfolio_perf_perc = m_portfolio_perf.drop(columns=['USD value'])
    y_portfolio_perf_usd = y_portfolio_perf.drop(columns=['% value'])
    y_portfolio_perf_perc = y_portfolio_perf.drop(columns=['USD value'])
    dia.create_plot_data(m_portfolio_perf_usd, "Monthly Portfolio Performance")
    dia.create_plot_data(m_portfolio_perf_perc, "Monthly Portfolio Performance", ylabel='% value')
    dia.create_plot_data(y_portfolio_perf_usd, "Annual Portfolio Performance")
    dia.create_plot_data(y_portfolio_perf_perc, "Annual Portfolio Performance", ylabel='% value')

    plt.show()

    standard_deviation_of_daily_returns = dp.get_standard_deviation_of_daily_returns(positions_value, cash_flow)
    print(f"Standard deviation value of the portfolio daily returns: {standard_deviation_of_daily_returns}")
