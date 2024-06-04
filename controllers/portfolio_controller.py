# '''
# portfolio_controller.py

# This module orchestrates the reading and processing of the portfolio data.

# GET Endpoints:
#     /etf-prices
#         Returns a diagram of ETF prices over time.
#     /monthly-portfolio-performance
#         Returns diagrams and a table of the monthly portfolio performance in USD and %.
#     /annually-portfolio-performance
#         Returns diagrams and a table of the annual portfolio performance in USD and %.
#     /positions-value-per-etf
#         Returns a diagram of position value per ETF over time.
#     /positions-value
#         Returns a diagram of position value over time.
#     /cash-flow
#         Returns a diagram of cash on hand over time.
#     /combined-cash-flow-positions-value
#         Returns a diagram of the combined cash on hand and positions value over time.
#     /risk-measures
#         Returns the value of the standard deviation of daily returns.
# '''

# import json
# import threading
# from datetime import datetime
# from pandas import read_csv
# from fastapi import APIRouter, Query
# from fastapi.responses import StreamingResponse, JSONResponse
# import utilities.data_processing as dp
# import visualization.ploting as dia

# router = APIRouter()
# plot_lock = threading.Lock()

# input_data = read_csv('px_etf.csv', parse_dates=['Date'], index_col='Date')
# INVESTMENT = 1e6
# START_DATE = input_data.index[0]
# END_DATE = input_data.index[-1]
# ETFS = input_data.columns.values
# DEFAULT_DISPLAY_DATA_FROM = START_DATE.strftime('%d-%m-%Y')
# DEFAULT_DISPLAY_DATA_TO = END_DATE.strftime('%d-%m-%Y')
# DEFAULT_EXCLUDE_ETFS = json.dumps([])

# @router.get("/etf-prices")
# def get_etf_prices(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_prices_filtered = etf_prices.loc[display_data_from:display_data_to]

#     with plot_lock:
#         dia.create_plot_data(etf_prices_filtered, "ETF Prices Over Time")
#         plot_buffer = dia.write_plot_to_buffer()

#     return StreamingResponse(plot_buffer, media_type="image/png")

# @router.get("/monthly-portfolio-performance")
# def get_monthly_portfolio_performance(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)
#     cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT, exclude_etfs)
#     positions_value = dp.get_positions_value(etf_prices, etf_quantities)

#     m_portfolio_perf = dp.get_portfolio_performance(
#         positions_value,
#         cash_flow,
#         START_DATE,
#         END_DATE,
#         'M'
#     )

#     m_portfolio_perf_usd = m_portfolio_perf.drop(columns=['% value'])
#     m_portfolio_perf_perc = m_portfolio_perf.drop(columns=['USD value'])
#     m_portfolio_perf_usd_filtered = m_portfolio_perf_usd.loc[display_data_from:display_data_to]
#     m_portfolio_perf_perc_filtered = m_portfolio_perf_perc.loc[display_data_from:display_data_to]

#     with plot_lock:
#         dia.create_plot_data(m_portfolio_perf_usd_filtered, "Monthly Portfolio Performance")
#         plot_buffer_usd = dia.write_plot_to_buffer()
#         dia.create_plot_data(
#             m_portfolio_perf_perc_filtered,
#             "Monthly Portfolio Performance",
#             ylabel='% value')
#         plot_buffer_percentage = dia.write_plot_to_buffer()

#     m_portfolio_perf.index = m_portfolio_perf.index.astype(str)

#     return JSONResponse(content={
#         "value": m_portfolio_perf.to_dict(orient="index"),
#         "diagramUSD": dia.encode_image_to_base64(plot_buffer_usd),
#         "diagramPercentage": dia.encode_image_to_base64(plot_buffer_percentage)
#     })

# @router.get("/annually-portfolio-performance")
# def get_annually_portfolio_performance(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)
#     cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT, exclude_etfs)
#     positions_value = dp.get_positions_value(etf_prices, etf_quantities)

#     y_portfolio_perf = dp.get_portfolio_performance(
#         positions_value,
#         cash_flow,
#         START_DATE,
#         END_DATE,
#         'Y'
#     )

#     y_portfolio_perf_usd = y_portfolio_perf.drop(columns=['% value'])
#     y_portfolio_perf_perc  = y_portfolio_perf.drop(columns=['USD value'])
#     y_portfolio_perf_usd_filtered = y_portfolio_perf_usd.loc[display_data_from:display_data_to]
#     y_portfolio_perf_perc_filtered = y_portfolio_perf_perc.loc[display_data_from:display_data_to]
#     with plot_lock:
#         dia.create_plot_data(y_portfolio_perf_usd_filtered, "Annually Portfolio Performance")
#         plot_buffer_usd = dia.write_plot_to_buffer()
#         dia.create_plot_data(
#             y_portfolio_perf_perc_filtered,
#             "Annually Portfolio Performance",
#             ylabel='% value'
#         )
#         plot_buffer_percentage = dia.write_plot_to_buffer()

#     y_portfolio_perf.index = y_portfolio_perf.index.astype(str)

#     return JSONResponse(content={
#         "value": y_portfolio_perf.to_dict(orient="index"),
#         "diagramUSD": dia.encode_image_to_base64(plot_buffer_usd),
#         "diagramPercentage": dia.encode_image_to_base64(plot_buffer_percentage)
#     })

# @router.get("/positions-value-per-etf")
# def get_positions_value_per_etf(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)

#     value_per_etf = etf_prices * etf_quantities
#     value_per_etf_filtered = value_per_etf.loc[display_data_from:display_data_to]
#     with plot_lock:
#         dia.create_plot_data(value_per_etf_filtered, "Positions Value for an ETF Over Time")
#         plot_buffer = dia.write_plot_to_buffer()

#     return StreamingResponse(plot_buffer, media_type="image/png")

# @router.get("/positions-value")
# def get_positions_value(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)

#     positions_value = dp.get_positions_value(etf_prices, etf_quantities)
#     positions_value_filtered = positions_value.loc[display_data_from:display_data_to]

#     with plot_lock:
#         dia.create_plot_data(positions_value_filtered, "Positions Value Over Time")
#         plot_buffer = dia.write_plot_to_buffer()

#     return StreamingResponse(plot_buffer, media_type="image/png")

# @router.get("/cash-flow")
# def get_cash_flow(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT, exclude_etfs)
#     cash_flow_filtered = cash_flow.loc[display_data_from:display_data_to]

#     with plot_lock:
#         dia.create_plot_data(cash_flow_filtered, "Cash on Hand Over Time")
#         plot_buffer = dia.write_plot_to_buffer()

#     return StreamingResponse(plot_buffer, media_type="image/png")

# @router.get("/combined-cash-flow-positions-value")
# def get_combined_cash_flow_positions_value(
#     display_data_from: str = Query(default=DEFAULT_DISPLAY_DATA_FROM),
#     display_data_to: str = Query(default=DEFAULT_DISPLAY_DATA_TO),
#     exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)
# ):
#     display_data_from = datetime.strptime(display_data_from, '%d-%m-%Y')
#     display_data_to = datetime.strptime(display_data_to, '%d-%m-%Y')
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)
#     cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT, exclude_etfs)
#     positions_value = dp.get_positions_value(etf_prices, etf_quantities)
#     combined_cash_position_value = cash_flow + positions_value
#     combined_filtered = combined_cash_position_value.loc[display_data_from:display_data_to]

#     with plot_lock:
#         dia.create_plot_data(
#             combined_filtered,
#             "Combined Cash Flow and Positions Value Over Time"
#         )
#         plot_buffer = dia.write_plot_to_buffer()

#     return StreamingResponse(plot_buffer, media_type="image/png")

# @router.get("/risk-measures")
# def get_risk_measures(exclude_etfs: str = Query(default=DEFAULT_EXCLUDE_ETFS)):
#     exclude_etfs = json.loads(exclude_etfs)
#     etfs_filtered = [x for x in ETFS if x not in exclude_etfs]

#     etf_prices = dp.get_etf_prices(START_DATE, END_DATE, exclude_etfs)
#     etf_quantities = dp.get_etf_quantities(START_DATE, END_DATE, etfs_filtered)
#     cash_flow = dp.get_cash_flow(etf_prices, START_DATE, END_DATE, INVESTMENT, exclude_etfs)
#     positions_value = dp.get_positions_value(etf_prices, etf_quantities)
#     std_deviation = dp.get_standard_deviation_of_daily_returns(positions_value, cash_flow)

#     return JSONResponse(content= {
#         "standardDeviation": std_deviation,
#         "description": "Standard deviation of daily returns(%)."
#     })
