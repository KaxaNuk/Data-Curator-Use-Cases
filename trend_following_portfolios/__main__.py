"""
Example entry script implementation that reads configuration from an Excel file and outputs to csv or parquet files.

It initializes a Configuration object through our ExcelConfigurator module, selects its possible data provider
modules and output handler, and passes all these dependencies to data_curator.main().

Once you build the futures you plan to use, this code helps you build historical equal weight portfolios ranking the
top tickers using a trend following signal and doing monthly rebalances.
"""

import datetime
import logging
import os
import pathlib

import kaxanuk.data_curator
import pandas
import pyarrow.csv

import cross_sectional_time_series


# change this constant to False if you want to skip downloading data
DOWNLOAD_DATA_FLAG = True


# Load environment variables
kaxanuk.data_curator.load_config_env()

# Load custom calculations if available
if pathlib.Path('Config/custom_calculations.py').is_file():
    from Config import custom_calculations
    custom_calculation_modules = [custom_calculations]
else:
    custom_calculation_modules = []

output_base_dir = 'Output'

# Load configuration
configurator = kaxanuk.data_curator.config_handlers.ExcelConfigurator(
    file_path='Config/parameters_datacurator.xlsx',
    data_providers={
        'financial_modeling_prep': {
            'class': kaxanuk.data_curator.data_providers.FinancialModelingPrep,
            'api_key': os.getenv('KNDC_API_KEY_FMP'),
        },
        'yahoo_finance': {
            'class': kaxanuk.data_curator.load_data_provider_extension(
                extension_name='yahoo_finance',
                extension_class_name='YahooFinance',
            ),
            'api_key': None
        },
    },
    output_handlers={
        'csv': kaxanuk.data_curator.output_handlers.CsvOutput(
            output_base_dir=output_base_dir,
        ),
        'parquet': kaxanuk.data_curator.output_handlers.ParquetOutput(
            output_base_dir=output_base_dir,
        ),
    },
)

if DOWNLOAD_DATA_FLAG:
    kaxanuk.data_curator.main(
        configuration=configurator.get_configuration(),
        market_data_provider=configurator.get_market_data_provider(),
        fundamental_data_provider=configurator.get_fundamental_data_provider(),
        output_handlers=[configurator.get_output_handler()],
        custom_calculation_modules=custom_calculation_modules,
        logger_level=configurator.get_logger_level(),
    )

# Express Configuration
configuration = configurator.get_configuration()
column_date_tag = 'm_date'
weighted_scoring_features_tuple = (
    'c_trend_following_signal_252d',
    'c_trend_following_signal_21d',
    'c_investable_universe_63d',
)

# === Global Dates ===
start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2025, 6, 30)

# Load Data
time_series_data = {}
for main_identifier in configuration.identifiers:
    logging.getLogger(__name__).info(
        'Loading data for main_identifier %s',
        main_identifier
    )
    data = pyarrow.csv.read_csv(f'{output_base_dir}/{main_identifier}.csv')
    time_series_data[main_identifier] = data

    # Create Cross Sectional Time Series
    cross_sectional_time_series_data = cross_sectional_time_series.data_creator(
        start_date=start_date,
        end_date=end_date,
        column_date_tag=column_date_tag,
        time_series_data_dictionary=time_series_data,
        weighted_scoring_features_tuple=weighted_scoring_features_tuple,
    )

    for (main_identifier, cross_sectional_time_series_table) in cross_sectional_time_series_data.items():
        cross_sectional_time_series_table = cross_sectional_time_series_table.to_pandas()
        cross_sectional_time_series_table.to_csv(f'{output_base_dir}/{main_identifier}.csv', index=False)

# === Create Rebalancing Signal ===
window_days = 5
threshold = 0.025
target_symbol = 'SPY'
signal_column_name = 'c_log_difference_high_to_low'
output_column_name = 'rebalancing_signal'
rebalancing_dates_file_name = f'rebalance_signal_{target_symbol}'

if target_symbol in time_series_data:
    df = time_series_data[target_symbol].to_pandas()
    df['m_date'] = pandas.to_datetime(df['m_date'])
    df = df.sort_values('m_date')
    df = df[
        (df['m_date'] >= pandas.Timestamp(start_date)) &
        (df['m_date'] <= pandas.Timestamp(end_date))
    ].copy()

    signal_values = df[signal_column_name].fillna(0).values
    rebalance_signal = [0] * len(signal_values)
    count = 0

    for i in range(len(signal_values)):
        if signal_values[i] > threshold:
            count += 1
        else:
            count = 0

        if count >= window_days and i + 1 < len(signal_values):
            rebalance_signal[i] = 1

    df[output_column_name] = rebalance_signal
    df[['m_date', output_column_name]].to_csv(
        f'{output_base_dir}/{rebalancing_dates_file_name}.csv',
        index=False
    )

# === Portfolio Construction Inputs ===
strategy_signals_file_name = 'c_trend_following_signal_21d'
investable_universe_file_name = 'c_investable_universe_63d'

# Load input files
investable_universe = pyarrow.csv.read_csv(f'{output_base_dir}/{investable_universe_file_name}.csv')
rebalancing_dates = pyarrow.csv.read_csv(f'{output_base_dir}/{rebalancing_dates_file_name}.csv')
strategy_signals = pyarrow.csv.read_csv(f'{output_base_dir}/{strategy_signals_file_name}.csv')

# Extract date and stock columns
date_column = strategy_signals.column(column_date_tag)
stock_columns = [col for col in strategy_signals.column_names if col != column_date_tag]

# Rebalancing dates where signal = 1
rebalancing_dates_filtered = {
    rebalancing_dates.column(column_date_tag)[i].as_py()
    for i in range(rebalancing_dates.num_rows)
    if rebalancing_dates.column('rebalancing_signal')[i].as_py() == 1
}

# Build investable universe map
investable_stocks_by_date = {}
for row_index in range(investable_universe.num_rows):
    date = investable_universe.column(column_date_tag)[row_index].as_py()
    eligible_stocks = {
        stock for stock in stock_columns if investable_universe.column(stock)[row_index].as_py() == 1
    }
    investable_stocks_by_date[date] = eligible_stocks

# Build weights
stock_weights = {stock: {} for stock in stock_columns}
for row_index in range(strategy_signals.num_rows):
    current_date = date_column[row_index].as_py()
    if current_date not in rebalancing_dates_filtered:
        continue

    eligible_stocks = investable_stocks_by_date.get(current_date, set())
    if not eligible_stocks:
        continue

    stock_signals = {}
    for stock in stock_columns:
        if stock in eligible_stocks:
            signal_value = strategy_signals.column(stock)[row_index].as_py()
            if signal_value is not None:
                stock_signals[stock] = signal_value

    n_top_stocks = 5
    sorted_signals = sorted(stock_signals.items(), key=lambda x: x[1], reverse=True)
    top_n_stocks = sorted_signals[:n_top_stocks]
    weight = 1.0 / n_top_stocks if top_n_stocks else 0

    for stock in stock_columns:
        stock_weights[stock][date_column[row_index + 1].as_py()] = weight if stock in dict(top_n_stocks) else 0

# Output portfolio
portfolio_rows = [
    {'Ticker': stock, **{str(date): weight for date, weight in weights.items()}}
    for stock, weights in stock_weights.items()
]
portfolio_table = pyarrow.Table.from_pylist(portfolio_rows)
pyarrow.csv.write_csv(portfolio_table, f'{output_base_dir}/portfolio.csv')
