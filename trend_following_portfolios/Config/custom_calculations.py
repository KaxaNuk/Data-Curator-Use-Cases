"""
Easily add your own custom feature calculation function.

To add a custom calculation function, you need to have this file the Config folder under the
project's root directory (and not the templates directory!), and add your functions there.

Each function needs to start with c_ as a prefix, and the rest of the name can be anything as
long as it's a valid Python function name.

Each function declares as arguments the names of each column it needs as input, which
are provided to it as our custom DataColumn objects that act as pyarrow.Array wrappers
but with neat features like:
- operator overloading (so you can directly perform arithmetic operations between columns,
like in pandas)
- automatically casting any operations involving NaN or null elements as null, as we
consider any null a missing value

Each function needs to return an iterable supported by pyarrow.array(), of the same length
(preferably another DataColumn, a pyarrow.Array, a pandas.Series or a 1D numpy.ndarray).
The result will automatically be wrapped in a DataColumn for any successive functions that
use that as input. Yes, you can absolutely chain together functions, and are encouraged to
do so!

Once you've added your function to the file, you need to add its name to the Output_Columns
sheet of the parameters_datacurator.xlsx file. Don't forget that your function name needs to
start with c_ as a prefix!

See more examples of how easy it is to program custom functions by checking out the file
src/kaxanuk/data_curator/features/calculations.py
"""

import pyarrow

# Here you'll find helper functions for calculating more complicated features:
from kaxanuk.data_curator.modules.data_column import DataColumn


def c_trend_following_signal_252d(m_close_dividend_and_split_adjusted,
                      c_simple_moving_average_252d_close_dividend_and_split_adjusted
                      ):
    """
    Calculate a 252-day trend-following signal based on the relative position
    of the current price to its 252-day simple moving average.

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted closing price.
    c_simple_moving_average_252d_close_dividend_and_split_adjusted : DataColumn
        The 252-day simple moving average of the adjusted closing price.

    Returns
    -------
    DataColumn
        The trend signal.
    """
    # we're just doing a subtraction here, but you can implement any logic
    # just remember to return the same number of rows in a single column!
    signal = ((m_close_dividend_and_split_adjusted - c_simple_moving_average_252d_close_dividend_and_split_adjusted)
              / c_simple_moving_average_252d_close_dividend_and_split_adjusted)
              
    return DataColumn.load(signal)


def c_trend_following_signal_21d(m_close_dividend_and_split_adjusted,
                      c_simple_moving_average_21d_close_dividend_and_split_adjusted
                      ):
    """
    Calculate a 21-day trend-following signal based on the relative position
    of the current price to its 21-day simple moving average.

    Parameters
    ----------
    m_close_dividend_and_split_adjusted : DataColumn
        The adjusted closing price.
    c_simple_moving_average_21d_close_dividend_and_split_adjusted : DataColumn
        The 21-day simple moving average of the adjusted closing price.

    Returns
    -------
    DataColumn
        The trend signal.
    """
    # we're just doing a subtraction here, but you can implement any logic
    # just remember to return the same number of rows in a single column!
    signal = ((m_close_dividend_and_split_adjusted - c_simple_moving_average_21d_close_dividend_and_split_adjusted)
              / c_simple_moving_average_21d_close_dividend_and_split_adjusted)
              
    return DataColumn.load(signal)

def c_investable_universe_63d(c_daily_traded_value_sma_63d):
    """
    Identify whether a stock belongs to the investable universe based on
    its 63-day average traded value exceeding a minimum liquidity threshold.

    Parameters
    ----------
    c_daily_traded_value_sma_63d : DataColumn
        The 63-day simple moving average of daily traded value.

    Returns
    -------
    DataColumn
        A binary column: 1 if the stock is investable, 0 otherwise.
    """
    signal = c_daily_traded_value_sma_63d > 1000000000

    result = pyarrow.compute.if_else(
        signal.to_pyarrow(),
        1,  
        0 
        )

    return DataColumn.load(result)













