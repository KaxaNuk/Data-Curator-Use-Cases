"""
This module contains functions for creating cross-sectional time series data.
"""

import pyarrow
import pyarrow.compute
import datetime

def data_creator(
        *,
        start_date: datetime.date,
        end_date: datetime.date,
        column_date_tag: str,
        time_series_data_dictionary: dict[str, pyarrow.Table],
        weighted_scoring_features_tuple: tuple[str, ...],
) -> dict[str, pyarrow.Table]:
    """
    Create a PyArrow Table for cross-sectional time series data.

    Parameters
    ----------
    start_date : str
        Start date for filtering the data.
    end_date : str
        End date for filtering the data.
    time_series_data_dictionary : dict[str, pa.Table]
        Dictionary of PyArrow Tables for each ticker.
    weighted_scoring_features_tuple : tuple[str, ...]
        Tuple of features/columns to include.

    Returns
    -------
    dict[str, pa.Table]
        A dictionary of PyArrow Tables containing the filtered and combined features.
    """

    features_dict = {}

    for feature in weighted_scoring_features_tuple:
        result = None
        for ticker, time_series_table in time_series_data_dictionary.items():

            # Convert date column to timestamp for filtering
            table = time_series_table.set_column(
                0,
                column_date_tag,
                pyarrow.compute.cast(time_series_table[column_date_tag],
                                     pyarrow.timestamp("ms")))

            # Apply filtering condition
            mask = pyarrow.compute.and_(
                pyarrow.compute.greater_equal(table[column_date_tag], pyarrow.scalar(start_date)),
                pyarrow.compute.less_equal(table[column_date_tag], pyarrow.scalar(end_date)),
            )
            filtered_table = table.filter(mask)

            # Select only the "c_market_cap_on_open" column
            data = filtered_table.select([column_date_tag, feature])
            data = data.rename_columns([column_date_tag, ticker])


            if result is None:
                # First table initializes the result
                result = data.combine_chunks()
            elif len(result) > len(data):
                # Perform full_outer join using pyarrow.Table.join
                result = result.join(
                    data,
                    keys=column_date_tag,
                    right_suffix='_r'
                )
            else:
                # Perform full_outer join using pyarrow.Table.join
                result = data.join(
                    result,
                    keys=column_date_tag,
                    right_suffix='_r'
                )
            result = result.sort_by([(column_date_tag, "ascending")])

        # Store the feature-specific table in the dictionary
        features_dict[feature] = result

    return features_dict
