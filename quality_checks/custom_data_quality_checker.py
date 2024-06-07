"""
The 'DataQualityChecker' class performs data quality checks on
a DataFrame using Snowpark's session.
It takes two arguments: 'session' for Snowpark
connectivity and 'data_quality_checks',
a list of checks to be performed. The class separates the DataFrame
into correct and incorrect data,
using the main method `apply_data_quality_checks`, which triggers
all the specified checks.
"""

import logging
import pandas as pd
import snowflake.snowpark as snowpark
import zipcodes
from datetime import datetime, timedelta
import numpy as np
import json
from snowflake.snowpark.functions import parse_json, lit
from snowflake.snowpark.types import VariantType


class CustomDataQualityChecker:
    def __init__(
        self, session, data_quality_checks, input_dataset=None, output_datasets=None
    ):
        self.session = session
        self.data_quality_checks = data_quality_checks
        self.input_dataset = input_dataset
        self.output_datasets = output_datasets
        self.deleted_rows = pd.DataFrame()

    def duplicate_check(self, df, duplicate_check_config, custom_index_values):
        """
        'duplicate_check' method to identify and handle duplicate rows in a
        DataFrame based on the specified column names.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for duplicates.
            duplicate_check_config (dict): Configuration details for duplicate
            checks.
        Returns:
        dataframe after adding '_IS_DUPLICATE' extension for respective columns
        with their boolean values.
        """

        # for checking keys and values
        for key, value in duplicate_check_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,re-check"
                        f"duplicate_check_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,re-check"
                        f"your duplicate_check_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,re-check your"
                    f"duplicate_check_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,re-check your"
                    f"duplicate_check_config"
                )

        input_col_names = duplicate_check_config["input_col_names"]
        action = duplicate_check_config["action"]

        try:
            for col_name in input_col_names:
                duplicate_col_name = col_name + "_IS_DUPLICATE"
                df[duplicate_col_name] = df.duplicated(subset=col_name, keep="first")
                deleted_rows = df[df[duplicate_col_name]]
                custom_index_values["error_rows_index_set"].update(deleted_rows.index)

                if action == "drop":
                    custom_index_values["drop_rows_index_set"].update(
                        deleted_rows.index
                    )

        except Exception as e:
            logging.error(f"Error occured in duplicate_check: {e}")
            raise e

        return df, custom_index_values

    def is_null_check(self, df, is_null_check_config, custom_index_values):
        """
        'is_null_check' method identifies and handles Null rows in a DataFrame
        based on the specified column names.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for null values.
            is_null_check_config (dict): Configuration details for null checks.
        Returns:
        dataframe after adding '_IS_NULL' extension for respective columns
        with their boolean values.
        """

        # for checking keys and values
        for key, value in is_null_check_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,re-check"
                        f"your null_check_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,re-check"
                        f"your null_check_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,re-check"
                    f"your null_check_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,re-check your"
                    f"null_check_config"
                )

        input_col_names = is_null_check_config["input_col_names"]
        action = is_null_check_config["action"]

        if input_col_names is None or len(input_col_names) == 0:
            logging.error("No input columns provided, please re-check your config file")

        try:
            """
            Using .copy() while iterating prevents the "SettingWithCopyWarning"
            caused by DataFrame slicing and ensures that modifications within
            the loop don't affect the original DataFrame.
            """
            df = df.copy()

            for col_name in input_col_names:
                null_col_name = col_name + "_IS_NULL"
                df[null_col_name] = df[col_name].isnull()
                deleted_rows = df[df[null_col_name]]
                custom_index_values["error_rows_index_set"].update(deleted_rows.index)

                if action == "drop":
                    custom_index_values["error_rows_index_set"].update(
                        deleted_rows.index
                    )

        except Exception as e:
            logging.error(f"Error occurred in is_null_check: {e}")
            raise e

        return df, custom_index_values

    def in_range_check(self, df, in_range_config, custom_index_values):
        """
        'in_range_check' method identifies and handles rows value to be in range in
        a DataFrame based on the specified column names.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for range values.
            in_range_config (dict): Configuration details for range checks.
        Returns:
        dataframe after adding '_NOT_IN_RANGE' extension for respective columns
        with their boolean values.
        """

        # for checking keys and values
        for key, value in in_range_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,re-check"
                        f"your in_range_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your in_range_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,re-check your"
                    f"in_range_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,re-check"
                    f"your in_range_config"
                )

        input_col_names = in_range_config["input_col_names"]
        min_value = in_range_config["min_value"]
        max_value = in_range_config["max_value"]
        action = in_range_config["action"]

        try:
            for col_name in input_col_names:
                range_col_name = col_name + "_NOT_IN_RANGE"
                df[range_col_name] = ~df[col_name].between(min_value, max_value)
                deleted_rows = df[df[range_col_name]]

                custom_index_values["error_rows_index_set"].update(deleted_rows.index)

                if action == "drop":
                    custom_index_values["drop_rows_index_set"].update(
                        deleted_rows.index
                    )

        except Exception as e:
            logging.error(f"Error occurred in in_range_check: {e}")
            raise e

        return df, custom_index_values

    # Date format checker function
    def check_date_format(self, df, date_format_check_config, custom_index_values):
        """
        'check_date_format' method validates the date formats in specified columns
        of a DataFrame.
        It converts the columns to datetime format and checks for valid dates
        within the provided range.
        Invalid dates are flagged in a new column.

        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for date format.
            date_format_check_config (dict): Configuration details for date format
            checks.

        Returns:
            pd.DataFrame: DataFrame with added '_INVALID_FORMAT' extension for
            respective columns
            containing boolean values indicating invalid date formats.
        """

        input_col_names = date_format_check_config["input_col_names"]
        min_value = date_format_check_config["min_value"]
        max_value = date_format_check_config["max_value"]
        action = date_format_check_config["action"]

        try:
            if (max_value is None or max_value == "") and (
                min_value is None or min_value == ""
            ):
                min_value = pd.Timestamp.min
                max_value = pd.Timestamp.max
            elif max_value is None or max_value == "":
                max_value = pd.Timestamp.max
            elif min_value is None or min_value == "":
                min_value = pd.Timestamp.min

            for col_name in input_col_names:
                date_format_col_name = col_name + "_INVALID_FORMAT"
                df[date_format_col_name] = False  # Initialize the new column

                # Convert the column to  check for valid dates
                try:
                    df[col_name] = pd.to_datetime(df[col_name], errors="ignore")

                    df[date_format_col_name] = ~df[col_name].between(
                        min_value, max_value
                    )
                    deleted_rows = df[df[date_format_col_name]]

                    custom_index_values["error_rows_index_set"].update(
                        deleted_rows.index
                    )

                    if action == "drop":
                        custom_index_values["drop_rows_index_set"].update(
                            deleted_rows.index
                        )

                except Exception as e:
                    logging.error(
                        f"""Error occurred while checking date format
                                  for column {col_name}: {e}"""
                    )
                    raise e

        except Exception as e:
            logging.error(f"Error occurred in check_date_format: {e}")
            return None

        return df, custom_index_values

    def is_valid_numeric(self, value):
        """
        'is_valid_numeric' method checks if a given value can be converted to a
        numeric type (float or int).
        It attempts to convert the value and returns True if successful, otherwise
        returns False.

        Parameters:
            value: The value to be checked for numeric validity.

        Returns:
            bool: True if the value can be converted to a numeric type, False
            otherwise.
        """

        try:
            if type(float(value)) == float or type(int(value)) == int:
                return False
        except ValueError:
            logging.info("Value:", value)
            return True

    def process_numeric_data(self, df, numeric_check_config, custom_index_values):
        """
        'process_numeric_data' method processes the numeric data validation for
        specified columns in a DataFrame.
        It applies the 'is_valid_numeric' method to each element in the columns
        and creates a new column indicating
        whether each value is numeric.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the columns to be
            processed.
            numeric_check_config (dict): Configuration details for numeric data
            processing.
        Returns:
            pd.DataFrame: DataFrame with added '_IS_NUMERIC' extension for
            specified columns,
            containing boolean values indicating whether values are numeric.
        """
        input_col_names = numeric_check_config["input_col_names"]
        action = numeric_check_config["action"]

        try:
            for col in input_col_names:
                # col_name = col.split('_validate')[0]
                numeric_col_name = col + "_NOT_NUMERIC"
                df[numeric_col_name] = df[col].apply(lambda x: self.is_valid_numeric(x))

                deleted_rows = df[(df[numeric_col_name])]

                custom_index_values["error_rows_index_set"].update(deleted_rows.index)

                if action == "drop":
                    custom_index_values["drop_rows_index_set"].update(
                        deleted_rows.index
                    )

        except Exception as e:
            logging.error(f"Error occurred in process_numeric_data: {e}")

        return df, custom_index_values

    def get_unique_batch_ids(self, input_df):
        """
        Retrieves unique batch IDs from the input table.

        Args:
            input_df (DataFrame): Input DataFrame containing the data.

        Returns:
            DataFrame: A DataFrame containing unique batch IDs.
        """
        try:
            unique_batch_ids = input_df.select("batch_id").distinct()
            return unique_batch_ids
        except Exception as e:
            # Handle exceptions here, include function name in the error message using logging
            function_name = "get_unique_batch_ids"
            error_message = f"An error occurred in function {function_name}: {str(e)}"
            logging.error(error_message)
            return None

    def getDataByDays(self, input_table_data, outliers_config):
        try:
            column_names = input_table_data.columns
            print("Column name in getDataByDays: %s", column_names)

            # Check if historical_date_column exists in the DataFrame columns
            historical_date_column = outliers_config["historical_date_column"]
            if historical_date_column not in input_table_data.columns:
                raise ValueError(f"Column '{historical_date_column}' not found in the DataFrame.")

            historical_datapoints_days = outliers_config["historical_datapoints_days"]

            # Assuming historical_date_column is a valid column in input_table_data
            records_historical_datapoints_days = input_table_data[input_table_data[historical_date_column] >= (datetime.now() - timedelta(days=historical_datapoints_days))]

            return records_historical_datapoints_days
        except KeyError as ke:
            # Handle missing key in outliers_config
            logging.error("KeyError: %s. Please check the configuration.", str(ke))
            return input_table_data
        except ValueError as ve:
            # Handle column not found exception
            logging.error(str(ve))
            return input_table_data
        except Exception as e:
            # Handle other exceptions
            logging.error("An error occurred: %s", str(e))
            return input_table_data

    def data_profiling(self, input_df, quality_checks_config, input_config_data):
        column_names = input_df.columns
        print("Column name in getDataByDays: %s", column_names)
        try:
            result_df = []
            input_df = self.session.createDataFrame(input_df)

            input_data_db = input_config_data["input_data_db"]
            input_data_schema = input_config_data["input_data_schema"]
            input_dataset = input_config_data["input_data_source"]
            input_table_name = f"{input_data_db}.{input_data_schema}.{input_dataset}"

            unique_batch_ids = self.get_unique_batch_ids(input_df)
            unique_batch_ids.show()
            total_count = input_df.count()
            for row in unique_batch_ids.collect():
                try:
                    outliers_description_list = []
                    batch_id = row[0]
                    logging.info(f"Processing Batch ID: {batch_id}")
                    batch_df = input_df.filter(input_df["BATCH_ID"] == batch_id)
                    if batch_df.count() == 0:
                        logging.warning(f"Batch ID {batch_id} has no data.")
                    if "numerical_data_profiling" in quality_checks_config:
                        numerical_data_profiling = quality_checks_config[
                            "numerical_data_profiling"
                        ]
                        numerical_profiling_result = self.numeric_data_profiling(
                            batch_df, numerical_data_profiling, total_count
                        )
                    else:
                        numerical_data_profiling = []
                        numerical_profiling_result = []

                    # Check if categorical_data_profiling exists
                    if "categorical_data_profiling" in quality_checks_config:
                        categorical_data_profiling = quality_checks_config[
                            "categorical_data_profiling"
                        ]
                        categorical_profiling_result = self.categorical_data_profiling(
                            batch_df, categorical_data_profiling
                        )
                    else:
                        categorical_data_profiling = []
                        categorical_profiling_result = []

                    filtered_records_count = batch_df.count()
                    for outliers_config in quality_checks_config["outliers"]:
                        df_to_calculate_outliers = self.getDataByDays(
                            input_df, outliers_config
                        )
                        is_outlier, description_list = self.check_outlier(
                            df_to_calculate_outliers, batch_df, outliers_config, batch_id
                        )
                        outliers_description_list += description_list
                    result_df.append(
                        {
                            "Input_Table_Name": input_table_name,
                            "Input_Column_Name": {
                                "numeric": numerical_data_profiling,
                                "categorical": categorical_data_profiling,
                            },
                            "Profiling_Summary": {
                                "numeric": numerical_profiling_result,
                                "categorical": categorical_profiling_result,
                            },
                            "Date_of_Profiling": datetime.utcnow(),
                            "Records_Processed": filtered_records_count,
                            "Batch_Id": batch_id,
                            "data_outlier": is_outlier,
                            "data_outlier_description": outliers_description_list,
                            "Total_data_volume": total_count,
                            "is_volume_outlier": None,
                            "is_velocity_outlier": None,
                        }
                    )
                except Exception as inner_exception:
                    logging.error(
                        f"An exception occurred processing Batch ID {batch_id}: {inner_exception}"
                    )

            if result_df is None or (isinstance(result_df, pd.DataFrame) and result_df.empty) or (isinstance(result_df, list) and len(result_df) == 0):
                updated_result_df = self.session.createDataFrame([])
            else:
                updated_result_df = self.session.createDataFrame(result_df)
            # logging.info(f"result_df : {result_df}")
            # updated_result_df = self.session.createDataFrame(result_df)
        except Exception as e:
            logging.error(f"An exception occurred in the data_profiling function: {e}")
            raise e
        return updated_result_df

    def numeric_data_profiling(self, df_snowpark, Data_Profiling_config, total_count):
        """
        Perform numerical data profiling on the DataFrame.

        Args:
            df_snowpark: Snowpark DataFrame.
            Data_Profiling_config: Configuration for data profiling.

        Returns:
            json_profiling: JSON containing numerical profiling information.
        """
        input_col_names = Data_Profiling_config["input_col_names"]
        summary_data_numeric = []

        for col in input_col_names:
            dtype = next(
                col_type[1] for col_type in df_snowpark.dtypes if col_type[0] == col
            )
            if dtype in ["timestamp", "date", "boolean", "variant"]:
                continue

            try:
                mean_val = df_snowpark.agg(
                    snowpark.functions.mean(df_snowpark[col])
                ).collect()[0][0]
                null_count = df_snowpark.agg(
                    snowpark.functions.sum(
                        snowpark.functions.when(df_snowpark[col].isNull(), 1).otherwise(
                            0
                        )
                    )
                ).collect()[0][0]
                maximum = df_snowpark.agg(
                    snowpark.functions.max(df_snowpark[col])
                ).collect()[0][0]
                minimum = df_snowpark.agg(
                    snowpark.functions.min(df_snowpark[col])
                ).collect()[0][0]
                std_dev = df_snowpark.agg(
                    snowpark.functions.stddev(df_snowpark[col])
                ).collect()[0][0]
                total_sum = df_snowpark.agg(
                    snowpark.functions.sum(df_snowpark[col])
                ).collect()[0][0]
                distinct_count = df_snowpark.agg(
                    snowpark.functions.approx_count_distinct(df_snowpark[col])
                ).collect()[0][0]

                summary_data_numeric.append(
                    {
                        "column": col,
                        "mean": mean_val,
                        "null_count": null_count,
                        "maximum": maximum,
                        "minimum": minimum,
                        "std_dev": std_dev,
                        "total_sum": total_sum,
                        "distinct_value_count": distinct_count,
                        "total_row_count": total_count,
                        "data_type": "numeric",
                    }
                )

            except Exception:
                pass

        return summary_data_numeric

    def categorical_data_profiling(self, df_snowpark, Data_Profiling_config):
        """
        Perform categorical data profiling on the DataFrame.

        Args:
            df_snowpark: Snowpark DataFrame.
            Data_Profiling_config: Configuration for data profiling.

        Returns:
            json_profiling: JSON containing categorical profiling information.
        """
        input_col_names = Data_Profiling_config["input_col_names"]
        summary_data_categorical = []

        for col in input_col_names:
            dtype = next(
                col_type[1] for col_type in df_snowpark.dtypes if col_type[0] == col
            )
            if dtype in ["timestamp", "date", "boolean", "variant"]:
                continue

            try:
                if col not in Data_Profiling_config.get("exclude_from_categorical", []):
                    null_count = df_snowpark.agg(
                        snowpark.functions.sum(
                            snowpark.functions.when(
                                df_snowpark[col].isNull(), 1
                            ).otherwise(0)
                        )
                    ).collect()[0][0]
                    logging.info(null_count)
                    black_count = df_snowpark.filter(df_snowpark[col] == "").count()
                    logging.info(black_count)
                    categorical_profile = df_snowpark.groupBy(col).count().collect()
                    categorical_data = [
                        {"value": row[0], "count": row[1]}
                        for row in categorical_profile
                    ]

                    summary_data_categorical.append(
                        {"column_name": col, "column_profile": categorical_data}
                    )

            except Exception:
                pass

        return summary_data_categorical

    def quantile_with_iqr(self, input_df, outlier_config, col_name):
        """
        Calculates the lower and upper outliers based on the specified configuration.

        Parameters:
        - df: Input DataFrame.
        - config: Configuration for outlier calculation.

        Returns:
        - Tuple containing lower_outliers, upper_outliers, iqr, outlier_threshold_min, max_threshold.
        """
        try:
            column_names = input_df.columns
            logging.info("Column name in percentile_calculation: %s", column_names)
            outlier_threshold_min = outlier_config["function_parameters"]["parameters"]["outlier_threshold_min"]
            outlier_threshold_max = outlier_config["function_parameters"]["parameters"]["outlier_threshold_max"]

            if outlier_threshold_min is None:
                outlier_threshold_min = 25
            if outlier_threshold_max is None:
                outlier_threshold_max = 75
            if outlier_threshold_min is None and outlier_threshold_max is None:
                outlier_threshold_min, outlier_threshold_max = 25, 75

            # Convert Spark DataFrame to Pandas DataFrame
            input_df = input_df.toPandas()

            # Sort the DataFrame by the specified column
            if col_name not in input_df.columns:
                raise ValueError(f"Column '{col_name}' not found in the DataFrame.")

            input_df.sort_values(by=col_name, inplace=True)
            input_df.dropna(subset=[col_name], inplace=True)

            # Reset the index after sorting
            input_df = input_df.reset_index(drop=True)
            # Perform IQR method to find outliers on the last 30 days of data
            q1 = input_df[col_name].quantile(outlier_threshold_min / 100)
            q3 = input_df[col_name].quantile(outlier_threshold_max / 100)
            iqr = q3 - q1

            lower_outliers = q1 - 1.5 * iqr
            upper_outliers = q3 + 1.5 * iqr
            return lower_outliers, upper_outliers, iqr, outlier_threshold_min, outlier_threshold_max
        except KeyError as ke:
            logging.error(f"KeyError in percentile_calculation: {str(ke)}")
            raise
        except ValueError as ve:
            logging.error(f"ValueError in percentile_calculation: {str(ve)}")
            raise
        except Exception as e:
            logging.error(f"An error occurred in percentile_calculation: {str(e)}")
            raise

    def quantile_calculation(self, input_df, outlier_config, col_name):
        """
        Calculates the lower and upper outliers based on the specified configuration.

        Parameters:
        - df: Input DataFrame.
        - config: Configuration for outlier calculation.

        Returns:
        - Tuple containing lower_outliers, upper_outliers, iqr, outlier_threshold_min, max_threshold.
        """
        try:
            column_names = input_df.columns
            logging.info("Column name in percentile_calculation: %s", column_names)
            outlier_threshold_min = outlier_config["function_parameters"]["parameters"]["outlier_threshold_min"]
            outlier_threshold_max = outlier_config["function_parameters"]["parameters"]["outlier_threshold_max"]

            if outlier_threshold_min is None:
                outlier_threshold_min = 25
            if outlier_threshold_max is None:
                outlier_threshold_max = 75
            if outlier_threshold_min is None and outlier_threshold_max is None:
                outlier_threshold_min, outlier_threshold_max = 25, 75

            # Convert Spark DataFrame to Pandas DataFrame
            input_df = input_df.toPandas()

            # Sort the DataFrame by the specified column
            if col_name not in input_df.columns:
                raise ValueError(f"Column '{col_name}' not found in the DataFrame.")

            input_df.sort_values(by=col_name, inplace=True)
            input_df.dropna(subset=[col_name], inplace=True)

            # Reset the index after sorting
            input_df = input_df.reset_index(drop=True)
            # Perform IQR method to find outliers on the last 30 days of data
            lower_outliers = input_df[col_name].quantile(outlier_threshold_min / 100)
            upper_outliers = input_df[col_name].quantile(outlier_threshold_max / 100)
            # iqr = q3 - q1

            # lower_outliers = q1 < outlier_threshold_min
            # upper_outliers = q3 > outlier_threshold_max
            return lower_outliers, upper_outliers, outlier_threshold_min, outlier_threshold_max
        except KeyError as ke:
            logging.error(f"KeyError in percentile_calculation: {str(ke)}")
            raise
        except ValueError as ve:
            logging.error(f"ValueError in percentile_calculation: {str(ve)}")
            raise
        except Exception as e:
            logging.error(f"An error occurred in percentile_calculation: {str(e)}")
            raise

    def check_outlier(self, df_to_calculate_outliers, input_df, config, batch_id):
        """
        Performs outlier checks on the input DataFrame based on the specified configuration.

        Parameters:
        - input_df: Input DataFrame.
        - config: Configuration for outlier checks.

        Returns:
        - DataFrame with outlier information.
        """
        try:
            batch_df = input_df.filter(input_df["BATCH_ID"] == batch_id)
            batch_df = batch_df.toPandas()
            batch_df = batch_df.reset_index(drop=True)
            row_level_description_list = []
            is_outlier_temp = False
            for col_name in config["input_col_name"]:
                if config["function_name"] == 'quantile':
                    lower_outliers, upper_outliers, min_threshold, max_threshold = self.quantile_calculation(df_to_calculate_outliers, config, col_name)
                    logging.info(f"Lower outliers values: {lower_outliers}, Upper outliers values: {upper_outliers}, Min threshold: {min_threshold}, Max threshold: {max_threshold}")
                    outliers = batch_df[(batch_df[col_name] < lower_outliers) | (batch_df[col_name] > upper_outliers)]
                    batch_df['outlier'] = 'No'
                    batch_df.loc[outliers.index, 'outlier'] = 'Yes'
                    outlier_counter = len(batch_df["outlier"].loc[lambda x: x == 'Yes'].tolist())
                    description = {
                        "function_name": config["function_name"],
                        "Column": col_name,
                        "Min_Threshold_Percentage": "{:.2f}".format(min_threshold),
                        "Max_Threshold_Percentage": "{:.2f}".format(max_threshold),
                        "Lower_Bound_Value": "{:.2f}".format(lower_outliers),
                        "Upper_Bound_Value": "{:.2f}".format(upper_outliers),
                        "Outlier_Counts": outlier_counter,
                        "data_outlier": (outlier_counter > 0),
                    }
                    if outlier_counter > 0:
                        is_outlier_temp = True
                    row_level_description_list.append(description)
                elif config["function_name"] == 'quantile_with_iqr':
                    lower_outliers, upper_outliers, iqr, min_threshold, max_threshold = self.quantile_with_iqr(df_to_calculate_outliers, config, col_name)
                    logging.info(f"IQR: {iqr}, Lower outliers values: {lower_outliers}, Upper outliers values: {upper_outliers}, Min threshold: {min_threshold}, Max threshold: {max_threshold}")
                    outliers = batch_df[(batch_df[col_name] < lower_outliers) | (batch_df[col_name] > upper_outliers)]
                    batch_df['outlier'] = 'No'
                    batch_df.loc[outliers.index, 'outlier'] = 'Yes'
                    outlier_counter = len(batch_df["outlier"].loc[lambda x: x == 'Yes'].tolist())
                    description = {
                        "function_name": config["function_name"],
                        "Column": col_name,
                        "Min_Threshold_Percentage": "{:.2f}".format(min_threshold),
                        "Max_Threshold_Percentage": "{:.2f}".format(max_threshold),
                        "Lower_Bound_Value": "{:.2f}".format(lower_outliers),
                        "Upper_Bound_Value": "{:.2f}".format(upper_outliers),
                        "Outlier_Counts": outlier_counter,
                        "data_outlier": (outlier_counter > 0),
                    }
                    if outlier_counter > 0:
                        is_outlier_temp = True
                    row_level_description_list.append(description)
            return is_outlier_temp, row_level_description_list
        except KeyError as ke:
            logging.error(f"KeyError in check_outlier: {str(ke)}")
            raise
        except (ValueError, TypeError) as e:
            logging.error(f"ValueError or TypeError occurred in check_outlier: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"An error occurred in check_outlier: {str(e)}")
            raise

    @staticmethod
    def is_valid_zip5(zip_code):
        try:
            if pd.isna(zip_code) or zip_code is None:
                return True

            if isinstance(
                zip_code,
                (
                    int,
                    float,
                    np.int16,
                    np.int32,
                    np.int64,
                    np.float16,
                    np.float32,
                    np.float64,
                ),
            ):
                zip_code = str(int(zip_code))

            return len(zip_code) != 5 or not zipcodes.is_real(zip_code)

        except Exception:
            return True

    def valid_zipcode_check(self, df, zipcode_config, custom_index_values):
        """
        'valid_zipcode_check' method to identify and handle invalid zipcodes
        from the input dataset.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for invalid zipcodes.
            zipcode_config (dict): Configuration details for duplicate checks
            custom_index_values(dict): which stores indexes of the dataframe
                for creating ok_df and error_df.
        Returns:
        dataframe after adding '_INVALID' extension for respective columns
        with their boolean values.
        custom_index_values
        """

        input_col_names = zipcode_config["input_col_names"]
        action = zipcode_config["action"]

        for column in input_col_names:
            zipcode_column = column + "_INVALID"
            unique_values = df[column].unique()
            validation_results = {
                zip_code: self.is_valid_zip5(zip_code) for zip_code in unique_values
            }
            df[zipcode_column] = df[column].map(validation_results)

            deleted_rows = df[(df[zipcode_column])]
            custom_index_values["error_rows_index_set"].update(deleted_rows.index)

            if action == "drop":
                custom_index_values["drop_rows_index_set"].update(deleted_rows.index)

        return df, custom_index_values

    def table_column_value_comparison(self, df, values_consistency_config, custom_index_values, read_data_set_config):
        """
        'table_column_value_comparison' method to identify to handle mismatch of values of column
        with their respective target table column values.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for dataconsistency validation
            values_consistency_config (dict): Configuration details for dataconsistency validation
            custom_index_values(dict): which stores indexes of the dataframe
                for creating ok_df and error_df.
            read_data_set_config: input dataset config information
        Returns:
        dataframe after adding Dataconsistency column with thier respective checks info (type dict)
        custom_index_values
        """
        dataframe = df.copy(deep=True)
        source_column_name = values_consistency_config["source_column_name"]
        target_column_mappings = values_consistency_config["target_column_mappings"]
        source_key = values_consistency_config["source_key"]

        dataframeslist = []  # append dataframes list
        target_tables = []  # append target table names
        target_table_columns = []  # append target columns
        actions = []
        data_consistency_list = []  # append the dataconsistency values list
        data_consistency_columns = []  # append dataconsistency keys and values
        error_rows_index_set = set()
        input_data_db = read_data_set_config["input_data_db"]
        input_data_schema = read_data_set_config["input_data_schema"]
        input_dataset = read_data_set_config["input_data_source"]
        input_table_name = f"{input_data_db}.{input_data_schema}.{input_dataset}"

        try:
            for tar_col in range(0, len(target_column_mappings)):
                tar_table_name = target_column_mappings[tar_col]["target_table_name"]
                tar_col_name = target_column_mappings[tar_col]["target_column_name"]
                action = target_column_mappings[tar_col]["action"]

                target_key = values_consistency_config["source_key"]
                try:
                    if target_column_mappings[tar_col]["target_table_key"]:
                        target_key = target_column_mappings[tar_col]["target_table_key"]
                except Exception:
                    pass

                logging.info(target_key)
                tar_table_column = list(target_key)
                tar_table_column.append(tar_col_name)
                dataframe_i = self.session.table(tar_table_name)
                alter_dataframe = dataframe_i.to_pandas()
                alter_dataframe = alter_dataframe[tar_table_column]
                alter_dataframe = alter_dataframe.drop_duplicates()
                dataframeslist.append(alter_dataframe)
                target_tables.append(tar_table_name)
                target_table_columns.append(tar_col_name)
                actions.append(action)

        except Exception as err:
            logging.error(
                f"quality check validation failed during table_consistency check with err:{err}"
            )
            raise err

        try:
            for i in range(0, len(dataframeslist)):

                dataframe = dataframe.merge(
                    dataframeslist[i],
                    how="left",
                    left_on=source_key,
                    right_on=target_key,
                    left_index=False,
                    right_index=False,
                    suffixes=("", f"_TARGET_{i}"),
                )
                logging.info(dataframe.columns)
                if target_table_columns[i] != source_column_name:
                    mask = ~(
                        dataframe[source_column_name].isna()
                        & dataframe[target_table_columns[i]].isna()
                    )
                    dataframe["mismatch"] = (
                        ~(
                            dataframe[source_column_name]
                            == dataframe[target_table_columns[i]]
                        )
                        & mask
                    )
                    deleted_rows = dataframe[dataframe["mismatch"]]

                    error_rows_index_set.update(deleted_rows.index)

                    if action == "drop":
                        custom_index_values["drop_rows_index_set"].update(
                            deleted_rows.index)

                    data_consistency_list.append(
                        dataframe.apply(
                            lambda row: {
                                "target_table_name": f"{target_tables[i]}",
                                "target_table_col": target_table_columns[i],
                                "target_column_value": row[target_table_columns[i]],
                                "source_column": source_column_name,
                                "source_column_value": row[source_column_name],
                                "mismatch": row["mismatch"],
                            },
                            axis=1,
                        )
                    )

                    dataframe = dataframe.drop(["mismatch"], axis=1)

                else:
                    mask = ~(
                        dataframe[source_column_name].isna()
                        & dataframe[f"{source_column_name}_TARGET_{i}"].isna()
                    )
                    dataframe[f"mismatch_{i}"] = (
                        ~(
                            dataframe[source_column_name]
                            == dataframe[f"{source_column_name}_TARGET_{i}"]
                        )
                        & mask
                    )
                    deleted_rows = dataframe[dataframe[f"mismatch_{i}"]]
                    error_rows_index_set.update(deleted_rows.index)

                    if action == "drop":
                        custom_index_values["drop_rows_index_set"].update(
                            deleted_rows.index)

                    target_column = f"{source_column_name}_TARGET_{i}".replace(
                        f"_TARGET_{i}", ""
                    )

                    data_consistency_list.append(
                        dataframe.apply(
                            lambda row: {
                                "target_table_name": f"{target_tables[i]}",
                                "target_table_col": target_column,
                                "target_column_value": row[
                                    f"{source_column_name}_TARGET_{i}"
                                ],
                                "source_column": source_column_name,
                                "source_column_value": row[source_column_name],
                                "mismatch": row[f"mismatch_{i}"],
                            },
                            axis=1,
                        )
                    )

                    dataframe = dataframe.drop([f"mismatch_{i}"], axis=1)

                i = i + 1
                column_name = f"CHECK_{i}"
                data_consistency_columns.append(column_name)

            data_consistency_df = pd.concat(data_consistency_list, axis=1)
            data_consistency_df.columns = data_consistency_columns

            dataframe["DATA_INCONSISTENCY"] = data_consistency_df.to_dict(orient="records")
            error_rows_index_list = list(error_rows_index_set)

            cons_error_df = dataframe.loc[error_rows_index_list].reset_index(drop=True)

            cons_error_df["DATA_INCONSISTENCY"] = cons_error_df["DATA_INCONSISTENCY"].apply(
                lambda value: json.dumps(value)
            )

            error_exclude_column = ["DATA_INCONSISTENCY", "BATCH_ID"]

            cons_error_df["INPUT_RECORDS"] = cons_error_df.apply(
                lambda row: json.dumps(
                    {
                        col1: row[col1]
                        for col1 in cons_error_df.columns
                        if col1 not in error_exclude_column
                    },
                    default=str,
                ),
                axis=1,
            )

            columns_to_keep = ["BATCH_ID", "INPUT_RECORDS", "DATA_INCONSISTENCY"]

            cons_error_df = cons_error_df[columns_to_keep]

            dataframe = self.session.createDataFrame(cons_error_df)

            dataframe = dataframe.withColumn(
                "INPUT_RECORDS", parse_json("INPUT_RECORDS").cast(VariantType())
            )
            dataframe = dataframe.withColumn(
                "DATA_INCONSISTENCY", parse_json("DATA_INCONSISTENCY").cast(VariantType())
            )

            dataframe = dataframe.withColumn("INPUT_DATA_SOURCE", lit(input_table_name))

        except Exception as err:
            logging.error(
                "quality check validation failed during table_consistency check, during operations, with err:{err}"
            )
            raise err

        return dataframe, custom_index_values

    def validate_consistency_check(self, df, text_to_id_config, custom_index_values):
        """
        'validate_consistency_check' method to identify to handle mismatch of text values
        with their respective id values.
        Parameters:
            df (pd.DataFrame): The DataFrame to be checked for mismatch of text and
                                with their respective ids.
            text_to_id_config (dict): Configuration details for duplicate checks
            custom_index_values(dict): which stores indexes of the dataframe
                for creating ok_df and error_df.
        Returns:
        dataframe after adding 'col1_AND_col2_MISMATCH' column to dataframe
        with their boolean values.
        custom_index_values
        """
        text_and_id_column = text_to_id_config['text_and_id_columns']
        try:
            for text_id in text_and_id_column:
                text_column_name = text_id["text_column_name"]
                id_column_name = text_id["id_column_name"]
                action = text_id["action"]
                data_mapping = text_id["data_mapping"]

                # adding _MISMATCH extension name column to the dataframe
                column = text_column_name + "_AND_SCORE_MISMATCH"
                logging.info("text to id validation started")

                mask = ~(
                    df[text_column_name].isna()
                    & df[id_column_name].isna()
                )

                df[column] = (~df.apply(lambda row: data_mapping.get(row[text_column_name]) == row[id_column_name], axis=1) & mask)
                deleted_rows = df[(df[column])]
                logging.info(deleted_rows)
                custom_index_values["error_rows_index_set"].update(deleted_rows.index)
                logging.info(custom_index_values)

                if action == "drop":
                    custom_index_values["drop_rows_index_set"].update(deleted_rows.index)

        except Exception as err:
            logging.error(f"quality check validation failed during text and id check validation with err:{err}")
            raise err

        return df, custom_index_values

    def apply_data_quality_checks(self, df, custom_index_values, read_dataset_config):
        """
        'apply_data_quality_checks' method acts as a main method,
        which triggers all the specified checks
        Parameters:
            df (pd.DataFrame): input dataframe from the source
            for applying specified checks.
        Returns:
        returns two dataframes after apply checks one with correct
        data and another with incorrect data.
        """
        data_profiling_df = None
        inconsistency_df = None
        try:
            quality_check_functions = {
                "is_duplicate": self.duplicate_check,
                "valid_zipcode": self.valid_zipcode_check,
                "check_date_format": self.check_date_format,
                "is_valid_numeric": self.process_numeric_data,
                "profiling": self.data_profiling,
                "table_column_value_comparison": self.table_column_value_comparison,
                "validate_question_ids": self.validate_consistency_check
            }

            for quality_check in self.data_quality_checks:
                function_name = quality_check["function_name"]

                if df is None or len(df) == 0:
                    logging.warning(
                        f"No Data availabe in input dataset while performing"
                        f"{function_name} quality check"
                    )
                    raise Exception("No Data available for performing quality checks")

                quality_check_function = quality_check_functions.get(function_name)

                if quality_check_function:
                    if function_name == "profiling":
                        data_profiling_df = quality_check_function(
                            df, quality_check, read_dataset_config
                        )
                    elif function_name == "table_column_value_comparison":
                        inconsistency_df, custom_index_values = quality_check_function(
                            df, quality_check, custom_index_values, read_dataset_config)
                    else:
                        quality_check_function(df, quality_check, custom_index_values)

        except Exception as e:
            logging.error(f"Error occurred in apply_data_quality_checks: {e}")
            raise e

        return df, custom_index_values, data_profiling_df, inconsistency_df
