from snowflake.snowpark.functions import parse_json, lit, current_timestamp
from snowflake.snowpark.types import VariantType
from .custom_data_quality_checker import CustomDataQualityChecker
from .gx_helper import GXHelper
from ..utils.custom_config_parser import CustomConfigParser
import logging
import json
from snowflake.snowpark.types import (
    StructType,
    StructField,
    TimestampType,
)


def parse_results(
    orginal_df, df_after_checks,
    ge_results,
    custom_index_values,
    ge_index_values
):
    """
    'parse_results' function is used for separating the dataframes based
    on quality checks performed

        Parameters:
            orginal_df : original dataframe without applying any checks
            df_after_checks: dataframe after applying checks
            ge_results: results json of ge checks
            custom_index_values: index dictionary of custom checks on input_df
            ge_index_values: ge index dictionary of ge checks on input_df

        Returns:
        returns two dataframes(ok_df, error_df) after modifying
        input dataframe based on the checks provided
    """

    try:
        results_json = ge_results.to_json_dict()
        result_key = next(iter(results_json["run_results"]))
        result_json = results_json["run_results"][result_key][
            "validation_result"]["results"]

        # checks for results that ran using ge
        if result_json:

            expectation_type_to_suffix = {
                "expect_column_values_to_not_be_null": "_IS_NULL",
                "expect_column_values_to_be_between": "_NOT_IN_RANGE",
                "expect_column_values_to_be_in_set": "_NOT_IN_SET",
                "expect_column_values_to_match_regex": "_INVALID_REGEX",
                "expect_column_values_to_match_json_schema": "_INVALID_JSON",
            }

            for entry in result_json:
                expectation_type = entry["expectation_config"][
                    "expectation_type"]
                column = entry["expectation_config"]["kwargs"][
                    "column"
                ] + expectation_type_to_suffix.get(expectation_type, "_INVALID")

                unexpected_indices = entry["result"]["unexpected_index_list"]
                df_after_checks.loc[unexpected_indices, column] = True
                df_after_checks[column].fillna(False, inplace=True)

                ge_index_values["error_rows_index_set"].update(
                    unexpected_indices)

        error_rows_index_set = custom_index_values[
            "error_rows_index_set"].union(
            ge_index_values["error_rows_index_set"]
        )

        drop_rows_index_set = custom_index_values["drop_rows_index_set"].union(
            ge_index_values["drop_rows_index_set"]
        )
        error_rows_index_list = list(error_rows_index_set)

        drop_rows_index_list = list(drop_rows_index_set)

        ok_df = orginal_df.drop(drop_rows_index_list).reset_index(drop=True)

        exclude_columns = orginal_df.columns

        orginal_df["QUALITY_CHECK_OUTPUT"] = df_after_checks.apply(
            lambda row: json.dumps(
                {
                    col: row[col]
                    for col in df_after_checks.columns
                    if col not in exclude_columns
                }
            ),
            axis=1,
        )

        # adding action column to the dataframe based on indices
        orginal_df["ROW_ACTION"] = "logged"
        orginal_df["ROW_ACTION"].loc[drop_rows_index_list] = "dropped"

        error_df = orginal_df.loc[error_rows_index_list].reset_index(drop=True)

        error_exclude_column = [
            "BATCH_ID", "QUALITY_CHECK_OUTPUT", "ROW_ACTION"
        ]
        error_df["INPUT_RECORDS"] = error_df.apply(
            lambda row: json.dumps(
                {
                    col: row[col]
                    for col in error_df.columns
                    if col not in error_exclude_column
                },
                default=str
            ),
            axis=1,
        )

        columns_to_keep = ["BATCH_ID",
                           "INPUT_RECORDS",
                           "QUALITY_CHECK_OUTPUT",
                           "ROW_ACTION"
                           ]

        error_df = error_df[columns_to_keep]

    except Exception as e:
        logging.error("Error parsing checkpoint results: " + str(e))
        raise e

    return ok_df, error_df


def quality_checks_intializer(session, df, data_quality_checks, read_dataset_config, write_dataset_config):
    """
    'quality_checks_intializer' functions acts as a main method,
        which triggers all the specified checks, mentioned in the config file
        using custom quality checks and ge quality checks

        Parameters:
            session : snowpark session for connecting to snowflake
            df: input pandas dataframe
            data_quality_checks: quality checks functions json

        Returns:
        returns two dataframes(ok_df, error_df) after modifying
        input dataframe based on the checks provided
    """

    orginal_df = df.copy(deep=True)
    validation_df = df.copy(deep=True)

    custom_index_values = {
        "error_rows_index_set": set(),
        "drop_rows_index_set": set()
    }

    ge_index_values = {
        "error_rows_index_set": set(),
        "drop_rows_index_set": set()
    }

    try:
        pd_checks_class = CustomDataQualityChecker(
            session, data_quality_checks
        )

        (
            df_after_checks,
            custom_index_values,
            profiling_df,
            inconsistency_df
        ) = pd_checks_class.apply_data_quality_checks(
            validation_df, custom_index_values, read_dataset_config
        )

        logging.info(df_after_checks)
    except Exception as e:
        logging.error("Error in applying pandas Data quality checks")
        raise e

    try:
        gx_class = GXHelper()

        validator = gx_class.validator_from_df(df_after_checks)

        (final_validator, ge_index_values) = gx_class.run_expectations(
            data_quality_checks, validator, session, ge_index_values
        )

        ge_results = gx_class.run_checkpoint(final_validator)

    except Exception as e:
        logging.error("Error in applying GE Data quality checks")
        raise e

    try:
        ok_df, error_df = parse_results(
            orginal_df,
            df_after_checks,
            ge_results,
            custom_index_values,
            ge_index_values,
        )
        ok_df = session.createDataFrame(ok_df)
        error_df = session.createDataFrame(error_df)

        # target table
        output_data_db = write_dataset_config['output_data_db']
        output_data_schema = write_dataset_config['output_data_schema']
        output_data_table = write_dataset_config['output_dataset']
        table_name = f"{output_data_db}.{output_data_schema}.{output_data_table}"

        # source table
        input_data_db = read_dataset_config['input_data_db']
        input_data_schema = read_dataset_config['input_data_schema']
        input_data_table = read_dataset_config['input_data_source']
        input_table_name = f"{input_data_db}.{input_data_schema}.{input_data_table}"

        # error table
        error_data_table = write_dataset_config['error_table']
        # error_data_table_mode = write_dataset_config['error_table_write_mode']
        error_table_name = f"{output_data_db}.{output_data_schema}.{error_data_table}"

        error_df = error_df.withColumn("INSERT_TIMESTAMP", current_timestamp())
        error_df = error_df.withColumn("INPUT_DATA_SOURCE", lit(input_table_name))
        error_df = error_df.withColumn("OUTPUT_TABLE", lit(table_name))
        error_df = error_df.withColumn("ERROR_TABLE", lit(error_table_name))
        error_df = error_df.withColumn("UPDATE_TIMESTAMP", lit(None))
        error_df = error_df.withColumn("STATUS", lit(None))
        error_df = error_df.withColumn("TRACKING_TICKET", lit(None))
        error_df = error_df.withColumn("INPUT_RECORDS", parse_json("INPUT_RECORDS").cast(VariantType()))
        error_df = error_df.withColumn("QUALITY_CHECK_OUTPUT", parse_json("QUALITY_CHECK_OUTPUT").cast(VariantType()))

        if profiling_df:
            profiling_df = profiling_df.withColumn("UPDATE_TIMESTAMP", lit(None))
            profiling_df = profiling_df.withColumn("STATUS", lit(None))
            profiling_df = profiling_df.withColumn("TRACKING_TICKET", lit(None))

    except Exception as e:
        logging.error("Error in parsing results of the quality checks to dataframes")
        raise e

    return ok_df, error_df, profiling_df, inconsistency_df


# input data_source reading
def input_dataframe(session, input_config_data):
    try:
        database = input_config_data["input_data_db"]
        schema = input_config_data["input_data_schema"]
        table_name = input_config_data["input_data_source"]
        read_input_table = f"{database}.{schema}.{table_name}"
        result = session.sql(f"select * from {read_input_table}")
        dataframe = result.collect()  # Get a list of Row objects
        df = dataframe.to_pandas()

    except Exception as e:
        logging.error(f"Error occured while reading input datasoure {e}")
        raise e

    return df


# reading config files and returning paths
def read_configuration_files(session, path):
    try:

        input_config_file = path

        # reading config file
        config_data = CustomConfigParser(session, input_config_file)
        for i in range(
            len(config_data["meta_data"]["execution_order_default"])
        ):  # noqa
            dataset_name = config_data["meta_data"]["execution_order_default"][i]

        input_config_data = config_data["datasets"][dataset_name]["input_data"]
        logging.info(input_config_data)

        output_config_data = config_data["datasets"][dataset_name]["output_data"]
        logging.info(output_config_data)

        # Read the data_quality_config configuration file
        quality_check_input_config = output_config_data["data_quality"][
            "quality_check_input_config"
        ]

        quality_checks_data = CustomConfigParser(session, quality_check_input_config)

        logging.info(quality_checks_data)

    except Exception as e:
        logging.error(f"Error occurred while reading the " f"configuration files: {e}")
        raise e

    return input_config_data, output_config_data, quality_checks_data


# apply data quality checks performed from the config file itself
def data_quality_checks_from_config(session, path):

    try:
        config_path = path
        (
            input_config_data,
            output_config_data,
            quality_checks_data,
        ) = read_configuration_files(
            session, config_path
        )
        data_quality_checks = quality_checks_data["data_quality_checks"]
    except Exception as e:
        logging.error(f"Error occurred while reading the " f"configuration files: {e}")
        raise e

    try:
        df = input_dataframe(session, input_config_data)
    except Exception as e:
        logging.error(f"Error with config stage file:{e}")
        raise e

    try:
        ok_df, error_df, profiling_df = quality_checks_intializer(session, df, data_quality_checks)
    except Exception as e:
        logging.error("Error running the expectations on the dataframe")
        raise e

    try:

        for col in ok_df.columns:
            col_dtype = df[col].dtype
            if col_dtype in ["datetime64[ns]", "timedelta64[ns]", "datetime64[ns, tz]"]:
                df[col] = df[col].dt.tz_localize("UTC")
                keep_schema = StructType([StructField(col, TimestampType())])

        ok_df = session.create_dataframe(ok_df, schema=keep_schema)

        error_df = session.create_dataframe(error_df, schema=keep_schema)

        database = output_config_data["output_data_db"]
        schema = output_config_data["output_data_schema"]
        table_name = output_config_data["output_table"]

        output_table_write_mode = output_config_data["output_table_write_mode"]
        error_table_write_mode = output_config_data["error_table_write_mode"]

        error_table_name = output_config_data["error_table"]

        profiling_table_write_mode = output_config_data["mode"]
        profiling_table = output_config_data["master_profiling_table"]

        print(profiling_table)
        profiling_df.write.mode(profiling_table_write_mode).saveAsTable(profiling_table)
        # ok_data insertion to snowflake database
        ok_output_table = f"{database}.{schema}.{table_name}"
        ok_df.write.mode(output_table_write_mode).saveAsTable(ok_output_table)

        # error_data insertion to snowflake database
        error_output_table = f"{database}.{schema}.{error_table_name}"
        error_df.write.mode(error_table_write_mode).saveAsTable(
            error_output_table)

    except Exception as e:
        logging.error("Error loading load into snowflake on the dataframe")
        raise e

    return ok_df, error_df, profiling_df
