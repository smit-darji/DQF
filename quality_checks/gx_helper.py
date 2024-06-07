"""
The 'GXHelper' class is used for intializing the Great_Expectations library\
reading the input datasource, forming validator, running expectations, \
running checkpoint for generating results and parsig_results.

Returns:
    returns two dataframes one with correct data and another with error data
"""

import great_expectations as ge
import logging
from .gx_data_quality_checker import GXDataQualityChecker
import json


class GXHelper:
    def __init__(self):
        self.context = ge.get_context()

    # Creating a validator from a DataFrame
    def validator_from_df(self, df):
        self.df = df
        try:
            validator = self.context.sources.pandas_default.read_dataframe(self.df) # noqa
        except Exception as e:
            logging.error("Error creating validator from DataFrame:" + str(e))
            raise e
        return validator

    # Creating a validator from a CSV file
    def validator_from_csv(self, csv):
        try:
            validator = self.context.sources.pandas_default.read_csv(csv)
        except Exception as e:
            logging.error("Error creating validator from CSV file: " + str(e))
            raise e
        return validator

    # Running exceptions on the validator
    def run_expectations(self, data_quality_checks, validator, session, drop_rows_index_set):

        try:
            ge_quality_class = GXDataQualityChecker(
                data_quality_checks, validator, session
            )
            final_validator, drop_rows_index_list = ge_quality_class.apply_data_quality_checks(drop_rows_index_set)
        except Exception as e:
            logging.error(f"Error running exceptions on the validator:"
                          f"{str(e)}")
            raise e

        return final_validator, drop_rows_index_list

    # Running checkpoint on validations and return results
    def run_checkpoint(self, final_validator):
        # self.validator = validator
        try:
            checkpoint = self.context.add_or_update_checkpoint(
                name="data_quality_checks",
                validator=final_validator,
                runtime_configuration={
                    "result_format": {
                        "result_format": "COMPLETE"
                    }
                },
            )
            results = checkpoint.run()
        except Exception as e:
            logging.error("Error running checkpoint: " + str(e))
            raise e
        return results

    # Parsing checkpoint results to endpoint
    def parse_results(self, orginal_df, df_after_checks, results, error_rows_index_set, drop_rows_index_set):

        try:
            results_json = results.to_json_dict()
            result_key = next(iter(results_json["run_results"]))
            result_json = results_json["run_results"][result_key][
                "validation_result"]["results"]

            # if the exceptions ran on using great_expectations greater than 1
            if result_json:

                expectation_type_to_suffix = {
                    "expect_column_values_to_not_be_null": "_IS_NULL",
                    "expect_column_values_to_be_between": "_NOT_IN_RANGE",
                    "expect_column_values_to_be_in_set": "_NOT_IN_SET",
                    "expect_column_values_to_match_regex": "_INVALID_REGEX",
                    "expect_column_values_to_match_json_schema": "_INVALID_JSON"
                }

                for entry in result_json:
                    expectation_type = entry['expectation_config'][
                        'expectation_type']
                    column = entry['expectation_config']['kwargs'][
                        'column'] + expectation_type_to_suffix.get(
                            expectation_type, "_INVALID")

                    unexpected_indices = entry["result"]["unexpected_index_list"]
                    df_after_checks.loc[unexpected_indices, column] = True
                    df_after_checks[column].fillna(False, inplace=True)

                    error_rows_index_set.update(unexpected_indices)

            error_rows_index_list = list(error_rows_index_set)

            drop_rows_index_list = list(drop_rows_index_set)

            ok_df = orginal_df.drop(drop_rows_index_list).reset_index(
                drop=True)

            exclude_columns = orginal_df.columns

            # print(exclude_columns)

            orginal_df['QUALITY_CHECK_REPORT'] = df_after_checks.apply(lambda row: json.dumps(
                {col: row[
                    col] for col in df_after_checks.columns if col not in exclude_columns}),
                      axis=1)
            error_exclude_column = ['BATCH_ID']
            error_df = orginal_df.loc[error_rows_index_list].reset_index(
                drop=True)
            error_df['ORIGINAL_DATA'] = df_after_checks.apply(lambda row: json.dumps(
                {col: row[
                    col] for col in df_after_checks.columns if col not in error_exclude_column}),
                      axis=1)

        except Exception as e:
            logging.error("Error parsing checkpoint results: " + str(e))
            raise e

        return ok_df, error_df


# acts as a main function for apply quality checks on the data set
def gx_intializer(session, df_after_checks, data_quality_checks, orginal_df, pd_error_list_index, drop_rows_index_set):

    try:
        gx_class = GXHelper()
        validator = gx_class.validator_from_df(df_after_checks)
        validator, drop_set_records_index_list = gx_class.run_expectations(data_quality_checks, validator, session, drop_rows_index_set)
        results = gx_class.run_checkpoint(validator)
        ok_df, error_df = gx_class.parse_results(orginal_df, df_after_checks, results, pd_error_list_index, drop_set_records_index_list)

    except Exception as e:
        logging.error("Error running the expectations on the dataframe")
        raise e

    return ok_df, error_df
