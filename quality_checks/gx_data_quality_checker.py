"""
The 'GXDataQualityChecker' class performs data quality checks on
a DataFrame using Snowpark's session.
Arguments:
        validator (object): input datasource of GE to perform expectations on \
              Dataset
        data_quality_checks (dict): Configuration details for all the to be \
            perfomed


** method `apply_data_quality_checks`, which triggers all the c
all the specified checks.
Returns:
    validator object, which stores all the expectation results, ready \
    for running a checkpoint.
"""
import json
import logging
from .custom_expectations.expect_column_values_to_be_valid_phonenumber import *
from .custom_expectations.expect_column_values_to_contain_valid_email import *


class GXDataQualityChecker:
    def __init__(self, data_quality_checks, validator, session):
        self.data_quality_checks = data_quality_checks
        self.validator = validator
        self.session = session
     
    # Null check using Great Expectations
    def is_null_check(self, is_null_check_config, ge_index_values):

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
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your is_null_check_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your is_null_check_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your is_null_check_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your is_null_check_config"
                )

        input_col_names = is_null_check_config["input_col_names"]
        action = is_null_check_config["action"]
        try:

            for column in input_col_names:
                logging.info(f"Checking null values for column: {column}")
                null_check_results = self.validator.expect_column_values_to_not_be_null(
                    column=column,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                # checking action of this check and indexes to list
                if action == "drop":
                    null_check_results = null_check_results.to_json_dict()
                    unexpected_indices = null_check_results['result'][
                        'unexpected_index_list']
                    
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)
            
            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in null check: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # Range check using Great Expectations
    def in_range_check(self, in_range_config, ge_index_values):

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
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your in_range_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your in_range_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your in_range_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your in_range_config"
                )

        input_col_names = in_range_config["input_col_names"]
        min_value = in_range_config["min_value"]
        max_value = in_range_config["max_value"]
        action = in_range_config["action"]

        try:
            for i in range(len(input_col_names)):
                logging.info(f"Checking range for column: {input_col_names[i]}")
                range_check_results = self.validator.expect_column_values_to_be_between(
                    column=input_col_names[i],
                    min_value=min_value,
                    max_value=max_value,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

            # checking action of this check and indexes to list
                if action == "drop":
                    range_check_results = range_check_results.to_json_dict()
                    unexpected_indices = range_check_results['result'][
                        'unexpected_index_list']               
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

                    # drop_rows_index_set.update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in range check: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # Duplicate check using Great Expectations
    def is_duplicate_check(self, duplicate_check_config, ge_index_values):

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
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your duplicate_check_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your duplicate_check_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,re-check"
                    f" your duplicate_check_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,re-check"
                    f" your duplicate_check_config"
                )

        input_col_names = duplicate_check_config["input_col_names"]
        action = duplicate_check_config["action"]
        try:

            for column in input_col_names:
                logging.info(f"Checking duplicates for column: {column}")
                duplicate_check_results = self.validator.expect_column_values_to_be_unique(
                    column=column
                )

                if action == "drop":
                    duplicate_check_results = duplicate_check_results.to_json_dict()
                    unexpected_indices = duplicate_check_results['result'][
                        'unexpected_index_list']
                    # drop_rows_index_set.update(unexpected_indices)
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in duplicate check: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # Verify element using Great Expectations
    def verify_element(self, verify_element_config, ge_index_values):

        # for checking keys and values
        for key, value in verify_element_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your verify_element_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your verify_element_config"
                    )
            elif value_type in ["int", "float"] and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your verify_element_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your verify_element_config"
                )

        input_col_names = verify_element_config["input_col_names"]
        values = verify_element_config["value_set"]
        action = verify_element_config["action"]
        try:

            for column in input_col_names:
                logging.info(f"Verifying elements for column: {column}")
                verify_element_checks = self.validator.expect_column_values_to_be_in_set(
                    column=column, value_set=values,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    verify_element_checks = verify_element_checks.to_json_dict()
                    unexpected_indices = verify_element_checks['result'][
                        'unexpected_index_list']
                    # drop_rows_index_set.update(unexpected_indices)
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in verify element: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # valid email check using Great Expectations
    def valid_email_check(self, valid_email_config, ge_index_values):

        # for checking keys and values
        for key, value in valid_email_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_email_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_email_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_email_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_email_config"
                )

        input_col_names = valid_email_config["input_col_names"]
        action = valid_email_config["action"]
        try:

            for column in input_col_names:
                logging.info(f"valid email check for column: {column}")
                email_check_results = self.validator.expect_column_values_to_contain_valid_email(
                    column=column,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    email_check_results = email_check_results.to_json_dict()
                    unexpected_indices = email_check_results['result'][
                        'unexpected_index_list']
                    # drop_rows_index_set.update(unexpected_indices)
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in valid email check function: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # valid phone number check using Great Expectations
    def valid_phonenumber_check(self, valid_phonenumber_config, ge_index_values):
        # for checking keys and values
        for key, value in valid_phonenumber_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_phonenumber_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_phonenumber_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_phonenumber_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_phonenumber_config"
                )

        input_col_names = valid_phonenumber_config["input_col_names"]
        action = valid_phonenumber_config['action']
        try:

            for column in input_col_names:
                logging.info(f"valid phonenumber check for column: {column}")
                phone_number_checks = self.validator.expect_column_values_to_be_valid_phonenumber(
                    column=column,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    phone_number_checks = phone_number_checks.to_json_dict()
                    unexpected_indices = phone_number_checks['result'][
                        'unexpected_index_list']
                    # drop_rows_index_set.update(unexpected_indices)
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in valid phonenumber check function:"
                          f"{str(e)}")
            raise e

        return self.validator, ge_index_values

    # valid zipcode check using Great Expectations
    def valid_zipcode_check(self, valid_zipcode_config, ge_index_values):

        # for checking keys and values
        for key, value in valid_zipcode_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_zipcode_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_zipcode_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_zipcode_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_zipcode_config"
                )

        input_col_names = valid_zipcode_config["input_col_names"]
        action = valid_zipcode_config['action']
        try:
            for column in input_col_names:
                logging.info(f"valid zipcode check for column: {column}")
                zipcode_code_results = self.validator.expect_column_values_to_be_valid_zip5(
                    column=column,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    zipcode_code_results = zipcode_code_results.to_json_dict()
                    unexpected_indices = zipcode_code_results['result'][
                        'unexpected_index_list']
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in valid zipcode check function: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # regex _pattern check using Great Expectations
    def regex_pattern_check(self, regex_pattern_config, ge_index_values):

        # for checking keys and values
        for key, value in regex_pattern_config.items():
            value_type = type(value).__name__
            if value_type in ["str", "list", "NoneType"]:
                if (
                    value is None
                    or (value_type == "str" and len(value) == 0)
                    or (value_type == "list" and len(value) == 0)
                ):
                    logging.warning(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_zipcode_config"
                    )
                    raise Exception(
                        f"Key: {key}, Value: {value} is None or empty,"
                        f"re-check your valid_zipcode_config"
                    )
            elif value_type == "int" and value is None:
                logging.warning(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_zipcode_config"
                )
                raise Exception(
                    f"Key: {key}, Value: {value} is None,"
                    f"re-check your valid_zipcode_config"
                )

        input_col_names = regex_pattern_config["input_col_names"]
        regex_pattern = regex_pattern_config['regex']
        action = regex_pattern_config['action']
        try:
            for i in range(len(input_col_names)):

                regex_check_results = self.validator.expect_column_values_to_match_regex(
                    column=input_col_names[i],
                    regex=regex_pattern[i],
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    regex_check_results = regex_check_results.to_json_dict()
                    unexpected_indices = regex_check_results['result'][
                        'unexpected_index_list']
                    
                    # drop_rows_index_set.update(unexpected_indices)

                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)

            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )

        except Exception as e:
            logging.error(f"Error in valid regex pattern "
                          f"check check function: {str(e)}")
            raise e

        return self.validator, ge_index_values

    def validate_json_data(self, json_data_config, ge_index_values):

        input_col_names = json_data_config["input_col_names"]
        action = json_data_config["action"]
        filepath = json_data_config["filepath"]
    
        json_schema_str = self.session.read.json(filepath).collect()[0][0]
        json_schema = json.loads(json_schema_str)
        # input_col_names = json_data_config.get("input_col_names")

        try:

            for column in input_col_names:
                logging.info(f"valid json_schema check for column: {column}")
                json_results = self.validator.expect_column_values_to_match_json_schema(
                    column=column,
                    json_schema=json_schema,
                    result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [],
                        "return_unexpected_index_query": True,
                    }
                )

                if action == "drop":
                    json_results = json_results.to_json_dict()
                    unexpected_indices = json_results['result'][
                        'unexpected_index_list']
                    # drop_rows_index_set.update(unexpected_indices)
                    ge_index_values['drop_rows_index_set'].update(unexpected_indices)
            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )
        except Exception as e:
            logging.error(f"Error in valid json_schema pattern "
                          f"check check function: {str(e)}")
            raise e

        return self.validator, ge_index_values

    # Apply data quality checks based on input quality checks
    def apply_data_quality_checks(self, ge_index_values):
        # drop_rows_index_set = drop_set_index
        try:
            quality_check_functions = {
                "is_null": self.is_null_check,
                "in_range": self.in_range_check,
                "verify_element": self.verify_element,
                "valid_email": self.valid_email_check,
                "valid_phonenumber": self.valid_phonenumber_check,
                "valid_regex_pattern": self.regex_pattern_check,
                "json_schema_validate": self.validate_json_data
            }

            for quality_check in self.data_quality_checks:
                function_name = quality_check["function_name"]
                logging.info(f"Applying data quality check: {function_name}")

                # Get the corresponding quality check function
                quality_check_function = quality_check_functions.get(
                    function_name)

                if quality_check_function:
                    quality_check_function(quality_check, ge_index_values)
                  
            self.validator.save_expectation_suite(
                discard_failed_expectations=False
            )
        except Exception as e:
            logging.error(f"Error in applying data quality checks: {str(e)}")
            raise e

        return self.validator, ge_index_values
