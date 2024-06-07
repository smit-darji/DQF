import validators
import pandas as pd
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ColumnValuesContainValidEmail(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_email"
    condition_value_keys = ()

    # This method defines the business logic for evaluating your metric when
    # using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def to_validate_email(email_id):

            # if pd.isna(email_id) or email_id is None:
            #     return False

            a = validators.email(email_id)
            if a is True:
                return True
            else:
                return False

        return column.apply(lambda x: to_validate_email(x) if x else False)

    # This method defines the business logic for evaluating your metric when
    # using a SqlAlchemyExecutionEngine
    #     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    #     def _sqlalchemy(cls, column, _dialect, **kwargs):
    #         return column.in_([3])

    # This method defines the business logic for evaluating your metric when
    # using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):

        def to_validate_email(email_id):
            a = validators.email(email_id)
            if a is True:
                return True
            else:
                return False

        return column.apply(lambda x: to_validate_email(x) if x else False)


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToContainValidEmail(ColumnMapExpectation):
    """Expect values in given column to be valid email addresses."""

    # These examples will be shown in the public gallery, and also executed as
    # unit tests for your Expectation

    def validate_configuration(self, configuration) -> None:
        """
        Validates that a configuration has been set, and sets a configuration
        if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation
        of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used
                to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is
            not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

    examples = [
        {
            "data": {
                "fail_case_1": ["a123@something", "a123@something.", "a123."],
                "fail_case_2": ["aaaa.a123.co", "aaaa.a123.", "aaaa.a123.com"],
                "fail_case_3": ["aaaa@a123.e", "aaaa@a123.a", "aaaa@a123.d"],
                "fail_case_4": ["@a123.com", "@a123.io", "@a123.eu"],
                "pass_case_1": [
                    "a123@something.com",
                    "vinod.km@something.au",
                    "this@better.work",
                ],
                "pass_case_2": [
                    "example@website.dom",
                    "ex.ample@example.ex",
                    "great@expectations.email",
                ],
                "valid_emails": [
                    "janedoe@company.org",
                    "someone123@stuff.net",
                    "mycompany@mycompany.com",
                ],
                "bad_emails": ["Hello, world!", "Sophia", "this should fail"],
            },
            "tests": [
                {
                    "title": "negative_test_for_no_domain_name",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_1"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "a123@something",
                            "a123@something.",
                            "a123.",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_no_at_symbol",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_2"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "aaaa.a123.co",
                            "aaaa.a123.",
                            "aaaa.a123.com",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_ending_with_one_character",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_3"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "aaaa@a123.e",
                            "aaaa@a123.a",
                            "aaaa@a123.d",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_emails_with_no_leading_string",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_4"},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "pass_test_1",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "pass_case_1"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "pass_test_2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "pass_case_2"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "valid_emails",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_emails"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "invalid_emails",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "bad_emails"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "Hello, world!",
                            "Sophia",
                            "this should fail",
                        ],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation"],
        "contributors": [  # Github
            "@aworld1",
            "@enagola",
            "@spencerhardwick",
            "@vinodkri1",
            "@degulati",
            "@ljohnston931",
            "@rexboyce",
            "@lodeous",
            "@sophiarawlings",
            "@vtdangg",
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name`
    # defined in your Metric class above.
    map_metric = "column_values.valid_email"

    # This is a list of parameter names that can affect whether the Expectation
    # evaluates to True or False
    # Please see {some doc} for more information about domain and success keys,
    # and other arguments to Expectations
    success_keys = ()

    # This dictionary contains default values for any parameters that should
    # have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    # !!! This example renderer should render RenderedStringTemplateContent,
    # not just a string


if __name__ == "__main__":
    ExpectColumnValuesToContainValidEmail().print_diagnostic_checklist()
