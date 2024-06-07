from datetime import timedelta, datetime
import boto3
import logging
from airflow.models import Variable
import snowflake.connector
from jira import JIRA


session = boto3.session.Session()
ssm_client = session.client('ssm', region_name='us-east-1')
env = ssm_client.get_parameter(Name='env', WithDecryption=True).get('Parameter').get('Value')
jira_api_token = ssm_client.get_parameter(Name='/apps/jira/api_token', WithDecryption=True).get('Parameter').get('Value')


def get_snowflake_info(ssm_client):
    logging.info("Getting env info")
    env_param = ssm_client.get_parameter(Name="env", WithDecryption=True)
    env_value = env_param.get("Parameter").get("Value")

    snf_acc_param = ssm_client.get_parameter(
        Name="/users/snowflake/account", WithDecryption=True
    )
    snf_acc_value = snf_acc_param.get("Parameter").get("Value")
    snf_user_param = ssm_client.get_parameter(
        Name="/users/snowflake/account/user", WithDecryption=True
    )
    snf_user_value = snf_user_param.get("Parameter").get("Value")
    snf_key_param = ssm_client.get_parameter(
        Name="/users/snowflake/account/password", WithDecryption=True
    )
    snf_key_value = snf_key_param.get("Parameter").get("Value")
    env_var = {
        "env": env_value,
        "snf_account": snf_acc_value,
        "snf_user": snf_user_value,
        "snf_key": snf_key_value,
    }
    env_var["warehouse"] = "INVH_ANALYTICS_WAREHOUSE"
    env_var["database"] = "QUALITY"

    if env_value == "qa":
        env_var["schema"] = "QA"
    elif env_value == "prod":
        env_var["schema"] = "PROD"
    else:
        env_var["schema"] = "DEV"
    return env_var


session = boto3.session.Session()
client = session.client("ssm", region_name="us-east-1")
sf_secret = get_snowflake_info(client)


conn = snowflake.connector.connect(
    user=sf_secret["snf_user"],
    password=sf_secret["snf_key"],
    account=sf_secret["snf_account"],
    warehouse=sf_secret["warehouse"],
    database=sf_secret["database"],
    schema=sf_secret["schema"],
    role="sysadmin",
)
env = sf_secret["env"]
schema = sf_secret["schema"]


def jira_authenticate():
    try:
        logging.info('Jira authentication process started')
        server = Variable.get("jira_endpoint")
        username = Variable.get("jira_user")
        jir = JIRA(basic_auth=(str(username), str(jira_api_token)), options={'server': str(server)})
        logging.info('Jira authentication successful')
        return jir
    except Exception as e:
        logging.error(f'Jira authentication failed: {str(e)}')
        raise e


def get_date():
    current_date = datetime.now().strftime('%Y-%m-%d')
    date_range_str = Variable.get("date_range")
    try:
        date_range = int(date_range_str)
        date_within_range = (datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=date_range)).strftime('%Y-%m-%d')
        return date_within_range
    except ValueError:
        logging.error('Error: Invalid date range value provided')
        raise ValueError


def filter_records(insert_date, batch_id, query_type):
    try:
        curs = conn.cursor()
        if query_type == 'profiling':
            curs.execute("select batch_id, CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE) as date_of_profiling, input_table_name, data_outlier_description, sum(records_processed) as records_processed from quality.{}.master_profiling_table where CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE) = '{}' and batch_id = '{}' and data_outlier = 'true' group by batch_id, CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE), input_table_name, data_outlier_description".format(schema, insert_date, batch_id))
        elif query_type == 'error':
            curs.execute("SELECT output_table, key AS failed_quality_checks, cast(insert_timestamp as date) as date_of_ingestion, batch_id, count(1) as error_count FROM quality.{}.master_error_table, LATERAL FLATTEN(PARSE_JSON(quality_check_output)) WHERE value::boolean = true and cast(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(insert_timestamp)) as date) = '{}' and batch_id = '{}' group by output_table, key, cast(insert_timestamp as date), batch_id order by date_of_ingestion".format(schema, insert_date, batch_id))
        return curs.fetchall()
    except Exception as e:
        logging.error(f"Error in filter_records: {str(e)}")
        raise e


def get_unprocessed_batches():
    try:
        curs = conn.cursor()
        curs.execute("SELECT DISTINCT batch_id, output_table FROM quality.{}.master_error_table WHERE jira_tracking_ticket is null and tracking_ticket is null".format(schema))
        return curs.fetchall()
    except Exception as e:
        logging.error(f"Error in get_unprocessed_batches: {str(e)}")
        raise e


def create_tickets_for_error_table():
    try:
        logging.info("Started creating the Jira tickets for distinct batches of error table.")
        logging.info(env)
        jir = jira_authenticate()
        curs = conn.cursor()
        insert_date = get_date()
        processed_batch_ids = {}

        curs.execute("SELECT DISTINCT batch_id, output_table as output_table FROM quality.{}.master_error_table WHERE CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(insert_timestamp)) AS DATE) = '{}'".format(schema, insert_date))
        distinct_batches_tables = curs.fetchall()

        if not distinct_batches_tables:
            epic_key = Variable.get("jira_error_epicid")
            summary = f"No batch_ids found on {insert_date}"
            description = f"No batch_ids were generated on {insert_date}. There are no records for today."
            field_data = {
                "project": 'DCS',
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Task"},
                "parent": {"key": str(epic_key)}
            }
            key = jir.create_issue(fields=field_data)
            logging.info("No batch_ids found today. Created a ticket to indicate the absence of batches.")
            return

        unprocessed_batches = get_unprocessed_batches()

        for batch_id_table in distinct_batches_tables:
            epic_key = Variable.get("jira_error_epicid")
            batch_id = batch_id_table[0]
            output_table = batch_id_table[1]
            if (batch_id, output_table) not in unprocessed_batches:
                continue
            custom_column_names = ['output_table', 'failed_quality_checks', 'date_of_ingestion', 'batch_id', 'error_count']
            records = filter_records(insert_date, batch_id, 'error')
            filtered_records = [record for record in records if record[0] == output_table]
            if not filtered_records:
                continue
            if batch_id in processed_batch_ids:
                logging.info(f"Batch ID {batch_id} for {output_table} has already been processed. Skipping ticket creation.")
                continue
            processed_batch_ids[batch_id] = output_table
            table_entries = ""
            for record in filtered_records:
                table_entries += "| " + " | ".join(str(cell) for cell in record) + " |\n"
            summary = f"{filtered_records[0][1]} records found in {output_table} for batch {batch_id}"
            description = f"Errors have been detected in batch {batch_id} within the '{output_table}' table. Below is a summary of the error details found in snowflake master_error_table on {insert_date}: \n\n"
            description += "| " + " | ".join(custom_column_names) + " |\n"
            description += table_entries
            field_data = {
                "project": 'DCS',
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Task"},
                "parent": {"key": str(epic_key)}
            }
            key = jir.create_issue(fields=field_data)
            logging.info("Created alert on error table for batch_id {} with ticket no: {}".format(batch_id, str(key)))
            sql1 = "UPDATE QUALITY.{}.MASTER_ERROR_TABLE SET JIRA_TRACKING_TICKET = '{}' WHERE JIRA_TRACKING_TICKET is null AND CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(insert_timestamp)) AS DATE) = '{}' AND batch_id = '{}' AND output_table = '{}'".format(schema, str(key), insert_date, batch_id, output_table)
            curs.execute(sql1)
            sql2 = "UPDATE QUALITY.{}.MASTER_ERROR_TABLE SET JIRA_STATUS = 'Ready for work' WHERE JIRA_STATUS IS NULL AND CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(insert_timestamp)) AS DATE) = '{}' AND batch_id = '{}' AND output_table = '{}'".format(schema, insert_date, batch_id, output_table)
            curs.execute(sql2)
            logging.info(f"Updated columns within quality.{schema}.master_error_table")
    except Exception as e:
        logging.error(f"Error in create_tickets_for_error_table: {str(e)}")
        raise e


def create_tickets_for_profiling_table():
    try:
        logging.info("Started creating the Jira issues for distinct batches of profiling table.")
        logging.info(env)
        jir = jira_authenticate()
        curs = conn.cursor()
        insert_date = get_date()

        curs.execute("SELECT distinct batch_id, input_table_name FROM quality.{}.master_profiling_table WHERE data_outlier = 'true' and CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE) = '{}'".format(schema, insert_date))
        distinct_batches_tables = curs.fetchall()

        for batch_id_table in distinct_batches_tables:
            batch_id = batch_id_table[0]
            input_table_name = batch_id_table[1]

            custom_column_names = ['batch_id', 'date_of_profiling', 'input_table_name', 'data_outlier_description', 'records_processed']
            records = filter_records(insert_date, batch_id, 'profiling')
            if not records:
                continue

            epic_key = Variable.get("jira_profiling_epicid")
            table_entries = ""
            for record in records:
                table_entries += "| " + " | ".join(str(cell) for cell in record) + " |\n"

            summary = f"Data Outliers found in {input_table_name} for {batch_id}"
            description = f"Data outliers have been detected in batch {batch_id} within the input_table '{input_table_name}'. Below is a summary of the data outliers details found in snowflake master_profiling_table on {insert_date}: \n\n"
            description += "| " + " | ".join(custom_column_names) + " |\n"
            description += table_entries
            field_data = {
                "project": 'DCS',
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Task"},
                "parent": {"key": str(epic_key)}
            }
            key = jir.create_issue(fields=field_data)
            logging.info("Created alert on profiling table for batch_id {} with ticket no: {}".format(batch_id, str(key)))
            sql1 = "UPDATE QUALITY.{}.MASTER_PROFILING_TABLE SET TRACKING_TICKET = '{}' where TRACKING_TICKET is null and CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE) = '{}' AND batch_id = '{}' AND input_table_name = '{}'".format(schema, str(key), insert_date, batch_id, input_table_name)
            curs.execute(sql1)
            sql2 = "UPDATE QUALITY.{}.MASTER_PROFILING_TABLE SET STATUS = 'Ready for work' where STATUS IS NULL and CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_LTZ(date_of_profiling)) AS DATE) = '{}' AND batch_id = '{}' AND input_table_name = '{}'".format(schema, insert_date, batch_id, input_table_name)
            curs.execute(sql2)
            logging.info(f"Updated columns within quality.{schema}.master_profiling_table")
    except Exception as e:
        logging.error(f"Error in create_tickets_for_profiling_table: {str(e)}")
        raise e


def update_tickets_status():
    try:
        logging.info('Updating status of the previouly created issues')
        logging.info(env)
        current_date = datetime.now().strftime('%Y-%m-%d')
        jir = jira_authenticate()
        curs = conn.cursor()
        for single_issue in jir.search_issues(jql_str='project = "DCS" and createdDate < "{}"'.format(current_date)):
            ticket_id = single_issue.key
            ticket_status = single_issue.fields.status.name
            sql3 = "UPDATE quality.{}.MASTER_ERROR_TABLE SET JIRA_STATUS = '{}' WHERE JIRA_TRACKING_TICKET = '{}'".format(schema, ticket_status, ticket_id)
            curs.execute(sql3)
            sql4 = "UPDATE quality.{}.MASTER_PROFILING_TABLE SET STATUS = '{}' WHERE TRACKING_TICKET = '{}'".format(schema, ticket_status, ticket_id)
            curs.execute(sql4)
        logging.info("Status of previously created tickets were updated within snowflake tables.")
    except Exception as e:
        logging.error(f"Error in create_tickets_for_profiling_table: {str(e)}")
        raise e
