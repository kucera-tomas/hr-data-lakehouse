from airflow.providers.amazon.aws.hooks.athena import AthenaHook
import logging

def check_dq(query, bucket_name, **kwargs):
    """
    Runs a Data Quality query. 
    The query must return a SINGLE integer (the count of bad rows).
    If the count > 0, the task FAILS.
    """
    hook = AthenaHook(aws_conn_id='aws_default', region_name='eu-north-1')

    logging.info(f"Running Data Quality Check: {query}")
    
    # Run the query
    query_id = hook.run_query(
        query, 
        query_context={'Database': 'default'},
        result_configuration={'OutputLocation': f's3://{bucket_name}/athena_results/'}
    )
    
    hook.poll_query_status(query_id)
    
    # Get the results
    results = hook.get_query_results(query_id)
    
    # Parse the result (The first row is usually the header, the second is the value)
    try:
        # Get the actual count value from the second row (index 1)
        bad_row_count = int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    except (IndexError, KeyError):
        raise ValueError("Query did not return a valid count!")

    logging.info(f"Found {bad_row_count} bad rows.")

    if bad_row_count > 0:
        raise ValueError(f"Data Quality Check Failed! Found {bad_row_count} bad rows. Pipeline stopped.")
    
    logging.info("Data Quality Check Passed!")