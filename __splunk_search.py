import re
from os import environ as env

import splunklib.client as client
import splunklib.results as results
from splunklib.binding import HTTPError
from __module_log import log_error, log_info

def connect_to_splunk(host, port, cred):
    splunk = client.connect(
                host=host, 
                port=port,
                username=env[cred+'_user'], 
                password=env[cred+'_word']
                )
    return splunk

def splunk_earliest_time(str):
    # extract the pattern returned such as returned from max_time_sql 
    res = re.search( 'earliest=(\S+)', str ) #  find all the non-space chars after an 'earliest=' string
    if res is None or res.lastindex < 1:
        return None
    else:
        return(res.group(1))  # return the first match

def splunk_to_str(splunk, splunk_search, col_end='\t', row_end='\n'):
    return_str = ""
    try:
        splunk.parse(**splunk_search, parse_only=True)
    except HTTPError as e:
        log_error("Error in splunk_to_str: splunk query returned an error:\n\t%s" % str(e), 2)
        return None
    search_results = splunk.jobs.export(**splunk_search)
    search_results_reader = search_results.ResultsReader(search_results, count=0)
    for result in search_results_reader:
        if isinstance(result, dict):
            for item in result:
                return_str += str(result[item]) + col_end
            return_str += row_end
        elif isinstance(result, results.Message):
            # Diagnostic messages may be returned in the results
            log_info (result)
    return return_str

def splunk_to_csv(splunk, splunk_search, csv_file):
    try:
        splunk.parse(**splunk_search, parse_only=True)
    except HTTPError as e:
        log_error("Error in splunk_to_csv: splunk query returned an error:\n\t%s" % str(e), 2)
        return
    search_job = splunk.jobs.create(**splunk_search, exec_mode="blocking")
    search_results = search_job.results(output_mode="csv", count=0)
    with open(csv_file, 'wb') as f:
        f.write(search_results.read())
