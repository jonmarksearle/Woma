from os import environ as env

import cx_Oracle
import psycopg2
import csv


def connect_to_redshift(env_stub):
    # connect to redshift using environment variables
    # return the redshift database connection
    red_dev_host=env[env_stub+'_host']
    red_dev_db=env[env_stub+'_db']
    red_dev_user=env[env_stub+'_user']
    red_dev_word=env[env_stub+'_word']
    #log_info (' red_dev_host:'+red_dev_host+'\n red_dev_db:'+red_dev_db+'\n red_dev_user:'+red_dev_user+'\n red_dev_word:'+red_dev_word)
    redshift = psycopg2.connect(host=red_dev_host, port="5439", database=red_dev_db, user=red_dev_user, password=red_dev_word, sslmode="require")
    return redshift

def connect_to_oracle(env_stub):
	# connect to oracle using environment variables
	# return the oracle database connection
	orcl_host=env[env_stub+'_host']
	orcl_sid=env[env_stub+'_sid']
	orcl_user=env[env_stub+'_user']
	orcl_word=env[env_stub+'_word']
	#write_log (' orcl_host:'+orcl_host+'\n orcl_sid:'+orcl_sid+'\n orcl_user:'+orcl_user+'\n orcl_word:'+orcl_word)
	#write_log (''+orcl_user+'/'+orcl_word+'@'+orcl_host+':1521/'+orcl_sid)
	#orcl = cx_Oracle.connect(orcl_user+'/'+orcl_word+'@'+orcl_host+':1521/'+orcl_sid)
	orcl = cx_Oracle.connect(orcl_user+'/'+orcl_word+'@'+orcl_host+':2484/'+orcl_sid)
	return orcl

def sql_to_str(con, sql, col_end=' ', row_end='\n'):
    return_str = ""
    curs = con.cursor()
    curs.execute(sql)
    if (curs.description is not None):
        for row in curs:
            for cell in row:
                return_str += str(cell) + col_end
            return_str += row_end
    curs.close()
    return return_str

def sql_to_csv(con, sql, csv_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n'):
	# run a sql statement and write the results into a csv file
	# return the number of rows read
	curs = con.cursor()
	curs.execute(sql)
	file = open(csv_file, 'w')
	csv_out_file = csv.writer(file, delimiter=delimiter, quotechar=quotechar, quoting=quoting, lineterminator=lineterminator )
	# write the column names row
	csv_out_file.writerow([col[0] for col in curs.description])
	# write the data rows
	csv_out_file.writerows(curs.fetchall())
	file.close()
	row_count = curs.rowcount
	curs.close()
	return row_count
