import psycopg2_binary
from util.dw_connection import DWConnection
import config

dw_db_name = config.DATABASES['dw']['dbname']
dw_user = config.DATABASES['dw']['user']
dw_password = config.DATABASES['dw']['password']
dw_host = config.DATABASES['dw']['host']

with DWConnection("host='{host}' user = '{user}' password = '{password}' dbname = '{dbname}'".format(host=dw_host,user=dw_user,password=dw_password,dbname=dw_db_name)) as connection:

	cur = connection.cursor()
	print('Connection established to DW')

	with open('sql/queries.sql', 'r') as f:
		sql_file = f.read()
	
		sql_commands = sql_file.strip('\n').strip(' ').split(';')
		for command in sql_commands:
			print('Executing------------------------------------------------------')
			print(command)
			print('---------------------------------------------------------------')
			cur.execute(command)
