#w wolnej chwili sprobowac przesztalcic kod w bloki try/except
import psycopg2 as p
from pprint import pprint
import pandas as pd
import time
import decimal as dc
import datetime as dt
import numpy as np
class InvalidSchemaTypeError(ValueError):
	'''
	Error to raise when schema type is not 'all',
	'system', 'user_made', or if user wants to type
	name of schema, that does not exists
	'''
	pass


class InvalidSchemaError(ValueError):
	'''
	Error to raise when schema name given by user
	is not name of real schema listed in db
	'''
	pass


class InvalidTableNameError(ValueError):
	'''
	Error to raise when table name given by user
	is not name of table listed in given schema
	'''

class UnevenColumnsError(Exception):
	'''
	Error to raise when parameters of two data frames,
	one taken from db and second external are not matchet
	'''


class DBConnection():

	'''
	Class to connect to postgresql database
	   and easily operate on it's elements
	   Author: Mateusz Janczak
	'''
	adapt_types = {bool: ['bool'], float: ['real', 'double'], int: ['smallint', 'integer', 'bigint'],
				   dc.Decimal: ['numeric'], str: ['varchar(200)', 'text'], memoryview: ['bytea'],
				   bytearray: ['bytea'], bytes: ['bytea'], dt.date: ['date'], np.datetime64: ['date', \
				   'timestamp(0) without time zone', 'timestamp(0) with time zone'], #sprawdz np.datetime czy na pewno wszystkie
				   dt.datetime: ['timestamp(0) without time zone', 'timestamp(0) with time zone'],
				   dt.timedelta: ['interval'], list: ['ARRAY'], dict: ['hstore']}

	adapt_types_pd = {'object': ['varchar(200)', 'text'], 'int64': ['smallint', 'integer', 'bigint'],
					  'float64': ['numeric'], 'datetime64[ns]': ['date', 'timestamp(0) without time zone',\
																 'timestamp(0) with time zone']}


	def __init__(self, db_name, user_name, user_password, db_host, db_port):
		self.db_name = db_name
		self.user_name = user_name
		self.user_password = user_password
		self.db_host = db_host
		self.db_port = db_port
		self.default_shema = 'public'

		#connect to data base
		try:
			self.connection = p.connect(
				"dbname='"+self.db_name+"' user='"+self.user_name+"' password='"+self.user_password
				+"' host='"+self.db_host+"' port='"+str(self.db_port)+"'"
			)
			self.cursor = self.connection.cursor()
		except:
			pprint("Cannot connect to database")

	#list db schemas
	def list_schemas(self, schema_type=''):
		if schema_type == '':
			sql_command = """select schema_name from information_schema.schemata;"""

		elif schema_type == 'user_made':
			sql_command = """select schema_name from information_schema.schemata
							 where schema_name not in ('information_schema', 'public')
							 and schema_name not like 'pg_%'"""
		elif schema_type == 'system':
			sql_command = """select schema_name from information_schema.schemata
							 where schema_name in ('information_schema', 'public')
							 or schema_name like 'pg_%'"""
		else:
			raise InvalidSchemaTypeError(schema_type)
		self.cursor.execute(sql_command)
		schemas = self.cursor.fetchall()
		schemas = tuple(map(lambda x: x[0], schemas))
		return(schemas)

	# function that checks if schema parameter is proper
	def schema_error_raiser(self, schema):
		if isinstance(schema, str)==False:
			raise TypeError(schema)
		if schema not in self.list_schemas():
			raise InvalidSchemaError(schema)
		else:
			pass

	# setting custom default schema, so you don't have to put it in each function
	def set_default_schema(self, schema):
		self.schema_error_raiser(schema)
		self.default_shema=schema
		return self

	# list tables of given schema
	def list_tables(self, schema=''):
		if schema == '':
			sql_command = """SELECT table_name FROM information_schema.tables;"""
		else:
			self.schema_error_raiser(schema)
			sql_command = """SELECT table_name FROM information_schema.tables
		WHERE table_schema='"""+ schema + """';"""
		self.cursor.execute(sql_command)
		tables = self.cursor.fetchall()
		tables = tuple(map(lambda x: x[0], tables))
		return(tables)

	# get column name from given schema.table and data type that it contains. returns dictionary.
	def get_table_columns(self, table, schema=''):
		schema = self.default_shema if schema == '' else schema
		self.schema_error_raiser(schema)
		sql_command = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
		FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid
		AND a.attnum > 0
		WHERE  i.indrelid = '""" + schema + """.""" + table + """'::regclass"""
		self.cursor.execute(sql_command)
		result = self.cursor.fetchall()
		keys = ["\"" + s[0] + "\"" if any(a.isupper() for a in s[0]) \
					 else s[0] for s in result]
		values = [s[1] for s in result]
		dct = dict(zip(keys, values))
		return dct

	# function to check if 'table' parameter is proper
	def table_error_raiser(self, table):
		if table not in self.list_tables():
			raise InvalidTableNameError(table)
		else:
			pass

	# function to check if '\df' parameter is proper
	def df_error_raiser(self, df):
		if isinstance(df, pd.DataFrame)==False:
			raise TypeError(df)
		else:
			pass

	# get schema.table primary key(s) name(s) and data type(s)
	def get_table_pk(self, table, schema=''):
		schema = self.default_shema if schema == '' else schema
		self.schema_error_raiser(schema)
		self.table_error_raiser(table)
		sql_command = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
		FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid 
		AND a.attnum = ANY(i.indkey) 
		WHERE  i.indrelid = '"""+schema+"""."""+table+"""'::regclass
		AND    i.indisprimary;"""
		self.cursor.execute(sql_command)
		result = self.cursor.fetchall()
		keys = ["\"" + s[0] + "\"" if any(a.isupper() for a in s[0]) \
					 else s[0] for s in result]
		values = [s[1] for s in result]
		dct = dict(zip(keys, values))
		return dct

	#compare column names and data types of schema.table and given pd.DataFrame
	def compare_cols(self, df, table, schema=''):
		schema = self.default_shema if schema == '' else schema
		self.schema_error_raiser(schema)
		self.df_error_raiser(df)
		table_cols = self.get_table_columns(table, schema)
		table_col_names = list(table_cols.keys())
		for col in df.columns:
			if table_cols[col] not in self.adapt_types_pd[str(df[col].dtype)]:
				return False
		result = set(df.columns).issubset(table_col_names)
		return result

	#check if there are duplicates in df and schema.table, considering primary key
	def find_duplicates(self, df, table, schema=''):
		schema = self.default_shema if schema == '' else schema
		self.schema_error_raiser(schema)
		self.table_error_raiser(table)
		self.df_error_raiser(df)
		if self.compare_cols(df, table, schema) == False:
			raise UnevenColumnsError
		#get table primary keys
		p_keys = self.get_table_pk(table, schema)
		p_keys = list(p_keys.keys())
		#inf df leave only columns that are table's primary keys
		df = df[p_keys]
		df = df.copy()
		#change data type to string if column containes dates
		for col in df.columns:
			if df[col].dtype in (dt.date, dt.datetime):
				df[col] = df[col].astype(str)
		#built conditions for query
		condition_sets = [str(tuple(df[col])) for col in df]
		condition_sets = [c+ ' in ' +l for c,l in zip(p_keys, condition_sets)]
		condition_sets = " and ".join(condition_sets)
		#built query
		sql_command = """SELECT """ + ", ".join(p_keys) + \
					  """ FROM """+ schema + """.""" + table + """
					  WHERE """ + condition_sets + """;"""
		#execute query
		self.cursor.execute(sql_command)
		duplicates = self.cursor.fetchall()
		duplicates = pd.DataFrame(duplicates, columns=df.columns)
		return duplicates


	def update_table(self, df, table,  schema='', keep_duplicates=False):
		schema = self.default_shema if schema == '' else schema
		if keep_duplicates not in ('first', 'last', False):
			raise ValueError(keep_duplicates)
		self.schema_error_raiser(schema)
		self.table_error_raiser(table)
		self.df_error_raiser(df)
		if self.compare_cols(df, table, schema) == False:
			raise UnevenColumnsError
		#get primary keys
		p_keys = self.get_table_pk(table, schema)
		p_keys = list(p_keys.keys())
		#get table columns details
		table_columns = self.get_table_columns(table, schema)
		# find duplicates in df, considering table primary keys
		# if keep_uplicates == False and df contains any duplicates, then raise error. MOŻE ZMIENIC TO, ZEBY PO PROSTU WYWALALO DUPLIKATY
		# else: drop duplicates from df
		if keep_duplicates == False:
			duplicates = df.duplicated(subset=p_keys, keep=keep_duplicates)
			if any(duplicates):
				raise ValueError("duplicate key found: {0}".format(df[p_keys][duplicates]))
		else:
			df.drop_duplicates(subset=p_keys, keep=keep_duplicates, inplace=True)
		#find duplicates betweeen df and table
		pk_to_update = self.find_duplicates(df,table,schema)
		# prepare parameters to build query
		cols = ", ".join([k + ' ' + table_columns[k] for k in df.columns])
		df_to_update = df.merge(pk_to_update, how='left')
		vals = [tuple(x) for x in df_to_update.to_records(index=False)]
		vals = ", ".join(repr(e) for e in vals).replace(',)', ')')
		pk_match = [table + "." + key + " = a."+key for key in p_keys]
		pk_match = ", ".join(pk_match)
		cols_to_update_match = [x for x in table_columns.keys() if x not in p_keys]
		cols_to_update_match = [col + " = a."+col for col in cols_to_update_match]
		cols_to_update_match = ", ".join(cols_to_update_match)
		#build query
		sql_query = """CREATE TEMP TABLE tmp(""" + cols + """);
   		INSERT INTO tmp VALUES """+ vals +""";
   		UPDATE """ + schema + """.""" + table + """
   		SET """ +cols_to_update_match+ """
   		FROM tmp a
		WHERE """ + pk_match + """;"""
		#execute query
		self.cursor.execute(sql_query)
		self.connection.commit()
		rows_updated = df_to_update.shape[0]
		result_dict = {'rows_updated': rows_updated}
		return result_dict #sprawdzic tez zapytanie w sposob z: https://stackoverflow.com/questions/18797608/update-multiple-rows-in-same-query-using-postgresql


	def insert_df(self, df, table, schema='', keep_duplicates=False, update_duplicates=False):
		schema = self.default_shema if schema == '' else schema
		if keep_duplicates not in ('first', 'last', False):
			raise ValueError(keep_duplicates)
		self.schema_error_raiser(schema)
		self.table_error_raiser(table)
		self.df_error_raiser(df)
		if self.compare_cols(df, table, schema) == False:
			raise UnevenColumnsError
		#get primary keys
		p_keys = self.get_table_pk(table, schema)
		p_keys = list(p_keys.keys())
		#get table columns details
		table_columns = self.get_table_columns(table, schema)
		inserted_rows = 0
		updated_rows = 0
		# find duplicates in df, considering table primary keys
		# if keep_uplicates == False and df contains any duplicates, then raise error. MOŻE ZMIENIC TO, ZEBY PO PROSTU WYWALALO DUPLIKATY
		# else: drop duplicates from df
		if keep_duplicates == False:
			duplicates = df.duplicated(subset=p_keys, keep=keep_duplicates)
			if any(duplicates):
				raise ValueError("duplicate key found: {0}".format(df[p_keys][duplicates]))
		else:
			df.drop_duplicates(subset=p_keys, keep=keep_duplicates, inplace=True)
		# find duplicates between df and table
		pk_duplicates = self.find_duplicates(df,table,schema)
		#update table in case there are duplicates between df and table and update_duplicates==True
		if (pk_duplicates.shape[0]!=0 and update_duplicates==True):
			#get records to update
			df_to_update = df.merge(pk_duplicates, how='right')
			cols = ", ".join([k + ' ' + table_columns[k] for k in df.columns])
			#prepare other parameters to build update query
			vals = [tuple(x) for x in df_to_update.to_records(index=False)]
			vals = ", ".join(repr(e) for e in vals).replace(',)', ')')
			pk_match = [table + "." + key + " = a." + key for key in p_keys]
			pk_match = ", ".join(pk_match)
			cols_to_update_match = [x for x in table_columns.keys() if x not in p_keys]
			cols_to_update_match = [col + " = a." + col for col in cols_to_update_match]
			cols_to_update_match = ", ".join(cols_to_update_match)
			#build update query
			sql_query_update = """CREATE TEMP TABLE tmp(""" + cols + """);
			   		INSERT INTO tmp VALUES """ + vals + """;
			   		UPDATE """ + schema + """.""" + table + """
			   		SET """ + cols_to_update_match + """
			   		FROM tmp a
					WHERE """ + pk_match + """;"""
			# execute query
			self.cursor.execute(sql_query_update)
			self.connection.commit()
			updated_rows = df_to_update.shape[0]
		#prepare records to insert
		df_to_insert = (pd.merge(df, pk_duplicates, indicator=True, how='outer')
			.query('_merge=="left_only"')
			.drop('_merge', axis=1))
		#if there are records to insert - prepare all parameters and build query
		if  df_to_insert.shape[0] != 0:
			#prepare parameters
			cols = ", ".join(df_to_insert.columns)
			vals = [tuple(x) for x in df_to_insert.to_records(index=False)]
			vals = ", ".join(repr(e) for e in vals).replace(',)', ')')
			#build query
			sql_query = """INSERT INTO """ + schema + """.""" + table + """ (""" + \
			cols +""")
			VALUES """+vals
			#execute query
			self.cursor.execute(sql_query)
			self.connection.commit()
			inserted_rows = df_to_insert.shape[0]
		# return result
		result_dict = {'rows_inserted': inserted_rows, 'rows_updated': updated_rows}
		return result_dict




conn = DBConnection('energy', 'MateuszJanczak', '4r/]309Yv|2', 'localhost', 5432)
conn = conn.set_default_schema('energy')


to_insert = pd.DataFrame({'energy_system_name': ['PL', 'DE'],
						  'country': ['Poland', 'Germany']})


start_time = time.time()
#print(conn.update_table(to_insert, 'energy_systems'))
print(conn.insert_df(to_insert, 'energy_systems', keep_duplicates='first', update_duplicates=False))
#print(conn.find_duplicates(to_insert,'energy_systems'))
print(time.time()-start_time)