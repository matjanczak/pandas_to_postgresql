#w wolnej chwili sprobowac przesztalcic kod w bloki try/except
import psycopg2 as p
from pprint import pprint
import pandas as pd
import time
import decimal as dc
import datetime as dt
import numpy as np
import itertools


class DBConnection():

	'''
	Class to connect to postgresql database
	   and easily operate on it's elements
	   Author: Mateusz Janczak
	'''

	## POUSUWAC NIEPOTRZEBNE SLOWNIKI!!!!

	__adapt_types_pd = {'object': ['varchar(200)', 'text'], 'int64': ['smallint', 'integer', 'bigint', 'numeric'],
					  'int32': ['smallint', 'integer', 'bigint', 'numeric'], 'bool': ['boolean'], 'boolean': ['boolean'],
					  'float64': ['numeric'], 'datetime64[ns]': ['date', 'timestamp without time zone',\
																 'timestamp with time zone']}

	__type_codes = {16: 'bool', 23: 'int', 1700: 'float', 25: 'str', 701: 'float', 17: 'bytes'} # DOKONCZYC TYPE CODES i TYPE CODES DATE POZNIEJ. NA RAZIE STARCZY

	__type_codes_date = {1114: '%Y-%m-%d %H:%M:%S', 1184: None,  1082: '%Y-%m-%d'}

	__date_sql_to_pd = {'timestamp without time zone': '%Y-%m-%d %H:%M:%S', 'date': '%Y-%m-%d'}

	def __init__(self, db_name, user_name, user_password, db_host, db_port):
		self.db_name = db_name
		self.user_name = user_name
		self.user_password = user_password
		self.db_host = db_host
		self.db_port = db_port
		self.__default_shema = 'public'

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
		"""List schemas stored in your db"""
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
			raise ValueError('Invalid schema type: '+ schema_type)
		self.cursor.execute(sql_command)
		schemas = self.cursor.fetchall()
		schemas = tuple(map(lambda x: x[0], schemas))
		return(schemas)


	# function that checks if schema_name parameter is proper
	def __schema_error_raiser(self, schema_name):
		"""check for errrors related to schema name and type."""
		if isinstance(schema_name, str)==False:
			raise TypeError('schema_name should be an instance of str')
		if schema_name not in self.list_schemas():
			raise ValueError('Invalid schema_name: '+ schema_name)
		else:
			pass


	# setting custom default schema, so you don't have to put it in each function
	def set_default_schema(self, schema_name):
		"""setting default schema you are going to use in your program."""
		self.__schema_error_raiser(schema_name)
		self.__default_shema=schema_name
		return self


	# list tables of given schema #moze zmienic na modle innych funkcji.
	def list_tables(self, schema=''):
		"""List tables stored in given schema or all database's schemas."""
		if schema == '':
			sql_command = """SELECT table_name FROM information_schema.tables;"""
		else:
			self.__schema_error_raiser(schema)
			sql_command = """SELECT table_name FROM information_schema.tables
		WHERE table_schema='"""+ schema + """';"""
		self.cursor.execute(sql_command)
		tables = self.cursor.fetchall()
		tables = tuple(map(lambda x: x[0], tables))
		return(tables)


	# function to check if 'table_name' parameter is proper
	def __table_error_raiser(self, table_name):
		"""Check for errors related to table name and type.."""
		if isinstance(table_name, str) == False:
			raise TypeError('table_name should be an instance of str')
		elif table_name not in self.list_tables():
			raise ValueError('Invalid table_name: ' + table_name)
		else:
			pass


	# get column name from given schema.table and data type that it contains. returns dictionary.
	def get_table_columns(self, table_name, schema='', dropped=False):
		"""Get details of table columns - name and stored data type"""
		schema = self.__default_shema if schema == '' else schema
		self.__schema_error_raiser(schema)
		self.__table_error_raiser(table_name)
		sql_command = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
		FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid
		AND a.attnum > 0
		WHERE  i.indrelid = '""" + schema + """.""" + table_name + """'::regclass"""
		self.cursor.execute(sql_command)
		result = self.cursor.fetchall()
		keys = ["\"" + s[0] + "\"" if any(a.isupper() for a in s[0]) \
					 else s[0] for s in result]
		values = [s[1] for s in result]
		dct = dict(zip(keys, values))
		if dropped is not True:
			k = "........pg.dropped.3........" # TO ZMIENIC ZEBY NIE BYLO SZTYWNE, TYLKO WYWALALO WSZYSTKIE POZOSTALOSCI USUNIETYCH KOLUMN
			if k in dct:
				del dct[k]

		return dct


	# function to check if '\df' parameter is proper
	def __df_error_raiser(self, df):
		"""check for error related to pandas Data Frame used to
		update/insert to data base"""
		if isinstance(df, pd.DataFrame)==False:
			raise TypeError('df_name should be an instance of pandas.DataFrame')
		else:
			pass


	# get schema.table primary key(s) name(s) and data type(s)
	def get_table_pk(self, table_name, schema_name=''):
		"""Retrieve table's primary keys details"""
		schema_name = self.__default_shema if schema_name == '' else schema_name
		self.__schema_error_raiser(schema_name)
		self.__table_error_raiser(table_name)
		sql_command = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
		FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid 
		AND a.attnum = ANY(i.indkey) 
		WHERE  i.indrelid = '"""+schema_name+"""."""+table_name+"""'::regclass
		AND    i.indisprimary;"""
		self.cursor.execute(sql_command)
		result = self.cursor.fetchall()
		keys = ["\"" + s[0] + "\"" if any(a.isupper() for a in s[0]) \
					 else s[0] for s in result]
		values = [s[1] for s in result]
		dct = dict(zip(keys, values))
		return dct


	#private function to convert types
	def __convert_table_sql_pd(self,  cursor): #funkcja do dokonczenia. obsluzyc wszystkie typy danych, jakie sie da
		"""Convert executed query to pandas DataFrame"""
		tbl_description = cursor.description
		tbl = cursor.fetchall()
		tbl = pd.DataFrame(tbl)
		types = [i[1] for i in tbl_description]
		for n, col in enumerate(tbl.columns):
			try:
				tbl[col] = tbl[col].astype(self.__type_codes[types[n]])
			except:
				tbl[col] = pd.to_datetime(tbl[col], format=self.__type_codes_date[types[n]])
		tbl.columns = [i[0] for i in tbl_description]
		return tbl


	# read all data from given table to pd.DataFrame
	def read_table(self, table_name, schema_name='', pk_as_index=False):
		"""read all data from table to pandas Data Frame"""
		schema_name = self.__default_shema if schema_name == '' else schema_name
		self.__schema_error_raiser(schema_name) #
		self.__table_error_raiser(table_name) # to i powyzsze sa powtorzone w get_table_columns, pomysl czy wywalic
		# build and execute query
		sql_query = """SELECT * FROM """+schema_name+"""."""+table_name+""";"""
		self.cursor.execute(sql_query)
		#get table description
		result = self.__convert_table_sql_pd(self.cursor)
		if pk_as_index == True:
			idx = self.get_table_pk(table_name, schema_name)
			result.set_index(list(idx.keys()), inplace=True)
		return result


	# Execute given query and return pd.DataFrame
	def read_table_from_query(self, sql_query):
		""" read table from custom query to pandas Data Frame"""
		self.cursor.execute(sql_query)
		result = self.__convert_table_sql_pd(self.cursor)
		return result


	#compare column names and data types of schema.table and given pd.DataFrame
	def compare_cols(self, df, table_name, schema_name=''):
		"""Check if columns details of df are contained in details of columns of table"""
		# check for errors
		schema_name = self.__default_shema if schema_name == '' else schema_name
		self.__schema_error_raiser(schema_name)
		self.__df_error_raiser(df)
		table_cols = self.get_table_columns(table_name, schema_name)
		table_col_names = list(table_cols.keys())
		table_pk = self.get_table_pk(table_name, schema_name)

		# check primary keys compatibility
		pk_name_condition = set(table_pk.keys()).issubset(df.columns)
		if not pk_name_condition:
			return "Compared Data Frame does not contain full set of table primary keys."
		else:
			pk_type_condition = [table_pk[k] in self.__adapt_types_pd[str(df[k].dtype)]
								 for k in list(table_pk.keys())]
			if not all(pk_type_condition):
				return "There are diffrences between data types of primary keys in DataFrame and table."

		# check column types and compatibility
		if set(df.columns)==set(table_col_names):
			result = True
		elif set(df.columns).issubset(table_col_names):
			result = "Columns of compared DataFrame and postgresql table are not fully equal." #JESLI ZMIENISZ TO, ZMIEN TEKST TAKZE W FUNKCJACH NIZEJ!!!
		else:
			result = False #i think this is not necessary

		if result or result == "Columns of compared DataFrame and postgresql table are not fully equal.":
			for col in df.columns:
				if table_cols[col] not in self.__adapt_types_pd[str(df[col].dtype)]:
					return "there are diffrences between DataFrame and table data types stored in relevant columns."
		return result


	#check if there are duplicates in df and schema.table, considering primary key
	def find_duplicates(self, df, table, schema=''):
		"""find duplicates in primary keys of df comparing to table."""
		schema = self.__default_shema if schema == '' else schema
		self.__schema_error_raiser(schema)
		self.__table_error_raiser(table)
		self.__df_error_raiser(df)
		acceptable_difference = "Columns of compared DataFrame and postgresql table are not fully equal."
		comparison = self.compare_cols(df, table, schema)
		if comparison is not True or comparison == acceptable_difference:
			raise Exception(comparison)

		#get table primary keys and erase columns in df that are not table primary keys
		p_keys = self.get_table_pk(table, schema)
		p_keys = list(p_keys.keys())
		df = df[p_keys]

		##built conditions for query
		df_len = df.shape[0]
		table_cols = ", ".join(p_keys)
		conditions_template ="""(%s)""" % ", ".join(["""%s"""] * df_len)
		conditions_sets = [col + " in " + conditions_template for col in df.columns]
		conditions_sets = ' and '.join(conditions_sets)

		#built query
		sql_command = """SELECT %s FROM %s.%s WHERE %s""" % (table_cols, schema, table, conditions_sets)

		# create values for tmp table to insert. Change NaN values to None in order to
		# properly insert NULL values
		values = list()
		for i in df.columns:
			val = df[i]
			val = [None if pd.isnull(x) else x for x in val]
			values = values + val

		values = [int(v) if isinstance(v, np.int64) else v for v in values]
		#if df.shape[1]==1:
		#	values = df[df.columns[0]]
		#	values = [None if pd.isnull(x) else x for x in values]
		#else:
		#	values = df.values.tolist()
		#	print(values)
		#	values = tuple(itertools.chain.from_iterable(values))		## TE FUNKCJE TRZEBA DOKONCZYC
		#	print(values)
		#	values = [None if pd.isnull(x) else x for x in values]

		#execute query
		self.cursor.execute(sql_command, values)
		try:
			result = self.__convert_table_sql_pd(self.cursor)
		except:
			result = pd.DataFrame()
		return result


	# WYSTAPIL JAKIS PROBLEM Z TA FUNKCJA, ZLE PRZYPISUJE PARAMETRY!! SPRAWDZIC
	#update sql table with given pd.DataFrame records ## ZASTANOWIC SIE CZY NIE ZMIENIC TAK, ZEBY WYSZUKIWALO KOLUMNY DO AKTUALIZACJI. ALE CHYBA NIE
	def update_table(self, df, table,  schema='', keep_duplicates=False):
		"""
		Update table with values passed in pandas data frame.
		"""
		#set proper schema and check for errors
		schema = self.__default_shema if schema == '' else schema
		self.__schema_error_raiser(schema)
		self.__table_error_raiser(table)
		self.__df_error_raiser(df)
		acceptable_difference = "Columns of compared DataFrame and postgresql table are not fully equal."
		comparison = self.compare_cols(df, table, schema)
		if comparison is not True or comparison == acceptable_difference:
			raise Exception(comparison)

		# get primary table keys
		p_keys = self.get_table_pk(table, schema)
		p_keys_names = list(p_keys.keys())

		# get table columns details and delete non-common columns with df
		table_columns = self.get_table_columns(table, schema)
		table_columns = {k: v for k , v in table_columns.items()
						 if k in df.columns}

		# find duplicates in df, considering table primary keys. Raise duplicate error or drop duplitaces
		if not keep_duplicates:
			duplicates = df.duplicated(subset=p_keys_names, keep=keep_duplicates)
			if any(duplicates):
				raise ValueError("duplicate key found: {0}".format(df[p_keys_names][duplicates]))
		else:
			df.drop_duplicates(subset=p_keys_names, keep=keep_duplicates, inplace=True)

		# find duplicates betweeen df and table and leave duplicates only.  NECESSARY??
		pk_to_update = self.find_duplicates(df, table, schema)
		df = pk_to_update.merge(df, how='left')
		df = df[list(table_columns.keys())]

		# prepare parameters to build sql query
		tmp_table_cols = """(%s)""" % ", ".join([k + ' ' + v for k, v in table_columns.items()])
		values_template = """(%s)""" % ", ".join(["""%s"""] * df.shape[1])
		values_sets = ", ".join([values_template] * df.shape[0])
		updated_cols_match = [x for x in table_columns.keys() if x not in p_keys_names]
		#updated_cols_match = [x for x in df.columns if x not in p_keys_names]
		updated_cols_match = ", ".join([x + " = a." + x for x in updated_cols_match])
		pk_match = " and ".join([table + "." + key + " = a."+key for key in p_keys])

		# build query and create values to insert
		sql_query = """CREATE TEMP TABLE tmp%s; 
		INSERT INTO tmp VALUES %s;
		UPDATE %s.%s
		SET %s 
		FROM tmp a
		WHERE %s;""" % (tmp_table_cols, values_sets, schema, table,
					 updated_cols_match, pk_match)

		# create values for tmp table to insert. Change NaN values to None in order to
		# properly insert NULL values
		values = df.values.tolist()
		values = tuple(itertools.chain.from_iterable(values))
		values = [None if pd.isnull(x) else x for x in values]

		# execute query and return result
		self.cursor.execute(sql_query, values)
		self.connection.commit()
		rows_updated = df.shape[0]
		result_dict = {'rows_updated': rows_updated}
		return result_dict


	#insert or insert and update pd.DataFrame to given sql table
	def insert_df(self, df, table, schema='', df_drop_duplicates=True, df_keep_duplicates='first', update_duplicates=False):
		"""
		update table with values passed in pandas data frame.
		if update_duplicates is False, function ignores
		duplicates between df and table
		"""
		#set proper schema and check for errors
		schema = self.__default_shema if schema == '' else schema
		self.__schema_error_raiser(schema)
		self.__table_error_raiser(table)
		self.__df_error_raiser(df)
		if not isinstance(df_drop_duplicates, bool):
			raise TypeError('df_drop_duplicates has to be an instance of bool')
		if not isinstance(update_duplicates, bool):
			raise TypeError('update_duplicates has to be an instance of bool')
		if df_keep_duplicates not in ('first', 'last', False):
			raise ValueError('df_keep_duplicates value has to be \'first\', \'last\' or False')
		comparison = self.compare_cols(df, table, schema)
		if comparison is not True:
			raise Exception(comparison)

		#declare result variables
		rows_updated = 0
		rows_inserted = 0

		# get primary table keys
		p_keys = self.get_table_pk(table, schema)
		p_keys_names = list(p_keys.keys())

		# find duplicates in df, considering table primary keys. Raise duplicate error or drop duplitaces
		duplicates = df.duplicated(subset=p_keys_names, keep=False)
		if any(duplicates):
			if df_drop_duplicates:
				df.drop_duplicates(subset=p_keys_names, keep=df_keep_duplicates, inplace=True)
			else:
				raise ValueError("duplicate primary key(s) found: {0}".format(df[p_keys_names][duplicates]))

		# find duplicates between df and table. Erase duplicates from data frame to insert
		# and optionally update duplicated rows in table
		pk_duplicates = self.find_duplicates(df, table, schema)
		if pk_duplicates.shape[0] != 0:
			df_to_update = pk_duplicates.merge(df, on=p_keys_names, how='left')
			df_to_insert = pd.concat([df, df_to_update]) #try to find one-line solution
			df_to_insert.drop_duplicates(keep=False, inplace=True)
			if update_duplicates:
				rows_updated = self.update_table(df_to_update,
												  table, schema)['rows_updated']
		else:
			df_to_insert = df

		#if df_to_insert is emptythen  return result
		if df_to_insert.shape[0] == 0:
			return {'rows_inserted': rows_inserted, 'rows_updated': rows_updated}

		# prepare parameters to build sql query and sort df to match table columns position
		table_columns = self.get_table_columns(table, schema)
		df_to_insert = df_to_insert[list(table_columns.keys())]
		table_columns = ", ".join(list(table_columns.keys()))
		print(table_columns)
		values_template = """(%s)""" % ", ".join(["""%s"""] * df_to_insert.shape[1])
		values_template = ", ".join([values_template] * df_to_insert.shape[0])

		#build query
		sql_query = """INSERT INTO %s.%s (%s) VALUES %s;""" %(schema, table,
																   table_columns,
																   values_template)

		# create values for query to insert
		values = df_to_insert.values.tolist() # ZROBIC Z TEGO FUNKCJE MOZE
		values = tuple(itertools.chain.from_iterable(values))
		values = [None if pd.isnull(x) else x for x in values]
		# execute query and return result
		self.cursor.execute(sql_query, values)
		self.connection.commit()
		rows_inserted = df_to_insert.shape[0]
		result = {'rows_inserted': rows_inserted, 'rows_updated': rows_updated}
		return result
