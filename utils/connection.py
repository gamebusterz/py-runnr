import psycopg2_binary

class DWConnection(object):
	def __init__(self, connection_string,logger = None):
		self.connection = None
		self.connection_string = connection_string
		self.logger = logger

	def __enter__(self):
		self.connection = self.create_conn(self.connection_string)
		if self.logger:
			self.logger.info('DW CONNECTION OPENED!')
		return self.connection

	def __exit__(self, type, value, traceback):
		self.connection.commit()
		self.connection.close()
		if self.logger:
			self.logger.info('DW CONNECTION CLOSED.')

	def create_conn(self, connection_string):
		connection = psycopg2_binary.connect(connection_string)
		return connection