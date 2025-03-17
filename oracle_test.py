import cx_Oracle

dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XEPDB1")
connection = cx_Oracle.connect(user="system", password="nabakallolghosh", dsn=dsn)

cursor = connection.cursor()
cursor.execute("SELECT 'Connected to Oracle' FROM dual")
print(cursor.fetchone()[0])

cursor.close()
connection.close()
