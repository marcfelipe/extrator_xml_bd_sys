import fdb

def conectar_db(host, database='c:/syspdv/syspdv_srv.fdb'):
    connection = fdb.connect(host=host,
                      database=database,
                      user='SYSDBA',
                      password='masterkey')
    return connection