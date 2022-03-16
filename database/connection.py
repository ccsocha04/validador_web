import arcpy
import os
import cx_Oracle as cx
import sqlalchemy
import pandas as pd

# Esto no puede quedar en git.
user = os.environ.get('USER')
password = os.getenv('PASSWORD')
ip = os.getenv('IP')
port = os.getenv('PORT')
db = os.getenv('DB')

arcpy.AddMessage(f'0. Database connection user: {user}')
schema = 'MJEREZ'
dsn = f'{ip}:{port}/{db}'
connection_string = f'oracle+cx_oracle://{user}:{password}@{ip}:{port}/?service_name={db}'
# TODO esto debe ser un pooling connection https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html
# seccion Connection Pooling
con = cx.connect(user, password, dsn)
con.autocommit = True

engine = sqlalchemy.create_engine(connection_string, arraysize=1000)
ruta = r'E:\VWMDG\validador_web\Data\IDO-08061_202203071.gdb'


def pd_upper_columns(sql: str, connection):
    # TODO check connection
    df = pd.read_sql_query(sql, connection)
    df.columns = map(str.upper, df.columns)
    return df

