import arcpy
import os
import cx_Oracle as cx

# Esto no puede quedar en git.
user = os.environ.get('USER')
password = os.getenv('PASSWORD')
ip = os.getenv('IP')
port = os.getenv('PORT')
db = os.getenv('DB')

arcpy.AddMessage(f'0. Database connection user: {user}')
dsn = f'{ip}:{port}/{db}'

# TODO esto debe ser un pooling connection https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html
# seccion Connection Pooling
con = cx.connect(user, password, dsn)
con.autocommit = True
