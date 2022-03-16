import pandas as pd
from typing import Tuple

from pydantic import ValidationError
from database.connection import pd_upper_columns

def por_validar (connection) -> int:
    """
    Consulta en la base de datos la cantidad de gdbs que faltan por validar

    Args:
        connection: conexión a la base de datos

    Returns:
        int: indicando la cantidad de gbs que faltan por validar.
    """
    with connection.cursor() as cur:
        row = cur.execute("""SELECT count(*) 
            FROM MJEREZ.VALW_GDBS_VALIDAR vgds 
            INNER JOIN MJEREZ.VALW_ESTADO_PROCESO vep 
                ON vgds.ESTADO_ID = vep.ID_ESTADO_PROCESO 
            WHERE vep.ESTADO ='Sin iniciar'""")
        return row.rowcount


def gdb_para_validar(connection, gdb: str) -> Tuple[str, str]:
    """
    Devuelve el path de la ubicación de la gdb.

    Args:
        conecction : conexión a la base de datos.
        gdb str: path de la base de datos.
    Returns:
        id, path: Tuple[str, str] tupla de la base de datos


    #TODO si no existe se busca el siguiente en cuyo caso debe haber un estado
    que diga error, o algo. en la tabla VALW_ESTADO_PROCESO.

    # TODO en caso de que no la entre debe arrojar un mensaje claro.
    """
    try: 
        sql = f"""
            SELECT ID, RUTA FROM MJEREZ.VALW_GDBS_VALIDAR  gdb_val
            WHERE RUTA = '{gdb}'
            """
        df = pd_upper_columns(sql, connection)
        if df.shape[0] > 1:
            raise ValidationError('No pueden existir dos registros iguales de gdb en la base de datos. Revise la unicidad.')
        return int(df.loc[0, 'ID']), df.loc[0,'RUTA']
    except Exception:
        raise TypeError('No se encuentra el path que está intentando validar en la base de datos.')

def update_estado(connection, id: int, estado:str) -> None:
    """
    Actualiza el estado de la base de datos
    """
    sql = """
    UPDATE MJEREZ.VALW_GDBS_VALIDAR SET ESTADO_ID = :estado WHERE ID = :id_bd
    """
    id_estado = get_id_estado(connection, estado)
    with connection.cursor() as cur:
        cur.execute(sql, estado=id_estado, id_bd=id)

def get_id_estado(connection, estado:str) -> int:
    sql = """
    SELECT ID_ESTADO_PROCESO FROM MJEREZ.VALW_ESTADO_PROCESO WHERE ESTADO = :estado
    """
    with connection.cursor() as cur:
        return cur.execute(sql, estado=estado).fetchone()[0]

def borrar_registros_mensajes(connection, id) -> None:
    """
    En caso de que la gdb ya haya sido valida borra los mensajes en la base de datos.
    """
    sql = """DELETE FROM MJEREZ.VALW_GDB_MENSAJE WHERE GDB_ID = :id_bd"""
    with connection.cursor() as cur:
        cur.execute(sql, id_bd=id)
    