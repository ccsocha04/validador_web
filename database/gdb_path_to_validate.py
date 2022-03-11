from typing import Tuple


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


def gdb_para_validar(connection, gdb) -> Tuple[str, str]:
    """
    Devuelve el path de la ubicación de la gdb.

    Args:
        conecction: conexión a la base de datos.
        gdb: path de la base de datos.
    Returns:
        id, path: Tuple[str, str] tupla de la base de datos


    #TODO si no existe se busca el siguiente en cuyo caso debe haber un estado
    que diga error, o algo. en la tabla VALW_ESTADO_PROCESO.
    """
    sql = """
        SELECT ID, RUTA FROM MJEREZ.VALW_GDBS_VALIDAR  gdb_val
        INNER JOIN MJEREZ.VALW_ESTADO_PROCESO vep ON gdb_val.ESTADO_ID =vep.ID_ESTADO_PROCESO 
        WHERE vep.ESTADO ='Sin iniciar' AND RUTA = :ruta
        """
    # TODO en caso de que no se actualice hay un TypeError.
    with connection.cursor() as cur:
        row = cur.execute(sql, ruta=gdb).fetchone()
        id, ruta = row
        return id, ruta

def update_estado(connection, id) -> None:
    """
    Actualiza el estado de la base de datos
    """
    sql = """
    UPDATE MJEREZ.VALW_GDBS_VALIDAR SET ESTADO_ID = 1 WHERE ID = :id_bd
    """
    with connection.cursor() as cur:
        cur.execute(sql, id_bd=id)
