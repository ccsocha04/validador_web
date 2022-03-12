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
    """
    sql = """
        SELECT ID, RUTA FROM MJEREZ.VALW_GDBS_VALIDAR  gdb_val
        WHERE RUTA = :ruta
        """
    # TODO en caso de que no se actualice hay un TypeError.
    with connection.cursor() as cur:
        row = cur.execute(sql, ruta=gdb).fetchone()
        id, ruta = row
        return id, ruta

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
    