import arcpy
import os
import sys
import pandas as pd
import requests


from importlib.resources import path
from typing import List, Dict, Tuple
from zipfile import ZipFile
from utils.utils import conversion_format, set_workspace


def extract_files(file_path: str, extract_path: str):
    """
    Extract all files from a zip file
    """
    arcpy.AddMessage("2. Extracting files...")
    with ZipFile(file_path, "r") as zip:
        file_name = os.path.basename(zip.filename).split(".")[0]
        file_format = os.path.basename(zip.filename).split(".")[1]
        file_date = file_name.split("_")[1]

        zip.extractall(path=extract_path)

        if file_format != "gdb":
            arcpy.AddWarning("File format is not gdb")    
            list_files = zip.namelist()
            for file in list_files:
                arcpy.AddMessage(F"2.1 File name: {file.split('.')[0]}")
                arcpy.AddMessage(F"2.2 File format: {file.split('.')[1]}")
                arcpy.AddMessage(F"2.3 File date: {file_date}")
            conversion_format(F"{extract_path}\{file}")
        else:
            arcpy.AddMessage(F"2.1 File name: {file_name}")
            arcpy.AddMessage(F"2.2 File format: {file_format}")
            arcpy.AddMessage(F"2.3 File date: {file_date}")
            set_workspace(F"{extract_path}\{file_name}.gdb")

def spatial_matching(connection, id, exp) -> None:
    """
    Spatial matching
    """
    """
    Persiste la información de la correspondencia espacial entre el póligono del servicio web geográfico y el feature class cargado en la GDB.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        exp: código del expediente.
    Returns:
        None
    """
    arcpy.AddMessage("4. Spatial matching...")
    arcpy.AddMessage("Verificación coincidencia espacial")

    id_validador = _id_validador(connection, validador='ESPACIAL')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""
    
    url_service_anm = f"https://geo.anm.gov.co/webgis/services/ANM/ServiciosANM/MapServer/WFSServer?service=WFS&version=2.0.0&request=GetFeature&typeName=Titulo_Vigente&PropertyName=CODIGO_EXPEDIENTE,FECHA_DE_INSCRIPCION,ESTADO,MODALIDAD,ETAPA,NOMBRE_DE_TITULAR,Shape&Filter=<ogc:Filter><ogc:PropertyIsEqualTo><ogc:PropertyName>CODIGO_EXPEDIENTE</ogc:PropertyName><ogc:Literal>{exp}</ogc:Literal></ogc:PropertyIsEqualTo></ogc:Filter>&outputformat=ESRIGEOJSON"
    geojson_service_anm = requests.get(url_service_anm).json()
    
    mining_title = arcpy.AsShape(geojson_service_anm, True)
    delimit_proyect_pg = arcpy.MakeFeatureLayer_management(os.path.join(arcpy.env.workspace, "TOPOGRAFIA_LOCAL", "DELIMIT_PROYEC_PG"))
    select_location = arcpy.SelectLayerByLocation_management(delimit_proyect_pg, "ARE_IDENTICAL_TO", mining_title, None, "NEW_SELECTION", "NOT_INVERT")

    if int(arcpy.GetCount_management(select_location)[0]) > 0:
        mensaje = f'La delimitación del título minero coincide con el polígono estructurado en AnnA Minería -> DELIMIT_PROYEC_PG'
        _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
        arcpy.AddMessage(mensaje)

    else:
        mensaje = f'La delimitación del título minero no coincide con el polígono estructurado en AnnA Minería -> DELIMIT_PROYEC_PG'
        _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
        arcpy.AddError(mensaje)

def get_feature_datasets():
    """
    Get all feature datasets in a geodatabase
    """
    walk_gds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    feature_datasets_dict = dict()
    for dirpath, dirnames, fnames in walk_gds:
        for gds in dirnames:
            gds_crs = arcpy.Describe(os.path.join(dirpath, gds)).spatialReference.GCSCode
            feature_datasets_dict[gds] = gds_crs
    return feature_datasets_dict

def get_feature_classes():
    """
    Get all feature classes
    """
    walk_fcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for dirpath, dirnames, fnames in walk_fcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    return feature_classes

def get_tables():
    """
    Get all tables
    """
    walk_tables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_tables:
        for fc in fnames:
            if not fc.endswith('__ATTACH'):
                tables.append(fc)
    
    return tables
    
def reference_system(connection, id, ds_version: Dict[str, List[str]], ds_validacion: List[str]):
    """
    Reference System
    Args:
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada
    """
    
    arcpy.AddMessage("5. Reference system...")
    arcpy.AddMessage("Verificación sistema de referencia")
    
    count_errors= 0
    id_validador = _id_validador(connection, validador='SISTEMA DE REFERENCIA')
    version = '1'
    codigo, nombre = _get_srs(connection=connection, version=version)
    # TODO cuidado con esto
    codigo = int(codigo)
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""
    
    for key_gds, value_gds in ds_validacion.items():
        if key_gds in ds_version:
            if value_gds == codigo:
                mensaje = f'Sistema de referencia correcto {ds_version[key_gds][1]} (EPSG: {ds_version[key_gds][0]}) -> {key_gds}'
                arcpy.AddMessage(mensaje)
            else:
                count_errors+= 1
                mensaje = f'Sistema de referencia, incorrecto el sistema debe ser {ds_version[key_gds][1]} (EPSG: {ds_version[key_gds][0]}) -> {key_gds}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb=id, mensaje=mensaje, id_validador=id_validador)    
    
    arcpy.AddMessage(F"Errores encontrados: {count_errors}")

def _get_srs(connection, version: str)-> Tuple[str, str]:
    """
    Obtiene el código y el sistema de referencia utilizado para una versión específica.
    Args:
        conn : Una conexión a la base de datos.
        version str: la versión de la validación.
    Returns:
        codigo, nombre Tuple[str, str]: codigo y nombre del sistema de referencia de validación.    
    """
    with connection.cursor() as cur:
        return cur.execute("""SELECT SRSCODE, NOMBRE FROM MJEREZ.VALW_SRS SRS
            INNER JOIN MJEREZ.VALW_VERSION VER ON  VER.ID = SRS.VERSION_ID WHERE VER.VERSION = :version""", version=version).fetchone()

def _insert_mensaje(connection, sql: str, id_gdb: int, mensaje: str, id_validador:int)->None:
    # TODO esto debería arrojar excepciones.
    with connection.cursor() as cur:
        cur.execute(sql, [id_gdb, mensaje, id_validador])

def _id_validador(connection, validador:str)->int:
    """
    Retorna el ID de la tabla VALW_DOM_VALIDADORES.
    Args:
        validador: str , Campo DESCRIPCIÓN de la tabla VALW_DOM_VALIDADORES

    RETURNS:
        int: el ID de la DESCRIPCIÓN.
    """
    with connection.cursor() as cur:
        return cur.execute("""SELECT ID FROM MJEREZ.VALW_DOM_VALIDADORES 
            WHERE DESCRIPCION = :validador""", validador=validador).fetchone()[0]

def quantity_dataset(connection, id, gvds, gds) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a datasets.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("6. Quantity of datasets...")
    arcpy.AddMessage("Verificación dataset")
    
    count_errors= 0
    id_validador = _id_validador(connection, validador='DATASETS')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""

    for key_gvds in gvds.items():
        if key_gvds[0] in gds:
            mensaje = f'Dataset correcto -> {key_gvds[0]}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddMessage(mensaje)
        else:
            count_errors+= 1
            mensaje = f'Dataset del MDG faltante -> {key_gvds[0]}'
            with connection.cursor() as cur:
                _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    for key_gds, _ in gds.items():
        if not key_gds in gvds:
            count_errors+=1
            mensaje = f"Dataset no incluido en el MDG -> {key_gds}"
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)
    
    arcpy.AddMessage(f"Errores encontrados: {count_errors}")

def quantity_feature_class(connection, id, gvfc, gfc) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a feature classes.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Feature Classes de la versión
        gds Dict[str, List[str]]: Feature Classes de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("7. Quantity of feature classes...")
    arcpy.AddMessage("Verificación feature class")
    
    count_errors= 0
    id_validador = _id_validador(connection=connection, validador='FEATURE CLASSES')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""

    for vfc in gvfc:
        if vfc in gfc:
            mensaje = f'Feature class correcto -> {vfc}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddMessage(mensaje)
        else:
            count_errors+= 1
            mensaje = f'Feature class del MDG faltante -> {vfc}'
            with connection.cursor() as cur:
                _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    for fc in gfc:
        if not fc in gvfc:
            count_errors+= 1
            mensaje = f'Feature class no incluido en el MDG -> {fc}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    arcpy.AddMessage(f"Errores encontrados: {count_errors}")

def quantity_tables(connection, id, gvtbl, gtbl) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a tablas.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvtbl Dict[str, List[str]]: Tables de la versión
        gtbl Dict[str, List[str]]: Tables de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("8. Quantity of tables...")
    arcpy.AddMessage("Verificación tablas")

    count_errors= 0
    id_validador = _id_validador(connection=connection, validador='TABLAS')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""

    for vtbl in gvtbl:
        if vtbl in gtbl:
            mensaje = f'Tabla o ficha correcta -> {vtbl}'
            arcpy.AddMessage(f"Tabla o ficha correcta -> {vtbl}")
        else:
            count_errors+= 1
            mensaje = f'Tabla o ficha del MDG faltante -> {vtbl}'
            arcpy.AddError(mensaje)
        _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)

    for tbl in gtbl:
        if tbl not in gvtbl:
            count_errors+= 1
            mensaje = f'Tabla o ficha no incluido en el MDG -> {tbl}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    arcpy.AddMessage(f"Errores encontrados: {count_errors}")

def quantity_required(connection, id, gvreq, greq) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a requerimientos.

    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvreq Dict[str, List[str]]: Feature Classes obligatorios de la versión
        greq Dict[str, List[str]]: Feature Classes obligatorios de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("9. Quantity of required objects...")
    arcpy.AddMessage("Verificación obligatoriedad objetos")

    count_errors= 0
    id_validador = _id_validador(connection=connection, validador='OBLIGATORIEDAD')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""

    for vreq in gvreq:
        if vreq in greq:
            mensaje = f'Feature class cumple con la Tabla de Obligatoriedad -> {vreq}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddMessage(mensaje)
        else:
            count_errors+= 1
            mensaje = f'Feature class obligatorio faltante según la Tabla de Obligatoriedad -> {vreq}'
            with connection.cursor() as cur:
                _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    arcpy.AddMessage(f"Errores encontrados: {count_errors}")

def feature_attributes(connection, id, gvreq, greq) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a requerimientos.

    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvreq Dict[str, List[str]]: Feature Classes obligatorios de la versión
        greq Dict[str, List[str]]: Feature Classes obligatorios de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("10. Feature attributes of the objects...")
    arcpy.AddMessage("Verificación características atributivas de los objetos")

    count_errors= 0
    id_validador = _id_validador(connection=connection, validador='ATRIBUTOS')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""

    #a GEOMETRIA -> OBJETOS_ATRIBUTOS
    #b CHECK GEOMETRY -> GEOPROCESO
    #c COD_ID_ATRIBUTO -> ATRIBUTO
    #d COD_EXPEDIENTE -> ATRIBUTO
    #e ATRIBUTOS OBLIGATORIOS -> OBJETOS_ATRIBUTOS
    #f FIELD NAME -> OBJETOS_ATRIBUTOS
    #g FIELD NAME ALIAS -> OBJETOS_ATRIBUTOS
    #h FIELD TYPE -> OBJETOS_ATRIBUTOS
    #i FIELD LENGTH -> OBJETOS_ATRIBUTOS
    #j DOMAIN -> OBJETOS_ATRIBUTOS
    #k VALIDATE DOMAIN -> OBJETOS_ATRIBUTOS

