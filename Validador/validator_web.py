import arcpy
import os
import sys
from importlib_metadata import version
import pandas as pd
import requests


from importlib.resources import path
from typing import List, Dict, Tuple
from zipfile import ZipFile
from utils.utils import (conversion_format, set_workspace, valw_gdb_mensaje, 
    valw_dom_validadores, valw_srs, valw_version)
from database.connection import pd_upper_columns, schema


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
    datasets = []
    dataset_codes = []
    for dirpath, dirnames, _ in walk_gds:
        for gds in dirnames:
            gds_crs = arcpy.Describe(os.path.join(dirpath, gds)).spatialReference.GCSCode
            datasets.append(gds)
            dataset_codes.append(str(gds_crs))
    return pd.DataFrame({'DATASETS': datasets, 'SRSCODES': dataset_codes})

def get_feature_classes():
    """
    Get all feature classes
    """
    walk_fcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for _, dirnames, fnames in walk_fcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    return pd.DataFrame({'FEATURES': feature_classes})

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
    
    return pd.DataFrame({'TABLAS': tables})

def left_join_ds_version_ds_validacion(
    ds_version: pd.DataFrame,
    ds_validacion: pd.DataFrame
)-> pd.DataFrame:
    df_comp = ds_version[['DS_NOMBRE', 'SRSCODE']]
    return df_comp.merge(ds_validacion, 
        how='left', 
        left_on='DS_NOMBRE', 
        right_on='DATASETS')
    
def reference_system( 
    version:str ,engine, id, ds_version: Dict[str, List[str]], ds_validacion: List[str]) -> pd.DataFrame:
    """
    Reference System
    Args:
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    """
    id_validador = _id_validador(engine=engine, validador=valw_dom_validadores.srs)
    codigo, nombre = _get_srs(connection=engine, version=version)
    # TODO cuidado con esto
    codigo = int(codigo)
    version_vs_gdb = left_join_ds_version_ds_validacion(
        ds_version=ds_version,
        ds_validacion=ds_validacion) 

    # verificar sistema de referencia incorrecto
    srs_incorrecto = version_vs_gdb[
        (version_vs_gdb['SRSCODE'] != version_vs_gdb['SRSCODES']) 
        & 
        (version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])
        ].copy()
    if len(srs_incorrecto)>0:
        srs_incorrecto[valw_gdb_mensaje.mensaje_column] = srs_incorrecto.apply(lambda x: 
            f'Sistema de referencia, incorrecto el sistema debe ser {x["DS_NOMBRE"]} -> {x["SRSCODE"]}', axis=1)
        srs_incorrecto[valw_gdb_mensaje.bool_column] = 0

    # verificar sistemas de referencia correcto
    srs_correcto = version_vs_gdb[
        (version_vs_gdb['SRSCODE'] == version_vs_gdb['SRSCODES']) 
        & 
        (version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])
        ].copy()
    if len(srs_correcto)>0:
        srs_correcto[valw_gdb_mensaje.mensaje_column] = srs_correcto.apply(lambda x: 
            f'Sistema de referencia correcto {x["DS_NOMBRE"]} -> {x["SRSCODE"]}', axis=1)
        srs_correcto[valw_gdb_mensaje.bool_column] = 1

    final = pd.concat([srs_incorrecto, srs_correcto])
    final[valw_gdb_mensaje.gdb_id_column] = id
    final[valw_gdb_mensaje.validador_id] = id_validador
    return  final[[
        valw_gdb_mensaje.gdb_id_column, 
        valw_gdb_mensaje.validador_id, 
        valw_gdb_mensaje.mensaje_column,
        valw_gdb_mensaje.bool_column]].copy()  

def _get_srs(connection, version: str)-> Tuple[str, str]:
    """
    Obtiene el código y el sistema de referencia utilizado para una versión específica.
    Args:
        conn : Una conexión a la base de datos.
        version str: la versión de la validación.
    Returns:
        codigo, nombre Tuple[str, str]: codigo y nombre del sistema de referencia de validación.    
    """
    sql = f"""SELECT {valw_srs.srscode}, {valw_srs.nombre} FROM {schema}.{valw_srs.table_name} SRS
            INNER JOIN {schema}.{valw_version.table_name} VER ON  VER.{valw_version.id} 
            = SRS.{valw_srs.version_id} WHERE VER.{valw_version.version} = {version}"""
    df = pd_upper_columns(sql, connection)
    df.reset_index(inplace=True)
    srs_code = df[valw_srs.srscode][0]
    nombre = df[valw_srs.nombre][0]

    return srs_code, nombre

def _insert_mensaje(connection, sql: str, id_gdb: int, mensaje: str, id_validador:int)->None:
    # TODO esto debería arrojar excepciones.
    with connection.cursor() as cur:
        cur.execute(sql, [id_gdb, mensaje, id_validador])

def _id_validador(engine ,validador:str)->int:
    """
    Retorna el ID de la tabla VALW_DOM_VALIDADORES.
    Args:
        validador: str , Campo DESCRIPCIÓN de la tabla VALW_DOM_VALIDADORES

    RETURNS:
        int: el ID de la DESCRIPCIÓN.
    """
    sql = f"""SELECT {valw_dom_validadores.id}, {valw_dom_validadores.descripcion} 
            FROM {schema}.{valw_dom_validadores.table_name}"""
    df = pd_upper_columns(sql, engine)
    return df[(df[valw_dom_validadores.descripcion] == validador)].reset_index()['ID'][0]
        


def quantity_dataset(engine, id, ds_version:pd.DataFrame, ds_validacion:pd.DataFrame) -> pd.DataFrame:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a datasets.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    Returns:
        pd.DataFrame
    """
    arcpy.AddMessage("6. Quantity of datasets...")
    arcpy.AddMessage("Verificación dataset")
    
    id_validador = _id_validador(engine, validador=valw_dom_validadores.datasets)
    
    version_vs_gdb = left_join_ds_version_ds_validacion(
        ds_version=ds_version,
        ds_validacion=ds_validacion)
    version_vs_gdb = version_vs_gdb[['DS_NOMBRE', 'DATASETS']].copy()

    ausentes = pd.DataFrame()
    # verificar si existen datasets ausentes.
    if version_vs_gdb.isna().sum().sum() > 0 :
        ausentes = version_vs_gdb[
            (version_vs_gdb['DS_NOMBRE'] != version_vs_gdb['DATASETS'])
            ].copy()
        if len(ausentes) > 0:
            ausentes[valw_gdb_mensaje.mensaje_column] = ausentes['DS_NOMBRE'].\
                apply(lambda x : f'Dataset del MDG faltante -> {x}')
            ausentes[valw_gdb_mensaje.bool_column] = 0
    
    # verificar datasets correctos
    presentes = version_vs_gdb[
        (version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])
        ].copy()
    if len(presentes) > 0:
        presentes[valw_gdb_mensaje.mensaje_column] = presentes['DS_NOMBRE'].\
            apply(lambda x : f'Dataset correcto -> {x}')
        presentes[valw_gdb_mensaje.bool_column] = 1

    # verificar datasets sobrantes
    
    gdb_vs_version_ds = ds_validacion.merge(
        ds_version, 
        how='left', 
        left_on='DATASETS', 
        right_on='DS_NOMBRE')[['DS_NOMBRE', 'DATASETS']].copy()
    if len(gdb_vs_version_ds) > 0:
        ds_adicionales = gdb_vs_version_ds[
            (gdb_vs_version_ds['DS_NOMBRE'] != gdb_vs_version_ds['DATASETS'])
            ].copy()
        ds_adicionales[valw_gdb_mensaje.mensaje_column] = ds_adicionales['DATASETS'].\
            apply(lambda x: f'Dataset no incluido en el MDG -> {x}')
        ds_adicionales[valw_gdb_mensaje.gdb_id_column] = id
        ds_adicionales[valw_gdb_mensaje.validador_id] = id_validador
        ds_adicionales[valw_gdb_mensaje.bool_column] = 0
        ds_adicionales = ds_adicionales[[
            valw_gdb_mensaje.gdb_id_column,
            valw_gdb_mensaje.validador_id,
            valw_gdb_mensaje.mensaje_column,
            valw_gdb_mensaje.bool_column]].copy()
    
    final = pd.concat([ausentes, presentes])
    final[valw_gdb_mensaje.gdb_id_column] = id
    final[valw_gdb_mensaje.validador_id] = id_validador
    final = final[[
        valw_gdb_mensaje.gdb_id_column,
        valw_gdb_mensaje.validador_id,
        valw_gdb_mensaje.mensaje_column,
        valw_gdb_mensaje.bool_column]].copy()
    return pd.concat([final, ds_adicionales])
    

def quantity_feature_class(engine, id, fc_version, fc_gdb) -> pd.DataFrame:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a feature classes.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Feature Classes de la versión
        gds Dict[str, List[str]]: Feature Classes de la gdb a ser comprobada.
    Returns:
        pd.DataFrame
    """
    id_validador = _id_validador(engine=engine, validador=valw_dom_validadores.features)

    ausentes_version_gdb = fc_version.set_index('NOMBRE_OBJETO').index.difference(fc_gdb.set_index('FEATURES').index)
    adicionales_gdb_version = fc_gdb.set_index('FEATURES').index.difference(fc_version.set_index('NOMBRE_OBJETO').index)
    correctos = fc_version.set_index('NOMBRE_OBJETO').index.intersection(fc_gdb.set_index('FEATURES').index)

    ausentes_version_gdb = ausentes_version_gdb.to_frame(index=False, name='FEATURE')
    adicionales_gdb_version = adicionales_gdb_version.to_frame(index=False, name='FEATURE')
    correctos = correctos.to_frame(index=False, name='FEATURE')

    if len(ausentes_version_gdb) > 0:
        ausentes_version_gdb[valw_gdb_mensaje.mensaje_column] = ausentes_version_gdb['FEATURE'].\
            apply(lambda x: f'Feature class del MDG faltante -> {x}')
        ausentes_version_gdb[valw_gdb_mensaje.bool_column] = 0
    
    if len(adicionales_gdb_version) > 0:
        adicionales_gdb_version[valw_gdb_mensaje.mensaje_column] = adicionales_gdb_version['FEATURE'].\
            apply(lambda x: f'Feature class no incluido en el MDG -> {x}')
        adicionales_gdb_version[valw_gdb_mensaje.bool_column] = 0
        
    if len(correctos) > 0:
        correctos[valw_gdb_mensaje.mensaje_column] = correctos['FEATURE'].\
            apply(lambda x: f'Feature class correcto -> {x}')
        correctos[valw_gdb_mensaje.bool_column] = 1

    final = pd.concat([ausentes_version_gdb, adicionales_gdb_version, correctos])
    final[valw_gdb_mensaje.gdb_id_column] = id
    final[valw_gdb_mensaje.validador_id] = id_validador
    return final[[
        valw_gdb_mensaje.gdb_id_column,
        valw_gdb_mensaje.validador_id,
        valw_gdb_mensaje.mensaje_column, 
        valw_gdb_mensaje.bool_column]].copy()

def quantity_tables(engine, id, tbl_version, tbl_gdb) -> pd.DataFrame:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a tablas.
    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvtbl Dict[str, List[str]]: Tables de la versión
        gtbl Dict[str, List[str]]: Tables de la gdb a ser comprobada.
    Returns:
        pd.DataFrame
    """
    arcpy.AddMessage("8. Quantity of tables...")
    arcpy.AddMessage("Verificación tablas")
    id_validador = _id_validador(engine=engine, validador=valw_dom_validadores.tables)

    ausentes_version_gdb = tbl_version.set_index('FICHAX').index.difference(tbl_gdb.set_index('TABLAS').index)
    adicionales_gdb_version = tbl_gdb.set_index('TABLAS').index.difference(tbl_version.set_index('FICHAX').index)
    correctos = tbl_version.set_index('FICHAX').index.intersection(tbl_gdb.set_index('TABLAS').index)

    ausentes_version_gdb = ausentes_version_gdb.to_frame(index=False, name='TABLA')
    adicionales_gdb_version = adicionales_gdb_version.to_frame(index=False, name='TABLA')
    correctos = correctos.to_frame(index=False, name='TABLA')

    if len(ausentes_version_gdb) > 0:
        ausentes_version_gdb[valw_gdb_mensaje.mensaje_column] = ausentes_version_gdb['TABLA'].\
            apply(lambda x: f'Tabla o ficha del MDG faltante -> {x}')
        ausentes_version_gdb[valw_gdb_mensaje.bool_column] = 0
    
    if len(adicionales_gdb_version) > 0:
        adicionales_gdb_version[valw_gdb_mensaje.mensaje_column] = adicionales_gdb_version['TABLA'].\
            apply(lambda x: f'Tabla o ficha no incluido en el MDG -> {x}')
        adicionales_gdb_version[valw_gdb_mensaje.bool_column] = 0

    if len(correctos) > 0:
        correctos[valw_gdb_mensaje.mensaje_column] = correctos['TABLA'].\
            apply(lambda x: f'Tabla o ficha correcta -> {x}')
        correctos[valw_gdb_mensaje.bool_column] = 1

    final = pd.concat([ausentes_version_gdb, adicionales_gdb_version, correctos])
    final[valw_gdb_mensaje.gdb_id_column] = id
    final[valw_gdb_mensaje.validador_id] = id_validador
    return final[[
        valw_gdb_mensaje.gdb_id_column,
        valw_gdb_mensaje.validador_id,
        valw_gdb_mensaje.mensaje_column,
        valw_gdb_mensaje.bool_column]].copy()
    

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

