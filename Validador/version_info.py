import arcpy
import os
import sys
import pandas as pd


from importlib.resources import path
from typing import List, Dict
from zipfile import ZipFile

from database.connection import pd_upper_columns

def get_version_datasets(connection) -> Dict[str, List[str]]:
    """
    Get all feature datasets in a geodatabase

    """
    arcpy.AddMessage("1. Getting version ...")
    sql = f"""SELECT DS.NOMBRE AS DS_NOMBRE, DS.VERSION_ID, SRSCODE, SRS.NOMBRE AS SRS_NOMBRE, OBLIGATORIO
        FROM MJEREZ.VALW_DATASETS DS 
        INNER JOIN MJEREZ.VALW_SRS SRS 
            ON DS.SRS_ID = SRS.ID

        """
    df = pd_upper_columns(sql, connection=connection)   
    arcpy.AddMessage(F"1.1 Version_Datasets: {df.shape[0]}")
    return df


def get_version_feature_classes(connection) -> List[str]:
    """
    Get all feature classes in a geodatabase
    """
    sql = """SELECT GRUPO_DATASET, NOMBRE_OBJETO 
        FROM VALW_OBJETOS_TOTALES vot"""
    df = pd_upper_columns(sql, connection=connection)
    arcpy.AddMessage(F"1.2 Version_Feature_Classes: {df.shape[0]}")
    
    return df

def get_version_tables(connection) -> List[str]:
    """
    Get all tables in a geodatabase

    This function is used to get the base tables it would be better to read this from DB
    """
    sql = f"""SELECT DISTINCT FICHAX 
        FROM VALW_VALIDAR_TABLAS"""
    df = pd_upper_columns(sql, connection=connection)
    
    arcpy.AddMessage(F"1.3 Version_Tables: {df.shape[0]}")
    
    return df

def get_version_required(connection, document, stage) -> List[str]:
    """
    Get all required feature classes required in a geodatabase

    This function is used to get the base required feature classes it would be better to read this from DB
    """
    required_fc = []
    with connection.cursor() as cur:
        for row in cur.execute("""SELECT voo.NOMBRE_CAPA FROM VALW_OBJ_OBLIGATORIOS voo WHERE voo.DOCUMENTO_TECNICO = :document AND voo.ETAPA = :stage""", document=document, stage=stage):
            required_fc.append(row[0])
    
    arcpy.AddMessage(f"1.4 Version_Required_Feature_Class: {len(required_fc)}")
    
    return required_fc

def get_version_attributes(connection) -> Dict[str, List[str]]:
    """
    Get all attributes in a geodatabase

    This function is used to get the base attributes it would be better to read this from DB
    """

    # 1354 Fields
    attributes_dict = dict()
    fields = 0

    with connection.cursor() as cur:
        for row in cur.execute("""SELECT voa.NOMBRE, voa.ALIAS, voa.GEOMETRIA, voa.CODIGO_OBJETO, voa.NOMBRE_ATRIBUTO, 
        voa.ALIAS_ATRIBUTO, voa.CODIGO_ATRIBUTO, voa.TIPO_ATRIBUTO, voa.TAMANO_ATRIBUTO, voa.DOMINIO, voa.OBLIGACION 
        FROM MJEREZ.VALW_OBJETOS_ATRIBUTOS voa"""):
            fields+= 1
            attributes_dict[row[fields]] = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]]
    
    arcpy.AddMessage(f"1.5 Version_Feature_Attributes: {len(attributes_dict)}")
    
    return attributes_dict