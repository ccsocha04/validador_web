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
