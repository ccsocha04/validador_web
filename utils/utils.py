from lib2to3.pytree import Base
import arcpy
import os
import sys
import pandas as pd


from importlib.resources import path
from typing import List, Dict
from zipfile import ZipFile
from pydantic import BaseModel


def conversion_format(path: str):
    """
    Convert all files to geodatabase
    """
    # TODO - Add conversion to geodatabase
    arcpy.AddMessage("2.4 Converting files to geodatabase...")
    arcpy.AddMessage("2.5 Converting feature datasets...")
    arcpy.AddMessage("2.6 Converting feature classes...")
    arcpy.AddMessage("2.7 Converting tables...")

def set_workspace(path: str) -> None:
    """
    Set workspace
    """
    arcpy.AddMessage("3. Setting workspace...")
    arcpy.env.workspace = path
    arcpy.env.overwriteOutput = True

class ValwGdbMensaje(BaseModel):
    table_name: str = 'VALW_GDB_MENSAJE'
    gdb_id_column: str = 'GDB_ID' 
    validador_id: str = 'VALIDADOR_ID'
    mensaje_column: str = 'MENSAJE_VAL'


class ValwDomValidadores(BaseModel):
    table_name: str = 'VALW_DOM_VALIDADORES'
    id: str = 'ID'
    descripcion: str='DESCRIPCION'
    srs: str = 'SISTEMA DE REFERENCIA'
    datasets: str = 'DATASETS'
    features: str = 'FEATURE CLASSES'

class ValwSRS(BaseModel):
    table_name: str = 'VALW_SRS'
    srscode: str = 'SRSCODE'
    nombre: str = 'NOMBRE'
    version_id: str = 'VERSION_ID'

class ValwVersion(BaseModel):
    table_name: str ='VALW_VERSION'
    version: str = 'VERSION' 
    id: str = 'ID'


valw_srs = ValwSRS()
valw_version = ValwVersion()
valw_gdb_mensaje = ValwGdbMensaje()
valw_dom_validadores = ValwDomValidadores()
    