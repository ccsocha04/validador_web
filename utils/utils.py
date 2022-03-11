import arcpy
import os
import sys
import pandas as pd


from importlib.resources import path
from typing import List, Dict
from zipfile import ZipFile


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