import arcpy
import pandas as pd
from pathlib import Path
from runpy import run_path

from Validador.validator_web import (cod_id_validator, extract_files, get_feature_classes, get_feature_datasets, get_tables, get_feature_attributes, 
                                    quantity_dataset, quantity_feature_class, 
                                    quantity_tables, reference_system, spatial_matching, 
                                    quantity_required, feature_attributes, verify_cod_expediente, verify_mandatory_fields)
from Validador.version_info import (get_version_datasets, get_version_feature_classes, get_version_tables,
                                    get_version_required, get_version_attributes)
from utils.utils import valw_gdb_mensaje, set_workspace
from database.connection import con, engine, schema
from database.gdb_path_to_validate import por_validar, gdb_para_validar, update_estado, borrar_registros_mensajes


if __name__ == '__main__':
    
    try:            
        # path = arcpy.GetParameterAsText(0)
        # expediente = arcpy.GetParameterAsText(1)
        # documento_tecnico = arcpy.GetParameterAsText(2)
        # etapa = arcpy.GetParameterAsText(3)
        # version='1'
            
        # path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validate_Web_Repository\Extract\IDO-08061_202203071.gdb"
        path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validate_Web_Repository\Files\IDO-08061_202203071.gdb.zip"
        extract_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validate_Web_Repository\Extract"
        version='1'

        arcpy.AddMessage('1. Database connection')

        id_bd_gdb, ruta_gdb, expediente, etapa_gdb, documento_gdb = gdb_para_validar(gdb=path)

        update_estado(con, id=id_bd_gdb, estado='En proceso')
        borrar_registros_mensajes(id=id_bd_gdb)

        # Get version Feature Datasets
        version_ds = get_version_datasets(engine)
        # Get version Feature Classes
        version_fc = get_version_feature_classes(engine)
        # Get version Tables
        version_tbl = get_version_tables(engine)
        # Get version Required objects
        version_req = get_version_required(con, documento_gdb, etapa_gdb)
        # Get versionFeatures attributes
        version_att = get_version_attributes(engine)

        # Extract files
        extract_files(path, extract_path)

        # Get Feature Datasets
        ds = get_feature_datasets()
        # Get Feature Classes
        fc = get_feature_classes()
        # Get Feature Classes
        tbl = get_tables()
        # Get Feature Attributes
        attributes = get_feature_attributes(version_fc, fc)

        # Validator 1 - Spatial Matching
        spatial_matching(con, id_bd_gdb, expediente)
        # Validator 2 - Reference System
        df_reference = reference_system(version, engine, id_bd_gdb, version_ds, ds)
        # Validator 3 - Quantity of datasets
        df_datasets = quantity_dataset(engine, id_bd_gdb, version_ds, ds)
        # Validator 4 - Quantity of feature classes
        df_features = quantity_feature_class(engine, id_bd_gdb, version_fc, fc)
        # Validator 5 - Quantity of tables
        df_tables = quantity_tables(engine, id_bd_gdb, version_tbl, tbl)
        # Validator 6 - Required
        quantity_required(con, id_bd_gdb, version_req, fc)
        # Validator 7 - Attributive
        df_attributes = feature_attributes(con, id_bd_gdb, version_att, attributes)
        df_id_validacion = cod_id_validator(dto_tecnico=documento_gdb, id_gdb=id_bd_gdb, features_gdb=fc)
        df_cod_expediente = verify_cod_expediente(engine, gdb_id=id_bd_gdb, version_features=version_fc, features_gdb=fc, expediente=expediente)
        df_mandatory_fields = verify_mandatory_fields(id_gdb=id_bd_gdb, features_gdb=attributes)
        
        final = pd.concat([
            df_reference, 
            df_datasets, 
            df_features, 
            df_tables, 
            df_attributes, 
            df_id_validacion,
            df_cod_expediente,
            df_mandatory_fields
            ])
        final.to_sql(
            name=valw_gdb_mensaje.table_name,
            con=engine, 
            schema=schema, 
            if_exists='append', 
            index=False, 
            chunksize=1000)
        
        update_estado(con, id=id_bd_gdb, estado='Finalizado')
    
    except Exception as e:
        arcpy.AddError(str(e))
        print(str(e))
    finally:
        arcpy.AddMessage("Cerrando conexiones")
        con.close()
        engine.dispose()
        arcpy.AddMessage("Finalizado")
