import pandas as pd
from pathlib import Path
from runpy import run_path

from Validador.validator_web import (extract_files, get_feature_classes, get_feature_datasets, get_tables,  
                                    quantity_dataset, quantity_feature_class, 
                                    quantity_tables, reference_system, spatial_matching)
from Validador.version_info import get_version_datasets, get_version_feature_classes, get_version_tables
from utils.utils import valw_gdb_mensaje, set_workspace
from database.connection import con, engine, schema
from database.gdb_path_to_validate import por_validar, gdb_para_validar, update_estado, borrar_registros_mensajes


if __name__ == '__main__':
    # cantidad = por_validar(con)

    #if cantidad > 0:
    #path = r'E:\VWMDG\validador_web\Data\IDO-08061_202203071.gdb'
    path = r'E:\VWMDG\validador_web\Data\IDO-08061_20220303.gdb'
    id_bd_gdb, ruta_gdb = gdb_para_validar(engine, gdb=path)
    # TODO UPDATE INICIO_VALIDACION
    update_estado(con, id=id_bd_gdb, estado='En proceso')
    borrar_registros_mensajes(con, id=id_bd_gdb)

    
    # Get version GDB
    
    # Feature Datasets
    version_ds = get_version_datasets(engine)
    # Feature Classes
    version_fc = get_version_feature_classes(engine)
    # Tables
    version_tbl = get_version_tables(engine)
    
    # file_path = str(current_path.parent.absolute().joinpath(data_folder).joinpath(zip_file))
    # TODO esto debe ser leido de la base de datos
    # extract_path = str(current_path.parent.absolute().joinpath(data_folder))

    # Extract files
    # TODO verificar que path no termine en zip. si es así se debe desempaquetar y actualizar la ruta
    #  enla base de datos
    # if ruta_gdb.endswith('.zip'):
    #    extract_files(file_path, extract_path)
    set_workspace(ruta_gdb)

    # Get Datasets
    ds = get_feature_datasets()

    # Get Feature Classes
    fc = get_feature_classes()

    # Get Feature Classes
    tbl = get_tables()
    
    # Validator 1 - Spatial Matching
    #validate_1 = spatial_matching()

    # Validator 2 - Reference System
    version='1'
    df_reference = reference_system(version, engine, id_bd_gdb, version_ds, ds)

    # Validator 3 - Quantity of datasets
    df_datasets = quantity_dataset(engine, id_bd_gdb, version_ds, ds)

    # Validator 4 - Quantity of feature classes
    df_features = quantity_feature_class(engine, id_bd_gdb, version_fc, fc)

    # Validator 5 - Quantity of tables
    df_tables = quantity_tables(engine, id_bd_gdb, version_tbl, tbl)

    # Validator 6 - Required fields
    # Validator 7 - Attributive characteristics
    
    # TODO actualizar estado de la gdb
    final = pd.concat([df_reference, df_datasets, df_features, df_tables])
    final.to_sql(
        name=valw_gdb_mensaje.table_name,
        con=engine, 
        schema=schema, 
        if_exists='append', 
        index=False, 
        chunksize=1000)
    
    update_estado(con, id=id_bd_gdb, estado='Finalizado')