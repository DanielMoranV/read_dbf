# functions.py (o directamente en el archivo principal)
import os
import time
from convert_dbf_csv import convert_dbf_to_csv
from import_csv_mysql import migrate_to_mysql


def run_migration_logic(path_databases, output_folder, tables, logger=None):
    """
    Ejecuta la lógica para convertir y migrar tablas DBF a CSV y luego a MySQL.
    """
    start_time = time.time()
    messages = []

    os.makedirs(output_folder, exist_ok=True)

    for table in tables:
        dbf_path = os.path.join(path_databases, f"{table}.DBF")
        csv_path = os.path.join(output_folder, f"{table}.csv")

        try:
            convert_dbf_to_csv(dbf_path, csv_path)
            message = f"Successfully converted {dbf_path} to {csv_path}"
            messages.append(message)
            if logger:
                logger(message)

            mysql_config = {
                'host': os.getenv('MYSQL_HOST', 'localhost'),
                'user': os.getenv('MYSQL_USER', 'root'),
                'password': os.getenv('MYSQL_PASS', ''),
                'database': os.getenv('MYSQL_DB', 'db_sisclin'),
                'allow_local_infile': True,
                'charset': 'utf8mb4'
            }
            migration_message = migrate_to_mysql(csv_path, table, mysql_config)
            messages.append(migration_message)
            if logger:
                logger(migration_message)
        except Exception as e:
            error_message = f"Error processing {table}: {str(e)}"
            messages.append(error_message)
            if logger:
                logger(error_message)

    elapsed_time = time.time() - start_time
    summary_message = f"Tiempo de ejecución: {elapsed_time:.2f} segundos"
    messages.append(summary_message)
    if logger:
        logger(summary_message)

    return messages
