import subprocess
import os
from process_csv import process_csv


def convert_dbf_to_csv(dbf_path, csv_path):
    try:
        # Obtener el nombre de la tabla a partir del nombre del archivo DBF
        table_name = os.path.splitext(os.path.basename(dbf_path))[0].upper()
        subprocess.run(["ConvertirDBF.exe", dbf_path, csv_path],
                       creationflags=subprocess.CREATE_NO_WINDOW, check=True)
        process_csv(csv_path, table_name)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error converting {dbf_path}: {e}")
    except Exception as e:
        print(f"Error en la conversión del archivo DBF: {e}")
