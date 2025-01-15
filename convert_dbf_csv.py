import subprocess
import os
from process_csv import process_csv


def convert_dbf_to_csv(dbf_path, csv_path):
    try:
        # Convertir rutas a formato consistente
        dbf_path = os.path.abspath(dbf_path)
        csv_path = os.path.abspath(csv_path)

        # Validar existencia del archivo DBF
        if not os.path.exists(dbf_path):
            raise FileNotFoundError(f"El archivo DBF no existe: {dbf_path}")

        # Crear la carpeta de destino si no existe
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        # Ruta al ejecutable de DBF Viewer 2000
        dbf_viewer_path = r"C:\Program Files (x86)\DBF Viewer 2000\dbview.exe"

        # Construir el comando para convertir el archivo DBF a CSV
        command = [
            dbf_viewer_path,
            dbf_path,
            f"/export:{csv_path}",
            "/SKIPD",  # Omitir registros eliminados
            "/SEP,",   # Usar coma como separador
            # "/UTF8",   # Codificación UTF-8
            "/HDR"     # Incluir encabezados en el CSV
        ]

        # Ejecutar el comando
        subprocess.run(
            command, creationflags=subprocess.CREATE_NO_WINDOW, check=True)

        # Verificar si el archivo CSV fue generado
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"El archivo CSV no fue generado: {csv_path}")

        # Procesar el archivo CSV resultante
        table_name = os.path.splitext(os.path.basename(dbf_path))[0].upper()
        process_csv(csv_path, table_name)

    except subprocess.CalledProcessError as e:
        raise Exception(f"Error al ejecutar DBF Viewer 2000: {e}")
    except FileNotFoundError as e:
        print(f"Archivo no encontrado: {e}")
    except Exception as e:
        print(f"Error en la conversión del archivo DBF: {e}")
