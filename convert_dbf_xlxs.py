import subprocess
import os
# Asumiendo que tienes una función equivalente a process_csv para XLSX
from process_xlsx import process_xlsx


def convert_dbf_to_xlsx(dbf_path, xlsx_path):
    try:
        # Convertir rutas a formato consistente
        dbf_path = os.path.abspath(dbf_path)
        xlsx_path = os.path.abspath(xlsx_path)

        # Validar existencia del archivo DBF
        if not os.path.exists(dbf_path):
            raise FileNotFoundError(f"El archivo DBF no existe: {dbf_path}")

        # Crear la carpeta de destino si no existe
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)

        # Ruta al ejecutable de DBF Viewer 2000
        dbf_viewer_path = r"C:\Program Files (x86)\DBF Viewer 2000\dbview.exe"

        # Construir el comando para convertir el archivo DBF a XLSX
        command = [
            dbf_viewer_path,
            dbf_path,
            f"/export:{xlsx_path}",
            "/SKIPD",  # Omitir registros eliminados
        ]

        # Ejecutar el comando
        subprocess.run(
            command, creationflags=subprocess.CREATE_NO_WINDOW, check=True)

        # Verificar si el archivo XLSX fue generado
        if not os.path.exists(xlsx_path):
            raise FileNotFoundError(
                f"El archivo XLSX no fue generado: {xlsx_path}")

        # Procesar el archivo XLSX resultante
        table_name = os.path.splitext(os.path.basename(dbf_path))[0].upper()
        process_xlsx(xlsx_path, table_name)

    except subprocess.CalledProcessError as e:
        raise Exception(f"Error al ejecutar DBF Viewer 2000: {e}")
    except FileNotFoundError as e:
        print(f"Archivo no encontrado: {e}")
    except Exception as e:
        print(f"Error en la conversión del archivo DBF: {e}")
