import subprocess
import csv
import os
from datetime import datetime
from fields import PREDEFINED_FIELDS  # Importar los campos predefinidos


def convert_dbf_to_csv(dbf_path, csv_path):
    try:
        # Obtener el nombre de la tabla a partir del nombre del archivo DBF
        table_name = os.path.splitext(os.path.basename(dbf_path))[0].upper()
        subprocess.run(["ConvertirDBF.exe", dbf_path, csv_path], check=True)
        process_csv(csv_path, table_name)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error converting {dbf_path}: {e}")
    except Exception as e:
        print(f"Error en la conversión del archivo DBF: {e}")


def process_csv(csv_path, table_name):
    temp_csv_path = csv_path + ".tmp"
    start_date = datetime.strptime("01-01-2023", "%d-%m-%Y")  # Fecha inicial
    current_date = datetime.now()  # Fecha actual
    records_found = 0  # Contador para datos en el rango

    # Obtener los campos permitidos para la tabla
    allowed_fields = PREDEFINED_FIELDS.get(table_name, {})

    try:
        with open(csv_path, 'r', encoding='latin-1') as infile, open(temp_csv_path, 'w', newline='', encoding='latin-1') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            headers = next(reader)
            # Filtrar los headers para que solo incluyan los permitidos
            filtered_headers = [
                header for header in headers if header.upper() in allowed_fields]
            writer.writerow(filtered_headers)

            # Identificación de columnas booleanas
            boolean_fields = {field.lower()
                              for field, field_type in allowed_fields.items() if field_type == 'BOOLEAN'}
            boolean_indices = {header: headers.index(
                header) for header in headers if header.lower() in boolean_fields}

            # Identificación de la columna de fecha
            date_index = headers.index(
                'fec_doc') if 'fec_doc' in headers else None
            id_pac_index = headers.index(
                'id_pac') if 'id_pac' in headers else None

            for row in reader:
                if any(row):
                    # Validación de valores en id_pac (debe ser numérico)
                    if id_pac_index is not None and not is_numeric(row[id_pac_index]):
                        continue  # Omite la fila si id_pac no es numérico

                    # Conversión de valores 'F' y 'T' a '0' y '1' en columnas booleanas
                    for header, index in boolean_indices.items():
                        row[index] = convert_boolean_to_int(row[index])

                    if date_index is not None:
                        try:
                            # Limpia espacios en blanco o caracteres no visibles
                            raw_date = row[date_index].strip()
                            if raw_date:  # Verifica que la fecha no sea None o vacía
                                # Convierte la fecha
                                row_date = datetime.strptime(
                                    raw_date, "%m/%d/%Y")
                                if start_date <= row_date <= current_date:
                                    # Filtrar la fila para que solo incluya los campos permitidos
                                    filtered_row = [
                                        row[headers.index(header)] for header in filtered_headers]
                                    writer.writerow(filtered_row)
                                    records_found += 1
                        except ValueError:
                            print(f"Fecha inválida detectada: '{
                                  row[date_index].strip()}'")
                    else:
                        # Si no hay 'fec_doc', escribe las filas sin filtrar por fecha
                        filtered_row = [
                            row[headers.index(header)] for header in filtered_headers]
                        writer.writerow(filtered_row)
        # Reemplazar el archivo original con el archivo temporal solo si la escritura fue exitosa
        os.replace(temp_csv_path, csv_path)

    except Exception as e:
        print(f"Error procesando el archivo CSV: {e}")
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)

    if date_index is not None:
        if records_found > 0:
            print(f"Se encontraron {records_found} registros en el rango de fechas {
                  start_date.date()} - {current_date.date()}.")
        else:
            print(f"No se encontraron registros en el rango de fechas {
                  start_date.date()} - {current_date.date()}.")
    else:
        print("El archivo se procesó sin filtrar por fechas debido a la ausencia de 'fec_doc'.")


def convert_boolean_to_int(value):
    """Convierte 'F' y 'T' en valores enteros 0 y 1 respectivamente."""
    if value == 'F':
        return '0'
    elif value == 'T':
        return '1'
    return value  # Si no es 'F' ni 'T', devuelve el valor tal cual


def is_numeric(value):
    """Verifica si un valor es numérico."""
    try:
        int(value)
        return True
    except ValueError:
        return False
