import os
import subprocess
import pandas as pd
from fields import PREDEFINED_FIELDS

# dbf_path = "Z:/SoporteTi/sisclin/DATA/SC0011.DBF"
# csv_path = "C:/sisclin/TablasVFP/SC0011.csv"
# table_name = os.path.splitext(os.path.basename(dbf_path))[0].upper()
# subprocess.run(["ConvertirDBF.exe", dbf_path, csv_path], check=True)


def process_csv(csv_path, table_name):
    # print(f"Procesando archivo CSV: {csv_path}")
    # print(f"Tabla: {table_name}")

    # Leer el archivo CSV
    df = pd.read_csv(csv_path, encoding='latin1', low_memory=False)
    # initial_row_count = len(df)
    # print(f"Cantidad de filas iniciales: {initial_row_count}")

    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()

    # Obtener los campos permitidos para la tabla y convertirlos a minúsculas
    allowed_fields = [field.lower()
                      for field in PREDEFINED_FIELDS.get(table_name, {}).keys()]

    # Verificar que los campos permitidos existan en las columnas del DataFrame
    existing_fields = [
        field for field in allowed_fields if field in df.columns]

    # Filtrar las filas que solo contienen los campos permitidos
    df = df[existing_fields]
    # filtered_row_count = len(df)

    if 'fec_fac' in df.columns:
        df['fec_fac'] = pd.to_datetime(
            df['fec_fac'], format='%m/%d/%Y', errors='coerce')

    # Si el campo 'fec_doc' existe, procesar fechas
    if 'fec_doc' in df.columns:
        df['fec_doc'] = pd.to_datetime(
            df['fec_doc'], format='%m/%d/%Y', errors='coerce')
        # Contar valores válidos
        # valid_date_count = df['fec_doc'].notna().sum()
        # print(f"Cantidad de fechas válidas en 'fec_doc': {valid_date_count}")

        # Filtrar solo registros con fechas válidas en el rango especificado
        df = df[(df['fec_doc'] >= '2023-01-01') &
                (df['fec_doc'] <= pd.Timestamp.now())]

    # Si el campo 'fec_ser' existe, procesar fechas
    if 'fec_ser' in df.columns:
        df['fec_ser'] = pd.to_datetime(
            df['fec_ser'], format='%m/%d/%Y', errors='coerce')
        # Contar valores válidos
        # valid_date_count = df['fec_ser'].notna().sum()
        # print(f"Cantidad de fechas válidas en 'fec_ser': {valid_date_count}")

        # Filtrar solo registros con fechas válidas en el rango especificado
        df = df[(df['fec_ser'] >= '2023-01-01') &
                (df['fec_ser'] <= pd.Timestamp.now())]

    # Identificar campos booleanos según el esquema definido
    boolean_fields = {field.lower(): field_type for field, field_type in PREDEFINED_FIELDS.get(
        table_name, {}).items() if field_type == 'BOOLEAN'}

    # Si el campo num_doc existe, convertir a entero sin decimales y formatear con ceros a la izquierda
    if 'num_doc' in df.columns:
        df['num_doc'] = pd.to_numeric(df['num_doc'], errors='coerce')
        df = df.dropna(subset=['num_doc'])
        df['num_doc'] = df['num_doc'].round().astype(
            int).astype(str).str.zfill(10)

    if 'cod_ser' in df.columns:
        df['cod_ser'] = pd.to_numeric(df['cod_ser'], errors='coerce')
        df = df.dropna(subset=['cod_ser'])
        df['cod_ser'] = df['cod_ser'].round().astype(
            int)
    if 'cod_pac' in df.columns:
        df['cod_pac'] = pd.to_numeric(df['cod_pac'], errors='coerce')
        df = df.dropna(subset=['cod_pac'])
        df['cod_pac'] = df['cod_pac'].round().astype(
            int)

    if 'nh_pac' in df.columns:
        df['nh_pac'] = pd.to_numeric(df['nh_pac'], errors='coerce')
        df = df.dropna(subset=['nh_pac'])
        df['nh_pac'] = df['nh_pac'].round().astype(
            int)

    if 'cod_cia' in df.columns:
        df['cod_cia'] = pd.to_numeric(df['cod_cia'], errors='coerce')
        df = df.dropna(subset=['cod_cia'])
        df['cod_cia'] = df['cod_cia'].astype(
            int).astype(str).str.zfill(2)

    # Convertir campos booleanos
    for field in boolean_fields:
        if field in df.columns:
            df[field] = df[field].map(
                {'F': 0, 'T': 1, 'f': 0, 't': 1}).fillna(0).astype(int)

    # Verificar y limpiar la columna 'id_pac' si existe
    if 'id_pac' in df.columns:
        df['id_pac'] = pd.to_numeric(df['id_pac'], errors='coerce')
        # invalid_id_pac_count = df['id_pac'].isna().sum()
        # print(f"Cantidad de valores no válidos en 'id_pac': {
        #       invalid_id_pac_count}")
        df = df.dropna(subset=['id_pac'])
        df['id_pac'] = df['id_pac'].astype(int)

    # final_row_count = len(df)
    # print(f"Cantidad de filas después de las transformaciones: {
    #       final_row_count}")

    # Guardar el DataFrame filtrado de vuelta al archivo CSV
    df.to_csv(csv_path, index=False, encoding='latin1')


# process_csv(csv_path, table_name)
