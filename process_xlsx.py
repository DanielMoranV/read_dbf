import os
import pandas as pd
from fields import PREDEFINED_FIELDS


def process_xlsx(xlsx_path, table_name):
    # Leer el archivo Excel
    df = pd.read_excel(xlsx_path, sheet_name=0, engine='openpyxl')

    # print("Cabeceras originales:", df.columns.tolist())
    df.columns = df.columns.str.lower()
    # print("Cabeceras después de convertir a minúsculas:", df.columns.tolist())

    # Obtener los campos permitidos para la tabla y convertirlos a minúsculas
    allowed_fields = [field.lower()
                      for field in PREDEFINED_FIELDS.get(table_name, {}).keys()]

    # Verificar que los campos permitidos existan en las columnas del DataFrame
    existing_fields = [
        field for field in allowed_fields if field in df.columns]

    # Filtrar las filas que solo contienen los campos permitidos
    df = df[existing_fields]

    if 'fec_fac' in df.columns:
        df['fec_fac'] = pd.to_datetime(
            df['fec_fac'], format='%d/%m/%Y', errors='coerce')

    # Si el campo 'fec_doc' existe, procesar fechas
    if 'fec_doc' in df.columns:
        df['fec_doc'] = pd.to_datetime(
            df['fec_doc'], format='%d/%m/%Y', errors='coerce')

        # Filtrar solo registros con fechas válidas en el rango especificado
        df = df[(df['fec_doc'] >= '2023-01-01') &
                (df['fec_doc'] <= pd.Timestamp.now())]

    # Si el campo 'fec_ser' existe, procesar fechas
    if 'fec_ser' in df.columns:
        df['fec_ser'] = pd.to_datetime(
            df['fec_ser'], format='%d/%m/%Y', errors='coerce')

        # Filtrar solo registros con fechas válidas en el rango especificado
        df = df[(df['fec_ser'] >= '2023-01-01') &
                (df['fec_ser'] <= pd.Timestamp.now())]

    # Formatear fh_dev a datetime
    if 'fh_dev' in df.columns:
        df['fh_dev'] = pd.to_datetime(
            df['fh_dev'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

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
        df = df.dropna(subset=['id_pac'])
        df['id_pac'] = df['id_pac'].astype(int)

    # Guardar el DataFrame filtrado de vuelta al archivo Excel
    df.to_excel(xlsx_path, index=False, engine='openpyxl')

# process_xlsx(xlsx_path, table_name)  // llama a esta función para procesar archivos XLSX
