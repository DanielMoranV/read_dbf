import mysql.connector
import csv
import time
from fields import PREDEFINED_FIELDS
from datetime import datetime


def convert_date(value, date_format):
    try:
        return datetime.strptime(value, date_format).strftime('%Y-%m-%d')
    except ValueError:
        return None


def convert_datetime(value, datetime_format):
    try:
        return datetime.strptime(value, datetime_format).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def migrate_to_mysql(csv_path, table_name, mysql_config):
    start_time = time.time()
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        # Eliminar la tabla si ya existe
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        with open(csv_path, 'r', encoding='latin-1') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
            # Convertir encabezados a mayúsculas
            headers = [header.upper() for header in headers]

           # Obteniendo los campos predefinidos y la llave primaria de PREDEFINED_FIELDS
            predefined_table = PREDEFINED_FIELDS.get(table_name, {})
            predefined_fields = predefined_table.get('fields', {})

            if predefined_fields:
                # Crear tabla con campos y tipos predefinidos
                fields_definitions = [
                    f'{field} {predefined_fields[field]}'
                    for field in headers if field in predefined_fields
                ]
            else:
                # Crear tabla con todos los campos como TEXT
                fields_definitions = [f'{header} TEXT' for header in headers]

            # Asegurarse de que haya al menos un campo en la definición
            if not fields_definitions:
                raise Exception(
                    f"No hay campos definidos para la tabla {table_name}.")

            primary_key = PREDEFINED_FIELDS.get(
                table_name, {}).get('primary_key')

            if primary_key and primary_key in headers:
                fields_definitions.append(f'PRIMARY KEY ({primary_key})')

            create_table_query = f"CREATE TABLE {
                table_name} ({', '.join(fields_definitions)})"
            cursor.execute(create_table_query)

            # Desactivar índices y claves foráneas
            cursor.execute("SET foreign_key_checks = 0")
            cursor.execute(f"ALTER TABLE {table_name} DISABLE KEYS")
            # Crear índices según las uniones y filtros comunes
            indices = {
                'SC0011': ['cod_ser', 'cod_emp', 'cod_pac', 'num_doc', 'fec_doc', 'tot_doc', 'nom_pac'],
                'SC0006': ['cod_ser'],
                'SC0002': ['cod_cia', 'nom_cia'],
                'SC0003': ['cod_emp'],
                'SC0004': ['cod_pac'],
                'SC0033': ['num_doc'],
                'SC0017': ['num_doc', 'num_fac'],
                'SC0022': ['num_fac']
            }

            if table_name in indices:
                for index in indices[table_name]:
                    cursor.execute(f"CREATE INDEX idx_{table_name.lower()}_{
                                   index} ON {table_name} ({index})")

            # Preparar consulta de inserción
            insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({
                ', '.join(['%s'] * len(headers))})"
            batch_size = 1000
            batch = []

            for row in reader:
                if len(row) < len(headers):
                    row.extend([None] * (len(headers) - len(row)))

                # Convertir fechas
                for i, header in enumerate(headers):
                    if header in predefined_fields:
                        field_type = predefined_fields[header]
                        if field_type == 'DATE':
                            # Ajusta el formato según sea necesario
                            row[i] = convert_date(row[i], '%Y-%m-%d')
                        elif field_type in ['DATETIME', 'TIMESTAMP']:
                            # Ajusta el formato según sea necesario
                            row[i] = convert_datetime(
                                row[i], '%m/%d/%Y %H:%M:%S')

                batch.append([None if value == '' else value for value in row])

                if len(batch) == batch_size:
                    cursor.executemany(insert_query, batch)
                    batch = []

            if batch:
                cursor.executemany(insert_query, batch)

            # Reactivar índices y claves foráneas
            cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS")
            cursor.execute("SET foreign_key_checks = 1")

            cursor.close()
            conn.close()

            end_time = time.time()
            duration = end_time - start_time
            print(f"Tiempo total de migración: {duration:.2f} segundos")
            return f"Table {table_name} migrated successfully."

    except mysql.connector.Error as e:
        raise Exception(f"Error migrating table {table_name}: {e}")
