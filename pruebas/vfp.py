from dbfread import DBF


def get_dbf_fields(dbf_path):
    table = DBF(dbf_path)
    fields = {field.name: convert_type(field.type) for field in table.fields}
    return fields


def convert_type(dbf_type):
    if dbf_type == 'C':
        return 'VARCHAR(255)'
    elif dbf_type == 'N':
        return 'DECIMAL(10,2)'
    elif dbf_type == 'D':
        return 'DATE'
    elif dbf_type == 'L':
        return 'BOOLEAN'
    elif dbf_type == 'T':
        return 'TIMESTAMP'
    elif dbf_type == 'I':
        return 'INT'
    else:
        return 'TEXT'


tables = ['SC0011', 'SC0006', 'SC0003', 'SC0004',
          'SC0033', 'SC0017', 'SC0002', 'SC0012', 'SC0022']
base_path = 'Z:/SoporteTi/sisclin/DATA/'

PREDEFINED_FIELDS = {}

for table_name in tables:
    dbf_path = f'{base_path}{table_name}.DBF'
    PREDEFINED_FIELDS[table_name] = get_dbf_fields(dbf_path)

print(PREDEFINED_FIELDS)
