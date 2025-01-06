def get_dbf_encoding(dbf_path):
    with open(dbf_path, 'rb') as f:
        f.seek(28)  # Byte 29 (posición 28 en base cero)
        ldid = f.read(1)
        return ldid.hex()


dbf_path = 'C:/sisclin/DATA/SC0011.DBF'
ldid = get_dbf_encoding(dbf_path)
print(f"Language Driver ID (LDID): {ldid}")
