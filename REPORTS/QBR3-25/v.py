# Lee IDs de un archivo y arma un WHERE IN para SQL, con cada ID entre comillas simples

input_filename = 'REPORTS/QBR3-25/ids.txt'

with open(input_filename, "r") as file:
    ids = [line.strip() for line in file if line.strip()]

# Si quieres eliminar duplicados, descomenta la siguiente línea:
# ids = list(set(ids))

# Cada id entre comillas simples
in_string = ", ".join(f"'{id_}'" for id_ in ids)

sql_query = f"WHERE user_id IN ({in_string})"

print(sql_query)