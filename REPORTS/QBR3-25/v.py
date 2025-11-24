import csv

def texto_a_csv_palabras(texto, nombre_archivo="salida.csv"):
    with open(nombre_archivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for linea in texto.strip().split("\n"):
            palabras = linea.strip().split()
            writer.writerow(palabras)

    print("CSV generado:", nombre_archivo)

texto = """
TEXTO
"""
texto_a_csv_palabras(texto)
