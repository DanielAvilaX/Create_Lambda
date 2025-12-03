
import boto3
from datetime import datetime
s3 = boto3.client('s3')

def extract_final_key(source_key):
    date_folder = datetime.now().strftime("%Y-%m-%d")
    filename = source_key.split("/")[-1]
    filenamewithoutextension = filename.split(".")[0]
    extension = filename.split(".")[-1]
    new_filename = f"{filenamewithoutextension}-lambda.{extension}"
    target_key = f"{date_folder}/{new_filename}"
    return target_key

            
def format_price (data):
    try:
        # Formatear el precio en formato COP manualmente
        if "precio" in data:
            valor = float(data["precio"])
            precio_format = f"{valor:,.2f}" 
            precio_format = precio_format.replace(",", "X").replace(".", ",").replace("X", ".")
            data["precio"] = f"$ {precio_format} COP"

        # Agregar notificación y fecha de procesamiento
        data["Notificacion"] = "Notificación generada por Lambda"

        print(f"✅ JSON procesado: {data}")
        return data

    except Exception as e:
        print(f"❌ Error modificando el JSON: {e}")
        return e

def add_metadata(data):

    try:
        # Obtener fecha y hora actual en formato legible
        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Agregar campo al json
        data["metadata"] = {
             "fecha": fecha_actual,
             "user": "santiago"
        }

        print(f"Fecha agregada: {fecha_actual}")
        return data

    except Exception as e:
        print(f"❌ Error agregando la fecha de creación: {e}")
        return e


    