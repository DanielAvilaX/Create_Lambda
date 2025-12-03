
import boto3
from datetime import datetime
from models.final_model import FinalModel, Metadata
s3 = boto3.client('s3')

def extract_final_key(source_key):
    
    #souce_key: items/23-10-2025/Andres.json
    array_source_key = source_key.split("/")
    filename = array_source_key[-1]
    date_folder = array_source_key[-2]
    filenamewithout = filename.rsplit(".", 1)[0]
    extension = filename.rsplit(".", 1)[-1]

    new_filename = f"{filenamewithout}-lambda.{extension}"
    target_key = f"{date_folder}/{new_filename}"

    print(f"✅ target_key: {target_key}")
    return target_key


            
def format_price(data):
    #final_model = FinalModel()
    try:
        # Formatear el precio en formato COP manualmente
        if "precio" in data:
            valor = float(data["precio"])
            precio_format = f"{valor:,.2f}" 
            precio_format = precio_format.replace(",", "X").replace(".", ",").replace("X", ".")
            #final_model["precio"] = f"$ {precio_format} COP"

        # Agregar notificación y fecha de procesamiento
        #final_model["Notificacion"] = "Notificación generada por Lambda"
        return FinalModel(
            id = data["id"],
            nombre = data["nombre"],
            precio =  f"$ {precio_format} COP",
            notificacion = "Notificación generada por Lambda"
        )

    except Exception as e:
        print(f"❌ Error modificando el JSON: {e}")
        return e

def add_metadata(data, source_bucket, source_key, record, final_model):

    try:
        # Obtener fecha y hora actual en formato legible
        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        version_id = record["s3"]["object"].get("versionId")
        versions = s3.list_object_versions(Bucket=source_bucket, Prefix=source_key)
        file_versions = versions.get('Versions', [])
        file_versions.sort(key=lambda x: x['LastModified'], reverse=True)
        
        if len(file_versions) == 0:
          print(f"No existe")
          raise Exception(f"No se encontró el archivo con source_key: {source_key}")
      
        elif len(file_versions) < 0:
            version_anterior = None

         
        elif len(file_versions) > 1:
            version_anterior = file_versions[1]['VersionId']            
                

        print(f"idVersion actual: {version_id}")
        print(f"idVersion anterior: {version_anterior}")

        # Agregar campo al json
        final_model.metadata = Metadata(
            fecha=fecha_actual,
            user="santiago",
            versionID=version_id,
            versionAnterior=version_anterior
        )

        print(f"Fecha agregada: {fecha_actual}")
        return final_model

    except Exception as e:
        print(f"❌ Error agregando la metadata: {e}")
        return e

def populate_final_model(data,source_bucket,source_key,record):
    
    try:
     # Transformar el formato del peso
     final_model = format_price(data)
     print(f"📄 Datos transformados: {data}")

     #Insertar la metadata
     final_model = add_metadata(data, source_bucket, source_key,record, final_model)
     print(f"Se agrego la metadata correctamente")
    
    except Exception as e:
        print(f"❌ Error agregando la metadata: {e}")
        return e
     #final_model = FinalModel(**data)
    return final_model
    
    
    
    