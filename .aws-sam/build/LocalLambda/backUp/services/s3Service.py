import json
import boto3
s3 = boto3.client('s3')

def read_data (source_bucket, source_key):
    print(f"📂 Leyendo archivos: {source_bucket}/{source_key}")
    try:
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return data
    except Exception as e:
        print(f"❌ Error leyendo el archivo original: {e}")
        return None
    
    
def save_data (target_bucket, target_key,data):
    print(f"📂 Guardando archivo procesado en: {target_bucket}/{target_key}")

    try:
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False),
            ContentType='application/json'
        )
        print("✅ Archivo modificado y guardado correctamente")
    except Exception as e:
        print(f"❌ Error al guardar el archivo modificado: {e}")
     
def getIdOfLatestVersionsn (event):
    
    try:
        for record in event.get("Records", []):
         version_id = record["s3"]["object"].get("versionId")
         print(f"idVersion: {version_id}")
        
    except Exception as e:
        print(f"No se pudo traer el ID de la version: {e}")
