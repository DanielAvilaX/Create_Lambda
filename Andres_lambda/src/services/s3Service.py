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
    
    
def save_data (target_bucket, target_key,final_model):
    print(f"📂 Guardando archivo procesado en: {target_bucket}/{target_key}")

    try:
        # 🔹 Si el modelo es Pydantic, conviértelo a JSON antes de subirlo
        if hasattr(final_model, "model_dump_json"):  # Pydantic v2
            json_body = final_model.model_dump_json(indent=2, ensure_ascii=False)
        elif hasattr(final_model, "json"):  # Pydantic v1
            json_body = final_model.json(indent=2, ensure_ascii=False)
        else:
            # En caso de que sea un dict normal
            json_body = json.dumps(final_model, indent=2, ensure_ascii=False)
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=json_body,
            ContentType='application/json'
        )
        print("✅ Archivo modificado y guardado correctamente")
    except Exception as e:
        print(f"❌ Error al guardar el archivo modificado: {e}")
     