import json
from io import BytesIO
from datetime import datetime, timezone
from services.minio.client import client

def save_raw_json(bucket: str, prefix: str, data: dict):
    json_bytes = json.dumps(data).encode('utf-8')

    object_name = f"{prefix}/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H_%M_%S_%f')}.json"

    client.put_object(bucket, object_name, BytesIO(json_bytes), length=len(json_bytes), content_type="application/json")
    print(f"Saved {len(json_bytes)} bytes to {object_name} in MinIO")
