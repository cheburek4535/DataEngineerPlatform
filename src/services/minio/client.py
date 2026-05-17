from minio import Minio

client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password123",
    secure=False,
)