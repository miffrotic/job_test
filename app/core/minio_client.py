"""MinIO client for object storage."""

import io
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error

from app.core.config import settings


class MinIOClient:
    """MinIO object storage client."""

    def __init__(self) -> None:
        self._client: Minio | None = None

    def connect(self) -> None:
        """Initialize MinIO client."""
        self._client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Create default bucket if it doesn't exist."""
        if self._client is None:
            raise RuntimeError("MinIO client not initialized")

        bucket_name = settings.MINIO_BUCKET_NAME
        if not self._client.bucket_exists(bucket_name):
            self._client.make_bucket(bucket_name)

    @property
    def client(self) -> Minio:
        """Get MinIO client instance."""
        if self._client is None:
            raise RuntimeError("MinIO client not initialized. Call connect() first.")
        return self._client

    def upload_file(
        self,
        file_data: BinaryIO | bytes,
        object_name: str,
        bucket_name: str | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload a file to MinIO.

        Args:
            file_data: File data as bytes or file-like object
            object_name: Name of the object in the bucket
            bucket_name: Bucket name (uses default if not provided)
            content_type: MIME type of the file

        Returns:
            Object name in the bucket
        """
        bucket = bucket_name or settings.MINIO_BUCKET_NAME

        if isinstance(file_data, bytes):
            file_data = io.BytesIO(file_data)

        file_data.seek(0, 2)
        file_size = file_data.tell()
        file_data.seek(0)

        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=file_data,
            length=file_size,
            content_type=content_type,
        )

        return object_name

    def download_file(
        self,
        object_name: str,
        bucket_name: str | None = None,
    ) -> bytes:
        """Download a file from MinIO.

        Args:
            object_name: Name of the object in the bucket
            bucket_name: Bucket name (uses default if not provided)

        Returns:
            File content as bytes
        """
        bucket = bucket_name or settings.MINIO_BUCKET_NAME

        response = self.client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def delete_file(
        self,
        object_name: str,
        bucket_name: str | None = None,
    ) -> None:
        """Delete a file from MinIO.

        Args:
            object_name: Name of the object to delete
            bucket_name: Bucket name (uses default if not provided)
        """
        bucket = bucket_name or settings.MINIO_BUCKET_NAME
        self.client.remove_object(bucket, object_name)

    def list_files(
        self,
        prefix: str = "",
        bucket_name: str | None = None,
    ) -> list[dict]:
        """List files in a bucket.

        Args:
            prefix: Filter objects by prefix
            bucket_name: Bucket name (uses default if not provided)

        Returns:
            List of object information dictionaries
        """
        bucket = bucket_name or settings.MINIO_BUCKET_NAME

        objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)

        return [
            {
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified,
                "etag": obj.etag,
            }
            for obj in objects
        ]

    def get_presigned_url(
        self,
        object_name: str,
        bucket_name: str | None = None,
        expires_hours: int = 1,
    ) -> str:
        """Generate a presigned URL for file download.

        Args:
            object_name: Name of the object
            bucket_name: Bucket name (uses default if not provided)
            expires_hours: URL expiration time in hours

        Returns:
            Presigned URL string
        """
        from datetime import timedelta

        bucket = bucket_name or settings.MINIO_BUCKET_NAME

        return self.client.presigned_get_object(
            bucket,
            object_name,
            expires=timedelta(hours=expires_hours),
        )

    def file_exists(
        self,
        object_name: str,
        bucket_name: str | None = None,
    ) -> bool:
        """Check if a file exists in the bucket.

        Args:
            object_name: Name of the object
            bucket_name: Bucket name (uses default if not provided)

        Returns:
            True if file exists, False otherwise
        """
        bucket = bucket_name or settings.MINIO_BUCKET_NAME

        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False


# Global MinIO client instance
minio_client = MinIOClient()


def get_minio_client() -> MinIOClient:
    """FastAPI dependency for MinIO client access."""
    return minio_client
