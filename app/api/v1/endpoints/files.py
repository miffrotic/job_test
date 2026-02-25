"""File upload and download endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from app.core.minio_client import MinIOClient, get_minio_client
from app.schemas.base import SuccessResponse

router = APIRouter()


@router.post("/upload")
async def upload_file(
    file: Annotated[UploadFile, File(description="File to upload")],
    folder: str = "",
    minio: MinIOClient = Depends(get_minio_client),
) -> dict:
    """
    Upload a file to MinIO storage.
    
    Files can be organized in folders by providing the folder parameter.
    Returns the object name and a presigned URL for download.
    """
    try:
        # Create object name with optional folder
        object_name = f"{folder}/{file.filename}" if folder else file.filename
        object_name = object_name.lstrip("/")

        # Read file content
        content = await file.read()

        # Upload to MinIO
        minio.upload_file(
            file_data=content,
            object_name=object_name,
            content_type=file.content_type or "application/octet-stream",
        )

        # Generate presigned URL
        url = minio.get_presigned_url(object_name)

        return {
            "object_name": object_name,
            "file_name": file.filename,
            "size": len(content),
            "content_type": file.content_type,
            "download_url": url,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}",
        )


@router.get("/download/{object_name:path}")
async def get_download_url(
    object_name: str,
    expires_hours: int = 1,
    minio: MinIOClient = Depends(get_minio_client),
) -> dict:
    """
    Get a presigned URL for downloading a file.
    
    The URL expires after the specified number of hours (default: 1 hour).
    """
    if not minio.file_exists(object_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {object_name} not found",
        )

    url = minio.get_presigned_url(object_name, expires_hours=expires_hours)
    return {
        "object_name": object_name,
        "download_url": url,
        "expires_hours": expires_hours,
    }


@router.get("")
async def list_files(
    prefix: str = "",
    minio: MinIOClient = Depends(get_minio_client),
) -> list[dict]:
    """
    List files in the storage bucket.
    
    Use the prefix parameter to filter files by path prefix.
    """
    return minio.list_files(prefix=prefix)


@router.delete("/{object_name:path}", response_model=SuccessResponse)
async def delete_file(
    object_name: str,
    minio: MinIOClient = Depends(get_minio_client),
) -> SuccessResponse:
    """Delete a file from storage."""
    if not minio.file_exists(object_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {object_name} not found",
        )

    minio.delete_file(object_name)
    return SuccessResponse(message=f"File {object_name} deleted successfully")
