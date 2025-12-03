from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Metadata(BaseModel):
    """
    Representa la información técnica asociada al archivo procesado por la Lambda.
    Incluye datos sobre la fecha, usuario, y versiones en S3.
    """
    fecha: datetime                       # Fecha y hora en que se generó la metadata
    user: str                              # Usuario que generó la metadata
    versionID: str                         # ID de la versión actual en S3
    versionAnterior: Optional[str] = None  # ID de la versión anterior (si existe)
