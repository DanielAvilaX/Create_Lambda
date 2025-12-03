from pydantic import BaseModel
from typing import Optional
from models.metadata import Metadata

class FinalModel(BaseModel):
    """
    Modelo principal que representa la estructura de datos procesada por la Lambda.
    Incluye información básica y su metadata asociada.
    """
    id: int                                # Identificador del objeto
    nombre: str                            # Nombre del elemento o persona
    precio: str                            # Precio en formato local (ej: "$ 200.000,00 COP")
    notificacion: str                      # Mensaje de notificación generado por Lambda
    metadata: Optional[Metadata] = None                    # Objeto con información adicional de contexto