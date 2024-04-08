from pydantic import BaseModel, ValidationError, validator, root_validator
from typing import Optional, Union, Literal


class Pipeline(BaseModel):
    source: Optional[dict]
    destination: Optional[dict]
    transform: Optional[dict]
    QA: Optional[dict]