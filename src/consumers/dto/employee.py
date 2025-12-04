from pydantic import BaseModel


class EmployeeDTO(BaseModel):
    ID: int
    NAME: str
    EMAIL: str
