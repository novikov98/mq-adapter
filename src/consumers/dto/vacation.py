from datetime import date
from pydantic import BaseModel


class VacationPayloadDTO(BaseModel):
    employee_email: str
    employee_full_name: str
    start_date: date
    end_date: date
