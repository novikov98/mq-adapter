from datetime import datetime
from pydantic import BaseModel


class VacationPayloadDTO(BaseModel):
    employee_email: str
    employee_full_name: str
    start_date: datetime
    end_date: datetime
