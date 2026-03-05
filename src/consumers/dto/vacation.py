from datetime import datetime
from pydantic import BaseModel


class VacationPayloadDTO(BaseModel):
    vacation_id: str
    employee_email: str
    employee_full_name: str
    start_date: datetime
    end_date: datetime
