from datetime import datetime
from typing import Literal

from pydantic import BaseModel


ABSENCE_TYPES = Literal[
    "VACATION",  # отпуск ежегодный
    "ASSIGNMENT",  # командировка
    "LEAVESICK",  # больничный
    "LEAVEMATERINITY",  # отпуск декретный
    "LEAVEUNPAYED",  # отгул за свой счет
    "UNKNOWN",  # прогул
    "OTHER",  # другое
]


class AbsenceDTO(BaseModel):
    user_id: int
    absence_type: ABSENCE_TYPES
    start_date: datetime
    end_date: datetime


class AbsenceDetailDTO(AbsenceDTO):
    id: int
