from logging import getLogger
from typing import Type

from bx_client.bitrix import BitrixAsync
from bx_client.exceptions import BitrixAPIException

from src.consumers.dto.absence import AbsenceDTO
from src.consumers.dto.vacation import VacationPayloadDTO
from src.consumers.repositoryes.absence import AbsenceRepository
from src.consumers.repositoryes.employee import EmployeeRepository
from src.core.rabbit.exceptions import EnvelopeProcessingError


logger = getLogger(__name__)


class VacationService:
    """
    Сервис для обработки отпусков.
    """

    def __init__(
        self,
        bx: BitrixAsync,
        employee_repo: Type[EmployeeRepository],
        absence_repo: Type[AbsenceRepository],
    ):
        self.bx = bx
        self.employee_repo = employee_repo(self.bx)
        self.absence_repo = absence_repo(self.bx)

    async def add_vacation(self, vacation: VacationPayloadDTO):
        try:
            employee = await self.employee_repo.get_by_email(vacation.employee_email)
            if not employee:
                raise EnvelopeProcessingError("Employee not found", requeue=False)
            absence = AbsenceDTO(
                user_id=employee.ID,
                absence_type="VACATION",
                start_date=vacation.start_date,
                end_date=vacation.end_date,
            )
            vacation = await self.absence_repo.add(absence)

        except BitrixAPIException as e:
            logger.exception(
                f"Bitrix API error\n"
                f"error: {e.bitrix_error.error}\n"
                f"description: {e.bitrix_error.error_description}\n"
            )
            raise
