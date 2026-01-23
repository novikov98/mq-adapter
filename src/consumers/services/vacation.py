from logging import getLogger
from typing import Type

from bx_client import BitrixAsync
from bx_client.exceptions import BitrixServerError, BitrixInvalidRequestError

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

    async def upsert_vacation(self, vacation: VacationPayloadDTO):
        try:
            # Получаем сотрудника
            employee = await self.employee_repo.get_by_email(vacation.employee_email)
            if not employee:
                raise EnvelopeProcessingError("Employee not found", requeue=False)

            absence = await self.absence_repo.get(vacation.vacation_id)
            if absence:
                absence.user_id = employee.ID
                absence.start_date = vacation.start_date
                absence.end_date = vacation.end_date
                await self.absence_repo.update(absence)
            else:
                absence = AbsenceDTO(
                    uuid=vacation.vacation_id,
                    user_id=employee.ID,
                    absence_type="VACATION",
                    start_date=vacation.start_date,
                    end_date=vacation.end_date,
                )
                await self.absence_repo.add(absence)

        except BitrixInvalidRequestError as e:
            logger.debug(
                f"Bitrix API error\n"
                f"error: {e.bitrix_error.error}\n"
                f"description: {e.bitrix_error.error_description}\n"
            )
            raise
        except BitrixServerError as e:
            logger.error(
                f"Bitrix server error\n"
                f"error: {e.bitrix_error.error}\n"
                f"description: {e.bitrix_error.description}\n"
            )
            raise
        except Exception as e:
            logger.error(f"Bitrix unknown error. {str(e)}")
            raise
