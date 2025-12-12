from logging import getLogger
from datetime import datetime

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest
from bx_client.exceptions import BitrixAPIException

from src.consumers.dto.vacation import VacationPayloadDTO
from src.consumers.repositoryes.employee import EmployeeRepository
from src.core.rabbit.exceptions import EnvelopeProcessingError


logger = getLogger(__name__)


class VacationService:
    """
    Сервис для обработки отпусков.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def upsert_vacation(self, vacation: VacationPayloadDTO):
        try:
            employee_repo = EmployeeRepository(bx=self.bx)
            employee = await employee_repo.get_by_email(vacation.employee_email)
            if not employee:
                raise EnvelopeProcessingError("Employee not found", requeue=False)
            req = BitrixRequest(
                method="lists.element.add",
                params={
                    "IBLOCK_TYPE_ID": "lists",
                    "IBLOCK_ID": 73,
                    "ELEMENT_CODE": f"vacation-{datetime.now()}",
                    "FIELDS": {
                        "NAME": "name",
                        "PROPERTY_325": employee.ID,
                        "PROPERTY_326": vacation.start_date.strftime("%Y-%m-%d"),
                        "PROPERTY_327": vacation.end_date.strftime("%Y-%m-%d"),
                    },
                },
            )
            await self.bx.call_method(req)
        except BitrixAPIException as e:
            logger.exception(
                f"Bitrix API error\n"
                f"error: {e.bitrix_error.error}\n"
                f"description: {e.bitrix_error.error_description}\n"
            )
            raise
