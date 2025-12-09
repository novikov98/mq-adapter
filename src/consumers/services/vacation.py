from datetime import datetime

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.consumers.dto.vacation import VacationPayloadDTO
from src.consumers.repositoryes.employee import EmployeeRepository


class VacationService:
    """
    Сервис для обработки отпусков.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def upsert_vacation(self, vacation: VacationPayloadDTO):
        employee_repo = EmployeeRepository(bx=self.bx)
        employee = await employee_repo.get_by_email(vacation.employee_email)
        req = BitrixRequest(
            method="lists.element.add",
            params={
                "IBLOCK_TYPE_ID": "lists",
                "IBLOCK_ID": 17,
                "ELEMENT_CODE": f"vacation-{datetime.now()}",
                "FIELDS": {
                    "NAME": "Тестовый отпуск",
                    "PROPERTY_290": employee.ID,
                    "PROPERTY_291": vacation.start_date.strftime("%Y-%m-%d"),
                    "PROPERTY_292": vacation.end_date.strftime("%Y-%m-%d"),
                },
            },
        )
        await self.bx.call_method(req)
