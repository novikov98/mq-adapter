from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.consumers.dto.employee import EmployeeDTO


class EmployeeRepository:
    """
    Репозиторий сотрудников Б24.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def get_by_email(self, email: str) -> EmployeeDTO:
        req = BitrixRequest(method="user.get", params={"FILTER": {"EMAIL": email}})
        res = await self.bx.call_method(req)
        employee = res.result[0]
        return EmployeeDTO(**employee)
