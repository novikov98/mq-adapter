from logging import getLogger

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.consumers.dto.employee import EmployeeDTO


logger = getLogger(__name__)


class EmployeeRepository:
    """
    Репозиторий сотрудников Б24.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def get_by_email(self, email: str) -> EmployeeDTO | None:
        try:
            req = BitrixRequest(method="user.get", params={"FILTER": {"EMAIL": email}})
            res = await self.bx.call_method(req)
            if len(res.result) == 0:
                return None
            elif len(res.result) > 1:
                logger.error("Получено больше одно сотрудника по email")
                return None
            employee = res.result[0]
            return EmployeeDTO(**employee)
        except Exception:
            logger.error(f"Error while getting employee by email: {email}")
            return None
