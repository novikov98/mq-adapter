from logging import getLogger

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest

from src.consumers.dto.absence import AbsenceDetailDTO, AbsenceDTO

logger = getLogger(__name__)


class AbsenceRepository:
    """
    Репозиторий отсутсвий Б24.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def add(self, absence: AbsenceDTO) -> AbsenceDetailDTO | None:
        try:
            req = BitrixRequest(method="absence.add", params={**absence})
            resp = await self.bx.call_method(req)
            result = resp.result
            absence = AbsenceDetailDTO(**result)
            return absence
        except Exception:
            return None
