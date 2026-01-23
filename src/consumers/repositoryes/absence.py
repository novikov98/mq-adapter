from logging import getLogger

from bx_client.bitrix import BitrixAsync
from bx_client.dto.requests import BitrixRequest
from bx_client.exceptions import BitrixNotFoundError

from src.consumers.dto.absence import AbsenceDTO

logger = getLogger(__name__)


class AbsenceRepository:
    """
    Репозиторий отсутсвий Б24.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def add(self, absence: AbsenceDTO) -> AbsenceDTO | None:
        try:
            req = BitrixRequest(method="absence.add", params=absence.model_dump(mode="json"))
            resp = await self.bx.call_method(req)
            result = resp.result
            absence = AbsenceDTO(**result)
            return absence
        except Exception:
            raise

    async def get(self, absence_uuid: str) -> AbsenceDTO | None:
        try:
            req = BitrixRequest(method="absence.get", params={"uuid": absence_uuid})
            resp = await self.bx.call_method(req)
            result = resp.result
            absence = AbsenceDTO(**result)
            return absence
        except BitrixNotFoundError:
            return None
        except Exception as e:
            logger.error(f"error while getting absence: {e}")
            raise

    async def update(self, absence: AbsenceDTO) -> AbsenceDTO:
        try:
            req = BitrixRequest(
                method="absence.update",
                params={"uuid": absence.uuid, "fields": absence.model_dump(mode="json", exclude="uuid")},
            )
            resp = await self.bx.call_method(req)
            result = resp.result
            absence = AbsenceDTO(**result)
            return absence
        except Exception as e:
            logger.error(f"error while updating absence: {e}")
            raise
