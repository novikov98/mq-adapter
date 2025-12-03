from bx_client.bitrix import BitrixAsync

from src.consumers.dto.vacation import VacationUpsertDTO


class VacationService:
    """
    Сервис для обработки отпусков.
    """

    def __init__(self, bx: BitrixAsync):
        self.bx = bx

    async def upsert_vacation(self, vacation: VacationUpsertDTO):
        print("РАБОТАЕЕЕЕЕТ")
        print(vacation)
