from src.consumers.repositoryes import AbsenceRepository, EmployeeRepository
from src.consumers.services.vacation import VacationService
from src.core.bitrix.client import BitrixClient
from src.core.rabbit.dto import Envelope
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.consumers.dto.vacation import VacationPayloadDTO


router = EnvelopeRouter()


@router.on("upsert", version=1.0)
async def upsert(envelope: Envelope):
    try:
        service = VacationService(bx=BitrixClient.bx, employee_repo=EmployeeRepository, absence_repo=AbsenceRepository)
        vacation = VacationPayloadDTO(**envelope.payload)
        await service.add_vacation(vacation)

    except Exception:
        raise
