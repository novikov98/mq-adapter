from logging import getLogger

from bx_client.exceptions import BitrixServerError, BitrixInvalidRequestError

from src.consumers.repositoryes import AbsenceRepository, EmployeeRepository
from src.consumers.services.vacation import VacationService
from src.core.bitrix.client import BitrixClient
from src.core.bitrix.exceptions import BitrixTemporaryUnavailableError
from src.core.rabbit.dto import Envelope
from src.core.rabbit.envelope_router import EnvelopeRouter
from src.consumers.dto.vacation import VacationPayloadDTO
from src.core.rabbit.exceptions import EnvelopeProcessingError

logger = getLogger(__name__)
router = EnvelopeRouter()


@router.on("upsert", version=1.0)
async def upsert(envelope: Envelope):
    try:
        service = VacationService(bx=BitrixClient.bx, employee_repo=EmployeeRepository, absence_repo=AbsenceRepository)
        vacation = VacationPayloadDTO(**envelope.payload)
        await service.add_vacation(vacation)

    except BitrixServerError:
        await BitrixClient.bitrix_availability_guard.mark_down()
        raise BitrixTemporaryUnavailableError()
    except BitrixInvalidRequestError:
        raise EnvelopeProcessingError(requeue=False)
    except Exception:
        raise EnvelopeProcessingError(requeue=False)
