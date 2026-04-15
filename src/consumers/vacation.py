from src.consumers.routers.vacation import router
from src.core.rabbit.client import Consumer
from src.core.rabbit.dto import QueueConfig


config = QueueConfig(
    queue_name="bitrix.hr.vacation",
    exchange_name="hr.events",
    binding_key="hr.vacation.*",
    dlq_name="bitrix.hr.vacation.dlq",
    dlx_name="hr.dlq",
    dlx_routing_key="hr.vacation.dlq",
    prefetch_count=1,
)

vacation_consumer = Consumer(name="vacation", config=config, router=router)
