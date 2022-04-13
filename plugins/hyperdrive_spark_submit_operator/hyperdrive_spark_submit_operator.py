from airflow.models.baseoperator import BaseOperator
import time

from airflow.triggers.base import BaseTrigger, TriggerEvent
from typing import Any, Dict, Tuple
import asyncio

class HyperdriveSparkSubmitOperator(BaseOperator):
    def __init__(self, name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        print("Starting")
        if YarnApi.isJobRunning(self):
            self.defer(trigger=HyperdriveSparkSubmitTrigger(job_uuid="job_uuid_unique"), method_name="onSparkJobFinish")
        else:
            self.__sparkSubmit__()

    def __sparkSubmit__(self):
        time.sleep(60)
        print("Submitting spark job")
        self.defer(trigger=HyperdriveSparkSubmitTrigger(job_uuid="job_uuid_unique"), method_name="onSparkJobFinish")


    def onSparkJobFinish(self, context, event=None):
        return "done"

    def on_kill(self):
        #Kill subprocess if exist if no we can kill job in yarn if needed
        time.sleep(60)


class HyperdriveSparkSubmitTrigger(BaseTrigger):
    def __init__(self, job_uuid: str):
        super().__init__()
        self.job_uuid = job_uuid

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("hyperdrive_spark_submit_operator.hyperdrive_spark_submit_operator.HyperdriveSparkSubmitTrigger", {"job_uuid": self.job_uuid})

    async def run(self):
        await asyncio.sleep(30)
        yield TriggerEvent(self.job_uuid)

class YarnApi():
    @staticmethod
    def isJobRunning(self) -> bool:
        return False

    def isJobSuccessful(self) -> bool:
        return True