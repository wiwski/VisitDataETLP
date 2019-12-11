from datetime import datetime
from visitdata.models.operators import ExtractOperator


class VisitExtractOperator(ExtractOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(task_id="visit_extract", *args, **kwargs)

    def check_format(self, file):
        return True

    def create_context(self, file) -> dict:
        context = {
            "poi_code": file.name.split("-")[1].strip(),
            "obs_date": datetime.today().strftime("%Y-%m-%d")
        }
        return context
