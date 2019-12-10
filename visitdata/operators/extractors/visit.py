from visitdata.models.operators import ExtractOperator


class VisitExtractOperator(ExtractOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(task_id="visit_extract", *args, **kwargs)

    def check_format(self, file):
        return True

    def create_context(self, data) -> dict:
        context = {
            "poi_code": None,
            "obs_date": None
        }
        return context
