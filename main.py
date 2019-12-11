from visitdata.operators.extractors.visit import VisitExtractOperator


v = VisitExtractOperator(datahub_task_id=1, hook=None)
v.execute(None)
print(v)
