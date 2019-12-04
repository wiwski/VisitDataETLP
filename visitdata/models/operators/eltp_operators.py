class ELTPOperator:
    """
    Abstract class for all operators following the Extract Load Transform PostProcess pattern.
    """

    datasource_id = None
    
    def init(self, datahub_task_id):
        # TODO implement init logic
        self.fetch_datasource(datahub_task_id)
        pass

    def execute(self, context):
        # TODO set datasource_id after fetching
        self.datasource_id = None
        pass

    def fetch_datasource(self, datahub_task_id):
        """
        Fetch Datasource information
        """
        pass
