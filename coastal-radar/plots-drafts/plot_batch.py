class PlotBatch:
    """
    Represents one surface-current plot batch:
    - a single ONC requestDataProduct window
    - corresponding runDataProduct
    - eventual downloadDataProduct
    """

    def __init__(self, dpRequestId: int, batch_id: str, start: str, end: str):
        """Constructor. """
        self.dpRequestId: int = dpRequestId     # int
        self.runIds = None    # TODO: typelist[int]?
        self.batch_id: str = batch_id           # "2025-03-2Q"
        self.start: str = start                 # ISO string
        self.end: str = end                     # ISO string
        self.last_call_ran = "requestDataProduct"
        self.call_status = "pending"
        self.file_count = 0
        self.downloaded = False

    @classmethod
    def dict_to_obj(cls, row: dict):
        """Create a PlotBatch from a dict / DataFrame row, overriding defaults."""
        batch = cls(
            dpRequestId=row["dpRequestId"],
            batch_id=row["batch_id"],
            start=row["start"],
            end=row["end"],
        )
        batch.runIds = row.get("runIds")
        batch.last_call_ran = row.get("last_call_ran", batch.last_call_ran)
        batch.call_status = row.get("call_status", batch.call_status)
        batch.file_count = row.get("file_count", batch.file_count)
        batch.downloaded = row.get("downloaded", batch.downloaded)
        return batch


    def to_dict(self):
        """Convert to dict for DataFrame / CSV writing."""
        return {
            "dpRequestId": self.dpRequestId,
            "runIds": self.runIds,
            "batch_id": self.batch_id,
            "start": self.start,
            "end": self.end,
            "last_call_ran": self.last_call_ran,
            "call_status": self.call_status,
            "file_count": self.file_count,
            "downloaded": self.downloaded,
        }

    def update_after_new_request(self, new_req_id):
        self.dpRequestId = new_req_id
        self.runIds = None
        self.last_call_ran = "make_dp_request"
        self.call_status = "complete"

    def update_after_run(self, new_run_id, file_count: int, call_status: str):
        """Literally just updates with the generated run id and last call ran?"""
        self.runIds = new_run_id
        self.last_call_ran = "run_dp"
        self.call_status = call_status
        self.file_count = file_count

    def update_after_download(self):
        """ """
        self.last_call_ran = "download_dp"
        self.call_status = "complete"
        self.downloaded = True
