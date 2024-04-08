from pydantic import BaseModel


class QA(BaseModel):
    checks: dict = None
    raise_alert: bool = None
    qa_db: str = None
    qa_table: str = None
    qa_enable: bool = False
    check_name: str = "No check name"


class Profiling(BaseModel):
    source_database: str = None
    source_table: str = None
    qa_database: str = None
