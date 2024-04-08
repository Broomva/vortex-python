from pydantic import BaseModel


class Eventhub(BaseModel):
    consumer_group: str = None
    connection_string: str = None
    max_events_per_trigger: int = None
    start_time: str = None
    end_time: str = None
    offset: str = None
    table: str = "eventhub"
    trigger_type: str = None
    trigger_setting: str = None
    ignore_changes: bool = True


class BlobEventhubAutoloader(BaseModel):
    storage_account_key: str = None
    storage_account_name: str = None
    container: str = None
    namespace: str = None
    eventhub: str = None
    partition_id: str = "*"
    format: str = "avro"
    table: str = "eventhub_autoloader"


class Delta(BaseModel):
    database: str = None
    table: str = None
    columns: str = "*"
    filter: str = ""
    mode: str = "overwrite"
    starting_version: str = None
    starting_timestamp: str = None
    max_files_per_trigger: int = None
    max_bytes_per_trigger: int = None
    trigger_type: str = None
    trigger_setting: str = None
    ignore_changes: bool = True
    partition_by: str = None


class ADLS(BaseModel):
    storage_account_key: str = None
    storage_account_name: str = None
    container: str = None
    database: str = None
    table: str = None
    columns: str = None
    filter: str = ""
    mode: str = "overwrite"
    starting_version: int = None
    starting_timestamp: str = None
    max_files_per_trigger: int = None
    max_bytes_per_trigger: int = None
    trigger_type: str = None
    trigger_setting: str = None
    ignore_changes: bool = True
    partition_by: str = None


class Snowflake(BaseModel):
    user: str = None
    password: str = None
    warehouse: str = None
    database: str = None
    scheme: str = None
    table: str = None
    url: str = None
    role: str = None
    mode: str = "overwrite"
    columns: str = None
    trigger_type: str = None
    trigger_setting: str = None
    ignore_changes: bool = True
    clean_slate: bool = True


class PostgresSQL(BaseModel):
    host: str = None
    port: str = None
    database: str = None
    user: str = None
    password: str = None
    table: str = None
    ssl: bool = False
    scheme: str = None
    driver: str = "org.postgresql.Driver"
    columns: str = None
    filter: str = ""
    mode: str = "append"
    clean_slate: bool = True
    trigger_type: str = None
    trigger_setting: str = None
    ignore_changes: bool = True

class MSQLServer(BaseModel):
    host: str = None
    port: str = None
    database: str = None
    user: str = None
    password: str = None
    table: str = None
    scheme: str = None
    columns: str = None
    filter: str = ""
    mode: str = "overwrite"
    clean_slate: bool = True


class API(BaseModel):
    url: str = None
    user: str = None
    password: str = None
    content_type: str = None
    token: str = None
    token_type: str = None
    use_parallelism: str = None
    additional_headers: dict = None
    table: str = "api"


class Email(BaseModel):
    user: str = None
    password: str = None
    sender_account: str = None
    message_body: str = None
    message_body_format: str = "html"
    subject: str = None
    receipts: str = None
    host: str = "smtp-mail.outlook.com"
    port: int = 587
    attachment_name: str = None
    attachment_type: str = None
    attachment_separator: str = None
    table: str = "email"


class File(BaseModel):
    separator: str = None
    type: str = None
    name: str = None
    path: str = None
    table: str = "file"


class BlobStorage(BaseModel):
    storage_account_url: str = None
    storage_account_key: str = None
    container_name: str = None
    blob_name: str = None
    blob_name_is_python: bool = None
    blob_path: str = None
    blob_type: str = None
    blob_separator: str = None
    table: str = "blob_storage"


class FTP(BaseModel):
    host: str = None
    port: int = None
    user: str = None
    password: str = None
    root: str = "/"
    file_name: str = None
    file_type: str = None
    file_separator: str = None
    buffer: int = 32768
    file_datetime_name_format: str = None
    table: str = "ftp"
    trigger_type: str = "once"
    trigger_setting: str = None


class DeltaFeatureStoreParams(BaseModel):
    database: str = None
    table: str = None
    columns: str = None
    filter: str = ""
    mode: str = "overwrite"
    primary_keys: list = None
    timestamp_keys: list = None
    description: str = None
    tags: dict = None
    execution_mode: str = "write"


class Trino(BaseModel):
    host: str = None
    port: int = None
    user: str = None
    password: str = ""
    catalog: str = None
    scheme: str = None
    query: str = None
    mode: str = "overwrite"
    ssl: bool = False
    ssl_mode: str = "NONE"
    ssl_key_store_path: str = "/dbfs/FileStore/db_certificates/trino_server_ca.pem"
    driver: str = "io.trino.jdbc.TrinoDriver"
