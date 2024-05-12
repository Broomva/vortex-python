from vortex.engine import Engine
from vortex.datamodels.engine import Email, File

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE
from email.mime.text import MIMEText
import smtplib
from datetime import date
from vortex.utils.files import Files


class EmailEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = Email(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        file_params = {
            "separator": self._params.attachment_separator,
            "type": self._params.attachment_type,
            "name": self._params.attachment_name,
        }

        params = File(**file_params)
        attachment = getattr(Files, self._params.attachment_type)(
            dataframe=dataframe, spark=spark, mode="out", params=params
        )

        multipart = MIMEMultipart()
        multipart["From"] = self._params.sender_account
        multipart["To"] = COMMASPACE.join(self._params.receipts.split(";"))
        multipart[
            "Subject"
        ] = f"""{date.today().strftime("%Y%m%d")}_{self._params.subject}"""
        attachment = MIMEApplication(attachment)
        attachment["Content-Disposition"] = 'attachment; filename=" {}"'.format(
            f"{str(date.today())}_{self._params.attachment_name}.{self._params.attachment_type}"
        )
        multipart.attach(attachment)
        multipart.attach(
            MIMEText(self._params.message_body, self._params.message_body_format)
        )
        server = smtplib.SMTP(self._params.host, self._params.port)
        server.starttls()
        server.login(self._params.user, self._params.password)
        server.sendmail(
            self._params.sender_account,
            self._params.receipts.split(";"),
            multipart.as_string(),
        )
        server.quit()

    def read_batch(self, spark):
        pass
