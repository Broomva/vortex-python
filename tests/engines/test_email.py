from unittest.mock import MagicMock, patch

import pytest

from vortex.datamodels.engine import Email
from vortex.engine.email import EmailEngine


@pytest.fixture
def email_params():
    return {
        "sender_account": "sender@example.com",
        "receipts": "recipient1@example.com;recipient2@example.com",
        "subject": "Test Email",
        "attachment_separator": ",",
        "attachment_type": "csv",
        "attachment_name": "attachment",
        "host": "smtp.example.com",
        "port": 587,
        "user": "email_user",
        "password": "email_password",
        "message_body": "This is a test email",
        "message_body_format": "plain",
    }


@pytest.fixture
def email_engine(email_params):
    return EmailEngine(params=email_params)


def test_email_engine_init(email_engine, email_params):
    assert isinstance(email_engine._params, Email)
    assert email_engine._params.sender_account == email_params["sender_account"]


def test_email_engine_write_batch(email_engine):
    mock_dataframe = MagicMock()
    mock_spark = MagicMock()

    with patch("vortex.engine.email.Files.csv") as mock_csv, patch(
        "vortex.engine.email.MIMEApplication"
    ) as mock_mime_application, patch("vortex.engine.email.smtplib.SMTP") as mock_smtp:
        mock_mime_application_instance = MagicMock()
        mock_mime_application_instance.get_content_maintype.return_value = "application"
        mock_mime_application_instance.get_content_subtype.return_value = "octet-stream"
        mock_mime_application_instance.get_payload.return_value = "dummy_payload"
        mock_mime_application.return_value = mock_mime_application_instance

        email_engine.write_batch(mock_dataframe, mock_spark)

    mock_csv.assert_called_once()
    mock_mime_application.assert_called_once()
    mock_smtp.assert_called_once_with(
        email_engine._params.host, email_engine._params.port
    )
