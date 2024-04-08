from unittest.mock import MagicMock, patch

import pytest

from vortex.engine.ftp import FTPEngine


@pytest.fixture
def ftp_engine_params():
    return {
        "host": "ftp.example.com",
        "port": 21,
        "user": "test_user",
        "password": "test_password",
        "root": "/data/",
        "file_name": "test_file_",
        "file_datetime_name_format": "%Y%m%d%H%M%S",
        "file_separator": ",",
        "file_type": "csv",
    }


@pytest.fixture
def ftp_engine(ftp_engine_params):
    return FTPEngine(params=ftp_engine_params)


def test_ftp_engine_write_batch(ftp_engine):
    mock_dataframe = MagicMock()
    mock_spark = MagicMock()

    with patch("vortex.engine.ftp.paramiko.Transport") as mock_transport, patch(
        "vortex.engine.ftp.paramiko.SFTPClient.from_transport"
    ) as mock_sftp_client_from_transport, patch(
        "vortex.engine.ftp.BytesIO"
    ) as mock_bytes_io, patch(
        "vortex.engine.ftp.Files.csv"
    ) as mock_files_csv:
        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        mock_sftp_client_instance = MagicMock()
        mock_sftp_client_from_transport.return_value = mock_sftp_client_instance

        mock_files_csv_instance = MagicMock()
        mock_files_csv_instance.encode.return_value = b"some_encoded_data"
        mock_files_csv.return_value = mock_files_csv_instance

        ftp_engine.write_batch(mock_dataframe, mock_spark)

        mock_transport_instance.connect.assert_called_once_with(
            username=ftp_engine._params.user, password=ftp_engine._params.password
        )

        # Check if the first argument of putfo is the expected BytesIO instance
        assert (
            mock_sftp_client_instance.putfo.call_args[0][0]
            == mock_bytes_io.return_value
        )

        # Check if the second argument of putfo (file_path) starts with the expected prefix
        assert mock_sftp_client_instance.putfo.call_args[0][1].startswith(
            f"{ftp_engine._params.root}{ftp_engine._params.file_name}"
        )

        mock_sftp_client_instance.close.assert_called_once()
        mock_transport_instance.close.assert_called_once()
