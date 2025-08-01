import ftplib
import os
import pathlib
import stat as stat_module
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import paramiko
from omegaconf import DictConfig  # To use with Hydra configuration

from nrcan_etl_toolbox.etl_logging import CustomLogger


class FTP_SERVER_TYPE(Enum):
    """Enum for FTP server types"""

    FTP = "ftp"
    SFTP = "sftp"


ftp_logger = CustomLogger("FTPDownloader")


@dataclass
class FTPServerConfig:
    """Configuration class for server connection"""

    ftp_protocol: str
    ftp_host: str
    ftp_user: Optional[str] = None
    ftp_password: Optional[str] = None
    ftp_key_file_path: Optional[str] = None
    ftp_port: Optional[int] = None
    ftp_timeout: Optional[int] = None


class BaseDownloader(ABC):
    """Abstract class for downloaders"""

    @abstractmethod
    def directory_exists(self, directory_path):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def list_files(self, directory="/"):
        pass

    @abstractmethod
    def download_file(self, remote_file, local_path, file_filter=None):
        pass

    @abstractmethod
    def download_multiple_files(self, file_list, local_directory):
        pass

    @abstractmethod
    def disconnect(self):
        pass


class FTPDownloader(BaseDownloader):
    def __init__(self, host, username=None, password=None, port=21):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.ftp = None

    def directory_exists(self, directory_path):
        """Teste si un dossier existe sur le serveur FTP"""
        if not self.ftp:
            return False

        try:
            current_dir = self.ftp.pwd()
            self.ftp.cwd(directory_path)
            self.ftp.cwd(current_dir)  # Retour au dossier précédent
            return True
        except ftplib.all_errors:
            return False

    def connect(self):
        """Establish the connection to the FTP server"""
        try:
            self.ftp = ftplib.FTP()
            self.ftp.connect(self.host, self.port)

            if self.username and self.password:
                self.ftp.login(self.username, self.password)
            else:
                self.ftp.login()  # Anonymous connection

            ftp_logger.info(f"FTP connection successful to {self.host}")
            return True
        except ftplib.all_errors as e:
            ftp_logger.info(f"FTP connection error: {e}")
            return False

    def list_files(self, directory="/"):
        """List files in a directory"""
        if not self.ftp:
            return []

        try:
            self.ftp.cwd(directory)
            files = self.ftp.nlst()
            return files
        except ftplib.all_errors as e:
            ftp_logger.error(f"Error during listing: {e}")
            return []

    def download_file(self, remote_file, local_path, file_filter=None):
        """Download a file from the FTP server"""
        if not self.ftp:
            return False

        try:
            # Si c'est un dossier, appliquer le filtre
            if self.directory_exists(remote_file):
                remote_folder_name = pathlib.Path(remote_file).name
                local_dir = pathlib.Path(local_path) / remote_folder_name
                local_dir.mkdir(parents=True, exist_ok=True)

                self._download_directory_recursive_ftp(remote_file, local_dir, file_filter)
                ftp_logger.info(f"FTP directory downloaded: {remote_file} -> {local_dir}")
                return True
            else:
                # Pour un fichier unique, vérifier le filtre
                if file_filter and file_filter not in remote_file:
                    ftp_logger.info(f"File skipped (filter): {remote_file}")
                    return True

                local_file_path = pathlib.Path(local_path)
                local_file_path.parent.mkdir(parents=True, exist_ok=True)

                with open(local_file_path, "wb") as local_file:
                    self.ftp.retrbinary(f"RETR {remote_file}", local_file.write)

                ftp_logger.info(f"FTP file downloaded: {remote_file} -> {local_path}")
                return True
        except ftplib.all_errors as e:
            ftp_logger.error(f"FTP download error: {e}")
            return False

    def _download_directory_recursive_ftp(self, remote_dir, local_dir, file_filter=None):
        """Télécharge récursivement un dossier distant FTP avec filtre"""
        try:
            files = self.ftp.nlst(remote_dir)
            for file_path in files:
                filename = os.path.basename(file_path)
                local_file_path = local_dir / filename

                if self.directory_exists(file_path):
                    local_file_path.mkdir(exist_ok=True)
                    self._download_directory_recursive_ftp(file_path, local_file_path, file_filter)
                else:
                    # Appliquer le filtre
                    if file_filter is None or file_filter in filename:
                        with open(local_file_path, "wb") as local_file:
                            self.ftp.retrbinary(f"RETR {file_path}", local_file.write)
                    else:
                        ftp_logger.info(f"File skipped (filter): {filename}")
        except ftplib.all_errors as e:
            ftp_logger.error(f"Error downloading directory {remote_dir}: {e}")

    def download_multiple_files(self, file_list, local_directory):
        """Download multiple files"""
        success_count = 0
        for remote_file in file_list:
            filename = os.path.basename(remote_file)
            local_path = os.path.join(local_directory, filename)
            if self.download_file(remote_file, local_path):
                success_count += 1
        ftp_logger.info(f"{success_count}/{len(file_list)} FTP files downloaded")
        return success_count

    def disconnect(self):
        """Close the FTP connection"""
        if self.ftp:
            self.ftp.quit()
            ftp_logger.info("FTP connection closed")


class SFTPDownloader(BaseDownloader):
    def __init__(self, host, username, key_file_path=None, password=None, port=22):
        self.host = host
        self.username = username
        self.key_file_path = key_file_path
        self.password = password
        self.port = port
        self.ssh_client = None
        self.sftp_client = None

    def directory_exists(self, directory_path):
        """Teste si un dossier existe sur le serveur SFTP"""
        if not self.sftp_client:
            return False

        try:
            stat = self.sftp_client.stat(directory_path)
            return stat_module.S_ISDIR(stat.st_mode)
        except Exception:
            return False

    def connect(self):
        """Establish the connection to the SFTP server"""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.key_file_path:
                # Key-based authentication
                private_key = paramiko.RSAKey.from_private_key_file(self.key_file_path)
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    pkey=private_key,
                )
            else:
                # Password authentication
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )

            self.sftp_client = self.ssh_client.open_sftp()
            ftp_logger.info(f"SFTP connection successful to {self.host}")
            return True

        except Exception as e:
            ftp_logger.error(f"SFTP connection error: {e}")
            return False

    def list_files(self, directory="/"):
        """List files in a directory"""
        if not self.sftp_client:
            return []
        try:
            files = self.sftp_client.listdir(directory)
            return files
        except Exception as e:
            ftp_logger.error(f"SFTP listing error: {e}")
            return []

    def download_file(self, remote_file, local_path, file_filter=None):
        """Download a file from the SFTP server"""
        if not self.sftp_client:
            return False

        try:
            # Verify if the directory exists and it should be downloaded recursively
            if self.directory_exists(remote_file):
                remote_folder_name = pathlib.Path(remote_file).name
                local_dir = pathlib.Path(local_path) / remote_folder_name

                # Create local directory if it doesn't exist
                local_dir.mkdir(parents=True, exist_ok=True)
                self._download_directory_recursive(remote_file, local_dir, file_filter)
                ftp_logger.info(f"SFTP directory downloaded: {remote_file} -> {local_dir}")
                return True
            else:
                # For a single file, check the filter
                if file_filter and file_filter not in remote_file:
                    ftp_logger.info(f"File skipped (filter): {remote_file}")
                    return True

                local_file_path = pathlib.Path(local_path)
                local_file_path.parent.mkdir(parents=True, exist_ok=True)

                self.sftp_client.get(remote_file, str(local_file_path))
                ftp_logger.info(f"SFTP file downloaded: {remote_file} -> {local_path}")
                return True

        except Exception as e:
            ftp_logger.error(f"SFTP download error: {e}")
            return False

    def _download_directory_recursive(self, remote_dir, local_dir, file_filter=None):
        """Télécharge récursivement un dossier distant avec filtre"""
        try:
            files = self.sftp_client.listdir(remote_dir)
            for file in files:
                remote_file_path = f"{remote_dir}/{file}"
                local_file_path = local_dir / file

                if self.directory_exists(remote_file_path):
                    local_file_path.mkdir(exist_ok=True)
                    self._download_directory_recursive(remote_file_path, local_file_path, file_filter)
                else:
                    # Check if the file matches the filter
                    if file_filter is None or file_filter in file:
                        self.sftp_client.get(remote_file_path, str(local_file_path))
                    else:
                        ftp_logger.info(f"File skipped (filter): {file}")

        except Exception as e:
            ftp_logger.error(f"Error downloading directory {remote_dir}: {e}")

    def download_multiple_files(self, file_list, local_directory):
        """Download multiple files"""
        success_count = 0
        Path(local_directory).mkdir(parents=True, exist_ok=True)

        for remote_file in file_list:
            filename = os.path.basename(remote_file)
            local_path = os.path.join(local_directory, filename)
            if self.download_file(remote_file, local_path):
                success_count += 1

        ftp_logger.info(f"{success_count}/{len(file_list)} SFTP files downloaded")
        return success_count

    def disconnect(self):
        """Close the SFTP connection"""
        if self.sftp_client:
            self.sftp_client.close()
        if self.ssh_client:
            self.ssh_client.close()
        ftp_logger.info("SFTP connection closed")


class DownloaderFactory:
    """Factory to create the right type of downloader"""

    @staticmethod
    def create_downloader(
        server_type: FTP_SERVER_TYPE,
        host,
        username=None,
        password=None,
        key_file_path=None,
        port=None,
    ):
        """
        Create the right downloader according to server type

        Args:
            server_type: FTP or SFTP
            host: server address
            username: username
            password: password (optional for FTP, required for SFTP without key)
            key_file_path: path to key file (for SFTP)
            port: connection port (21 for FTP, 22 for SFTP by default)
        """
        match server_type:
            case FTP_SERVER_TYPE.FTP:
                port = port or 21
                return FTPDownloader(host, username, password, port)
            case FTP_SERVER_TYPE.SFTP:
                port = port or 22
                return SFTPDownloader(host, username, key_file_path, password, port)
            case _:
                raise ValueError(f"Unsupported server type: {server_type}")

    @staticmethod
    def create_from_config(config: DictConfig | dict) -> BaseDownloader:
        """
        Create downloader from Hydra configuration or dictionary.
        The key elements in the config should be:
            - ftp_protocol: 'ftp' or 'sftp'
            - ftp_host: server address
            - ftp_user: username (optional for FTP, required for SFTP without key)
            - ftp_password: password (optional for FTP, required for SFTP without key)
            - ftp_key_file_path: path to key file (for SFTP)
            - ftp_port: connection port (21 for FTP, 22 for SFTP by default)
        If using Hydra, the configuration should be structured as follows:
        server:
            ftp_protocol: 'ftp'  # or 'sftp'
            ftp_host: 'ftp.example.com'
            ftp_user: 'user'  # optional for FTP
            ftp_password: 'pass'  # optional for FTP, required for SFTP without key
            ftp_key_file_path: '/path/to/key.pem'  # required for SFTP if not using password
            ftp_port: 21  # optional, defaults to 21 for FTP and 22 for SFTP

        Args:
            config: Hydra configuration object or a dictionary containing the FTP server configuration.
        """
        try:
            match config:
                case DictConfig():
                    # Convert DictConfig to dict
                    server_config = FTPServerConfig(**config.server)
                case dict():
                    server_config = FTPServerConfig(**config)
                case _:
                    raise TypeError("Expected a DictConfig or dict")
        except KeyError as e:
            raise KeyError(f"Missing required configuration key: {e}") from e
        except Exception as e:
            raise ValueError(f"Invalid configuration or Exception in config: {e}") from e
        server_type = FTP_SERVER_TYPE(server_config.ftp_protocol.lower())

        return DownloaderFactory.create_downloader(
            server_type=server_type,
            host=server_config.ftp_host,
            username=server_config.ftp_user,
            password=server_config.ftp_password,
            key_file_path=server_config.ftp_key_file_path,
            port=server_config.ftp_port,
        )
