#!/usr/bin/env python3

# SFTP client

import logging
import paramiko
import stat
import os

import worker_functions.connection_aux_functions as cf

class SFTP_Client:
    """
    SFTP Client
    """

    def __init__(self, sftp_servers=[], username=None, password=None, logger = logging.getLogger(__name__)):
        self.sftp_servers = sftp_servers
        self.logger = logger

        self.sftp_connection = None
        self.ssh_connection = None

        if username:
            self.username = username
            self.password = password
        else:
            self.username = 'pero'
            self.password = 'pero'
    
    def sftp_connect(self):
        """
        Connect to server
        :raise: ConnectionError if connection to all SFTP servers fails
        """
        # setup client for connection
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # connect to firs server avaliable
        for server in self.sftp_servers:
            try:
                self.logger.info('Connecting to SFTP server {}'.format(cf.host_port_to_string(server)))
                # get SSH connection
                self.ssh_connection.connect(
                    hostname=server['host'],
                    port=server['port'],
                    username=self.username,
                    password=self.password,
                    allow_agent=False,
                    look_for_keys=False
                )
                self.logger.debug('Opening SFTP channel')
                # get SFTP (SSH File Transfer Protocol) connection
                self.sftp_connection = self.ssh_connection.open_sftp()
            except paramiko.AuthenticationException:
                self.logger.error('Wrong authentication credentials!')
                continue
            except Exception as e:
                self.logger.error('Failed to connect to SFTP server {server}! Received error:\n{error}'.format(
                    server = cf.host_port_to_string(server),
                    error = e
                ))
                continue
            else:
                self.logger.info('SFTP connection established successfully!')
                return
        
        # failed to connect
        raise ConnectionError('Failed to connect to SFTP servers!')
    
    def sftp_ensure_dir(self, path):
        """
        Ensures that target directory specified by path exists.
        :param path: directory to create / check for existence
        :raise ValueError: if relative path is suplied
        :raise PermissionError: if sftp user has insuficient permissions
        :raise FileExistsError: if file with conflicting name exists
        """
        
        # root dir always exists
        if path == '/':
            return
        
        # path not specified (does not start at root)
        if path == '':
            raise ValueError('Relative path is not supported!')

        try:
            # check if file/directory exists on given path
            stats = self.sftp_connection.stat(path)
        except FileNotFoundError:
            # create path to current folder
            self.sftp_ensure_dir(os.path.split(path)[0])
            # create current folder
            self.sftp_connection.mkdir(path)
        else:
            # check if file found on given path is directory
            if not stat.S_ISDIR(stats.st_mode):
                raise FileExistsError(
                    f'Failed to create directory, {path} is a file!'
                )

    def sftp_remove_empty_dir(self, path):
        """
        Removes empty directory and all empty parent directories given by path.
        :param path: directory to remove
        :raise ValueError: if path is empty or relative path is used
        """

        # root dir cannot be removed
        if path == '/':
            return
        
        # path not specified
        if path == '':
            raise ValueError('Path cannot be empty!')
        
        # remove empty dir
        try:
            self.sftp_connection.rmdir(path)
        except FileNotFoundError:
            # directory does not exists - nothing to remove
            return
        except PermissionError:
            # usually happens at the lowes level of direcotry structure
            return
        except OSError:
            # directory not empty
            return
        else:
            # remove parent directory if its now empty
            self.sftp_remove_empty_dir(os.path.split(path)[0])

    def sftp_remove_file(self, path):
        """
        Removes file given by path from sftp server.
        :param path: path to file to remove
        :raise FileNotFoundError: if file is not found
        :raise OSError: if target is directory not a file
        :raise PermissionError: if unsuficient permissions to remove the file
        """
        self.sftp_connection.remove(path)

    def sftp_put(self, local_file, remote_file):
        """
        Uploads local file to SFTP server to given path
        :param local_file: path to local file
        :param remote_file: path on SFTP server where local_file will be uploaded
        """
        self.sftp_connection.put(local_file, remote_file)

    def sftp_get(self, remote_file, local_file):
        """
        Receive file from SFTP server
        :param remote_file: file to receive
        :param local_file: path to local file where received file will be stored
        """
        self.sftp_connection.get(remote_file, local_file)

    def sftp_disconnect(self):
        """
        Disconnect from server
        """
        if self.sftp_connection:
            self.sftp_connection.close()
        if self.ssh_connection:
            self.ssh_connection.close()
    
    def __del__(self):
        self.sftp_disconnect()
