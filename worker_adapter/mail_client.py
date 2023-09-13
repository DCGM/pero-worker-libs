#!/usr/bin/env python3

from drymail import SMTPMailer, Message
import logging
import traceback
import datetime


class MailClient:
    """
    Mail client providing send_mail function for worker adapter.
    """
    def __init__(self,
        user:str=None,
        password:str=None,
        receivers:list[str]=None,
        host:str=None,
        mail_interval=0,
        logger=logging.getLogger(__name__)
    ):
        """
        Init - if arguments are omited, then send_mail function have to be used
        with all arguments.
        :param user: senders user name (in format username@subdomain.domain)
        :param password: password for server authentication
        :param receivers: list of email receivers
        :param host: name of the SMTP host name
        :param mail_interval: time after which another mail can be sent in
            seconds (default = 0 -- immediately)
        :param logger: logger instance to use
        """
        self.user = user
        self.password = password
        self.receivers = receivers
        self.host = host
        self.mail_interval = datetime.timedelta(seconds=mail_interval)
        self.logger = logger

        self.last_mail_time = datetime.datetime(
            year=1970,
            month=1,
            day=1,
            tzinfo=datetime.timezone.utc
        )
    
    @staticmethod
    def send_mail(
        subject:str,
        body:str,
        user:str,
        password:str,
        receivers:list[str],
        host:str
    ):
        """
        Send mail from user to receivers.
        :param subject: email subject
        :param body: email body
        :param user: senders user name (in format username@subdomain.domain)
        :param password: user's password
        :param receivers: list of receivers
        :param host: SMTP host name
        """
        if password:
            client = SMTPMailer(
                host=host,
                user=user,
                password=password,
                tls=True
            )
        else:
            client = SMTPMailer(host=host)
        
        message = Message(
            subject=subject,
            sender=user,
            receivers=receivers,
            html=body
        )
        client.send(message)
    
    def send_mail_notification(self, subject:str, body:str):
        """
        Send mail notification with preconfigured arguments.
        Preconfigured arguments have to be supplied on object init.
        Email can be send using this method only once per 'mail_interval'
        seconds (mail_interval is set on object init). If this method is called
        sooner than timeout exceeds, no email is sent and method does nothing.
        This is to prevent spamming when error notification is send periodically
        and error occures for long time.
        :param subject: email subject
        :param body: email body
        :nothrow
        """
        # guard to check for incomplete arguments
        if not self.user or not self.password  or \
           not self.receivers or not self.host:
            self.logger.error('Preconfigured arguemnts are empty!')
            self.logger.error(
                'Preconfigured arguments must be supplied on '
                f'{self.__class__.__name__} init!'
            )
            return
        
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        if timestamp - self.last_mail_time < self.mail_interval:
            return
        
        self.last_mail_time = timestamp
        
        try:
            self.send_mail(
                subject=subject,
                body=body.replace('\n', '<br>'),
                user=self.user,
                password=self.password,
                receivers=self.receivers,
                host=self.host
            )
        except Exception:
            self.logger.error('Failed to send notificaton email!')
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
