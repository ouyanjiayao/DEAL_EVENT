import zmail
class SendMail:
    def __init__(self):
        self.mail_content = None
    def send_email_to(self,subject,content_text,attachments,receivers,cc):
        mail_content = {
            'subject': subject,
            'content_text': content_text,
            'attachments': attachments
        }

        server = zmail.server('root@163.com', '123456')
       
        server.send_mail(receivers, mail_content, cc)
        print('send success')
