import os
import json
import base64

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from PIL import Image
from io import BytesIO

from email.header import decode_header
from email import policy
from email.parser import BytesParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


s3 = boto3.client("s3", region_name="us-east-1")
ses = boto3.client("ses", region_name="us-east-1")
secretsmanager = boto3.client("secretsmanager", region_name="us-east-1")

get_secret_value_response = secretsmanager.get_secret_value(SecretId="reportes")
secrets = get_secret_value_response['SecretString'] if 'SecretString' in get_secret_value_response else base64.b64decode(get_secret_value_response['SecretBinary'])
secrets = json.loads(secrets)


CHARSET = "utf-8"

SENDER = os.getenv("SENDER")

ADMIN_RECIPIENTS = os.getenv("ADMIN_RECIPIENTS")

VIEWER_EMAIL = os.getenv("VIEWER_EMAIL")
VIEWER_PASS = secrets.get("VIEWER_PASS")

AVANCE_COSECHA = os.getenv("AVANCE_COSECHA")
AVANCE_COSECHA_URL = os.getenv("AVANCE_COSECHA_URL") 
AVANCE_COSECHA_RECIPIENTS = os.getenv("AVANCE_COSECHA_RECIPIENTS") 

CAMIONES_RECHAZADOS = os.getenv("CAMIONES_RECHAZADOS")
CAMIONES_RECHAZADOS_URL = os.getenv("CAMIONES_RECHAZADOS_URL") 
CAMIONES_RECHAZADOS_RECIPIENTS = os.getenv("CAMIONES_RECHAZADOS_RECIPIENTS") 


def build_emails(recipients, subject, body_text, body_html, body_img=None):
    
    new_emails = []
    
    for recipient in recipients:
        email_root = MIMEMultipart("related")
        email_root["From"] = SENDER
        email_root["To"] = recipient
        email_root["Subject"] = subject
        
        msg_alternative = MIMEMultipart("alternative")
        
        if body_text is not None:
            textpart = MIMEText(body_text.encode(CHARSET), "plain", CHARSET)
            msg_alternative.attach(textpart)

        if body_html is not None:
            htmlpart = MIMEText(body_html.encode(CHARSET), "html", CHARSET)
            msg_alternative.attach(htmlpart)
            
        email_root.attach(msg_alternative)
        
        if body_img is not None:
            msgImage = MIMEImage(body_img)
            msgImage.add_header('Content-ID', '<image1>')
            msgImage.add_header('Content-Disposition', "attachment; filename= dashboard.jpg")
            email_root.attach(msgImage)
            
        new_emails.append(email_root)
    
    return new_emails


def convert_to_jpeg(payload):
    
    img = Image.open(BytesIO(payload))
    img = img.convert('RGB')
    
    with BytesIO() as f:
        img.save(f, format='JPEG')
        return f.getvalue()
        

def avance_de_cosecha(old_email):
    
    recipients = [email.strip() for email in AVANCE_COSECHA_RECIPIENTS.split(',')]
    
    SUBJECT = "Avance de cosecha"

    body_img = None
    for part in old_email.iter_attachments():
        filename = (part.get_filename())
        if filename == AVANCE_COSECHA + ".png":
            body_img = convert_to_jpeg(part.get_payload(decode=True))
            break
    
    body_html = """
    <html>
        <head></head>
        <body>
            <img width="540" src="cid:image1" />
            <p>
                <b>Usuario:</b> {}
            </p>
            <p>
                <b>Contraseña:</b> {}</br>
            </p>
            <p>
                <b>
                    <a href="{}">Ingresar al reporte</a>
                </b>
            </p>
        </body>
    </html>
    """.format(VIEWER_EMAIL, VIEWER_PASS, AVANCE_COSECHA_URL)
    
    body_text = "Ingresar al reporte: {}".format(AVANCE_COSECHA_URL)

    return build_emails(recipients, SUBJECT, body_text, body_html, body_img)
    

def test(old_email):

    recipients = [email.strip() for email in ADMIN_RECIPIENTS.split(',')]
    
    SUBJECT = "Avance de cosecha"

    body_img = None
    for part in old_email.iter_attachments():
        filename = (part.get_filename())
        if filename == "test.png":
            body_img = convert_to_jpeg(part.get_payload(decode=True))
            break
    
    body_html = """
    <html>
        <head></head>
        <body>
            <img width="540" src="cid:image1" />
            <p>
                <b>Usuario:</b> {}
            </p>
            <p>
                <b>Contraseña:</b> {}</br>
            </p>
            <p>
                <b>
                    <a href="{}">Ingresar al reporte</a>
                </b>
            </p>
        </body>
    </html>
    """.format(VIEWER_EMAIL, VIEWER_PASS, AVANCE_COSECHA_URL)
    
    body_text = "Ingresar al reporte: {}".format(AVANCE_COSECHA_URL)
    
    return build_emails(recipients, SUBJECT, body_text, body_html, body_img)
    
    
def forward_to_admins(old_email, subject):
    
    recipients = [email.strip() for email in ADMIN_RECIPIENTS.split(',')]
    
    part = old_email.get_body("html")
    body_html = part.get_payload(decode=True).decode(encoding=part.get_content_charset()) 

    return build_emails(recipients, subject, None, body_html)


def camiones_rechazados(json_body):

    recipients = [email.strip() for email in CAMIONES_RECHAZADOS_RECIPIENTS.split(',')]

    body_html = """
    <html>
        <head></head>
        <body>
            <div>
                {}
            </div>
            <p>
                <b>Usuario:</b> {}
            </p>
            <p>
                <b>Contraseña:</b> {}</br>
            </p>
            <p>
                <b>
                    <a href="{}">Ingresar al reporte</a>
                </b>
            </p>
        </body>
    </html>
    """.format(json_body["html"], VIEWER_EMAIL, VIEWER_PASS, CAMIONES_RECHAZADOS_URL)
    
    body_text = "Ingresar al reporte: {}".format(CAMIONES_RECHAZADOS_URL)

    return build_emails(recipients, json_body["subject"], body_text, body_html)


def alert_admins(json_body):
    
    recipients = [email.strip() for email in ADMIN_RECIPIENTS.split(',')]
    
    return build_emails(recipients, json_body["subject"], None, json_body["html"])
    

def send_email(raw_email):
    
    try:
        
        response =  ses.send_raw_email(
            Source=SENDER,
            Destinations=[raw_email["To"]],
            RawMessage={
                "Data": raw_email.as_string(),
            }
        )
        
    except ClientError as e:
        return e.response["Error"]["Message"]
    else:
        return "Email sent to {}! Message ID: {}".format(raw_email["To"], response["MessageId"])

def lambda_handler(event, context):
    
    print("Event:", event)
    
    raw_emails = []
    
    if "Records" in event:
        data = s3.get_object(Bucket=event["Records"][0]["s3"]["bucket"]["name"], Key=event["Records"][0]["s3"]["object"]["key"])
        contents = data["Body"].read()
        old_email = BytesParser(policy=policy.default).parsebytes(contents)
    
        subject, encoding = decode_header(old_email.get("subject"))[0]
        if isinstance(subject, bytes):
            subject = subject.decode(encoding)
        print(subject)
        
        if subject == AVANCE_COSECHA:
            raw_emails = avance_de_cosecha(old_email)
        elif subject == "Test":
            raw_emails = test(old_email)
        else:
            raw_emails = forward_to_admins(old_email, subject)
    elif "body" in event:

        if not "headers" in event or event["headers"].get("secret") != secrets.get("aws_lambda_reportes_invoker_secret"):
            return 'Unauthorized!'
            
        json_body = json.loads(event["body"])
        
        if json_body["subject"] == CAMIONES_RECHAZADOS:
            raw_emails = camiones_rechazados(json_body)
        else:
            raw_emails = alert_admins(json_body)
        
    for raw_email in raw_emails:
        print(send_email(raw_email))
    
    return 'Done!'
