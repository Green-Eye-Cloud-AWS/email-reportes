import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from datetime import datetime
import os
import email
from email import policy
from email.parser import BytesParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


s3 = boto3.client("s3", region_name="us-east-1")
ses = boto3.client("ses", region_name="us-east-1")

CHARSET = "utf-8"

SENDER = os.getenv("SENDER")
PNG_BUCKET = os.getenv("PNG_BUCKET")
ADMIN_RECIPIENTS = os.getenv("ADMINS_EMAIL")
VIEWER_EMAIL = os.getenv("VIEWER_EMAIL")
VIEWER_PASS = os.getenv("VIEWER_PASS")

AVANCE_COSECHA = os.getenv("AVANCE_COSECHA")
AVANCE_COSECHA_URL = os.getenv("AVANCE_COSECHA_URL") 
AVANCE_COSECHA_RECIPIENTS = os.getenv("AVANCE_COSECHA_RECIPIENTS") 


def build_email(recipients, subject, body_text, body_html):
    
    new_email = MIMEMultipart("mixed")
    new_email["From"] = SENDER
    new_email["To"] = ", ".join(recipients)
    new_email["Subject"] = subject
    
    msg_body = MIMEMultipart("alternative")
    
    if body_text is not None:
        textpart = MIMEText(body_text.encode(CHARSET), "plain", CHARSET)
        msg_body.attach(textpart)
        
    if body_html is not None:
        htmlpart = MIMEText(body_html.encode(CHARSET), "html", CHARSET)
        msg_body.attach(htmlpart)
    
    new_email.attach(msg_body)
    
    return new_email
    

def avance_de_cosecha(old_email):
    
    RECIPIENTS = [email.strip() for email in AVANCE_COSECHA_RECIPIENTS.split(',')]
    
    SUBJECT = "Avance de cosecha"

    key = datetime.now().strftime("%Y%d%m%H%M%S.png")
    
    for part in old_email.iter_attachments():
        filename = (part.get_filename())
        if filename == AVANCE_COSECHA + ".png":
            s3.put_object(Body=part.get_payload(decode=True), Bucket=PNG_BUCKET, Key=key, ContentType="image/png")
            break
    
    url_img = "https://{}.s3.amazonaws.com/{}".format(PNG_BUCKET, key)
    
    body_html = """\
    <html>
        <head></head>
        <body>
            <img width="540" src="{}" />
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
    """.format(url_img, VIEWER_EMAIL, VIEWER_PASS, AVANCE_COSECHA_URL)
    
    body_text = "Ingresar al reporte: {}".format(AVANCE_COSECHA_URL)

    new_email = build_email(RECIPIENTS, SUBJECT, body_text, body_html)

    return RECIPIENTS, new_email
    
    
def forward_to_admin(old_email, subject):
    
    RECIPIENTS = [email.strip() for email in ADMIN_RECIPIENTS.split(',')]
    
    part = old_email.get_body("html")
    body_html = part.get_payload(decode=True).decode(encoding=part.get_content_charset()) 

    new_email = build_email(RECIPIENTS, subject, None, body_html)
     
    return RECIPIENTS, new_email
    
    
def send_email(recipients, raw_email):
    
    try:
        
        response =  ses.send_raw_email(
            Source=SENDER,
            Destinations=recipients,
            RawMessage={
                "Data": raw_email.as_string(),
            }
        )
        
    except ClientError as e:
        return e.response["Error"]["Message"]
    else:
        return "Email sent! Message ID: {}".format(response["MessageId"])

def lambda_handler(event, context):

    data = s3.get_object(Bucket=event["Records"][0]["s3"]["bucket"]["name"], Key=event["Records"][0]["s3"]["object"]["key"])
    contents = data["Body"].read()
    old_email = BytesParser(policy=policy.default).parsebytes(contents)

    subject, encoding = email.header.decode_header(old_email.get("subject"))[0]
    if isinstance(subject, bytes):
        subject = subject.decode(encoding)
    print(subject)
    
    if subject == AVANCE_COSECHA:
        recipients, raw_email = avance_de_cosecha(old_email)
    else:
        recipients, raw_email = forward_to_admin(old_email, subject)
    
    print(send_email(recipients, raw_email))
