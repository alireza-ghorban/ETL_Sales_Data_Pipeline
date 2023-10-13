import boto3
from botocore.exceptions import ClientError

def send_email():
    # "From" email address; this must be verified with Amazon SES.
    SENDER = "alyresa@gmail.com"

    # "To" email address; if your account is in the SES sandbox, this must also be verified.
    RECIPIENT = "alyresa@gmail.com"

    # AWS Region for the Amazon SES service.
    AWS_REGION = "us-east-1"

    # Email subject.
    SUBJECT = "Files missing in S3 bucket"

    # Email body text for recipients using non-HTML email clients.
    BODY_TEXT = ("Files missing in AWS S3 bucket. Please check Snowflake task.")

    # Character encoding for the email.
    CHARSET = "UTF-8"

    # Initialize the Amazon SES client.
    client = boto3.client('ses', region_name=AWS_REGION)

    # Try sending the email.
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [RECIPIENT],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER
        )
    # Handle potential errors during the email sending process.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        # Print the success message and message ID if email sent successfully.
        print("Email sent! Message ID:", response['MessageId'])
