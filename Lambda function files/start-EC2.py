import boto3

def lambda_handler(event, context):
    # Initialize the EC2 client.
    ec2 = boto3.client('ec2')
    
    # List of EC2 instance IDs to be started.
    instances = ['i-092d67c228898fb75']
    
    # Use the EC2 client to start the specified instances.
    ec2.start_instances(InstanceIds=instances)
