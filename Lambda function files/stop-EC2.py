import boto3

def lambda_handler(event, context):
    # Initialize the EC2 client.
    ec2 = boto3.client('ec2')
    
    # List of EC2 instance IDs to be stopped.
    instances = ['i-092d67c228898fb75']
    
    # Use the EC2 client to stop the specified instances.
    ec2.stop_instances(InstanceIds=instances)
