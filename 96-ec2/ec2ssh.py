from dendutils import create_vm, filter_per_tag, execute_shell_script
from dendutils.ec2 import show_all_instances_status
import boto3
import configparser
import os
import time
os.chdir('/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/')
from dendutils import get_myip
from botocore.exceptions import ClientError

config_path = 'config_path'
config = configparser.ConfigParser()
config.read(config_path)
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
REGION = config.get("REGION", "REGION")
IMAGE_ID = config.get("EC2", "IMAGE_ID")

INSTANCE_TYPE = config.get("EC2", "INSTANCE_TYPE")
KEY_NAME = config.get("EC2", "KEY_NAME")
IAM_ROLE = config.get("IAM", "IAM_ROLE")
SG_ID = config.get("SG", "SG_ID")
TAG_KEY = "name"
TAG_VALUE = "runvalue"
sleep = 30

command_content = """
INPUT HERE OUTPUT CONTENT
"""
sh_all = [s for s in command_content.split('\n') if s.strip()!='']



if __name__ == '__main__':
    ec2c = boto3.client('ec2',
                         region_name=REGION,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )
    ec2r = boto3.resource('ec2',
                         region_name=REGION,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                  )
    print(f'starting vm.. wait {sleep} seconds')
    i = create_vm(ec2r, IMAGE_ID, KEY_NAME, INSTANCE_TYPE, SG_ID, IAM_ROLE, TAG_KEY, TAG_VALUE)
    time.sleep(sleep)
    j = i[0]
    i_id = j.id
    print("showing instances:")
    o = show_all_instances_status(ec2c)
    print(o)
    print(f"using instance {i_id}")
    print("launching scripts")
    try:
        o = execute_shell_script(i_id=i_id, config=config, commands=sh_all, sleep=30)
        print(o['StandardOutputContent'])
    except ClientError as e:
        print(e)
    try:
        o = j.terminate()
        print(f"terminating instance {j.id}")
        print(o)
    except ClientError as e:
        print(e)





