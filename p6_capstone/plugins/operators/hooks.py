class AwsTestHook():
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        pass

class Ec2Hook():
    def __init__(self, ImageId, InstanceType, KeyName, IamInstanceProfileName, SecurityGroupId, \
                 tag_key="", tag_value="", sleep=120):
        self.ImageId = ImageId
        self.InstanceType = InstanceType
        self.KeyName = KeyName
        self.IamInstanceProfileName = IamInstanceProfileName
        self.SecurityGroupId = SecurityGroupId
        self.tag_key = tag_key
        self.tag_value = tag_value
        self.sleep = sleep


