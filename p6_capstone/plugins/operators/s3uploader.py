from dendutils.s3 import upload_file

class S3Uploader():
    """
    - Uploads local file to s3
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, local_path, s3_bucket, object_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.local_path = local_path
        self.s3_bucket = s3_bucket
        self.object_name = object_name
        pass

    def execute(self, context=None):
        o = upload_file(
            aws_secret_access_key=self.aws_secret_access_key,
            aws_access_key_id=self.aws_access_key_id,
            region_name=self.region_name,
            file_path=self.local_path,
            bucket=self.s3_bucket,
            object_name=self.object_name
        )
        return None
