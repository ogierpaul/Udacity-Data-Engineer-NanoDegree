from dendutils.config import get_project_config
import logging
from p6_capstone.plugins.operators import AwsTestHook, Ec2Hook
from p6_capstone.plugins.operators import Ec2BashExecutor, RedShiftExecutor, CopyFromS3
from dendutils.redshift import get_conn
from p6_capstone.plugins.helpers.decp import DecpInputParams

config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/config/config_path.cfg'
#TODO: Call InputParams as VariableManager

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_format = '%(asctime)s %(filename)s: %(message)s'
    logging.basicConfig(format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    return logger

if __name__ == '__main__':
    logger = get_logger()
    config = get_project_config(config_path)
    aws_credentials = AwsTestHook(region_name=config.get("REGION", "REGION"),
                                  aws_access_key_id=config.get("AWS", "KEY"),
                                  aws_secret_access_key=config.get("AWS", "SECRET")
                                  )
    queries = DecpInputParams(config_path)
    ec2_template = Ec2Hook(tag_key='foo',
                           tag_value='bar',
                           ImageId='ami-0bd39c806c2335b95',
                           KeyName=config.get("EC2", "KEY_PAIR"),
                           InstanceType='t2.micro',
                           SecurityGroupId=config.get("SG", "SG_ID"),
                           IamInstanceProfileName=config.get("IAM", "IAM_EC2_ROLE"),
                           sleep=60
                           )
    ec = Ec2BashExecutor(
        commands=queries.bash_commands(),
        aws_credentials=aws_credentials,
        ec2_template=ec2_template,
        sleep=30,
        retry=15
    )
    ec.execute()
    conn = get_conn(config)
    rs_c = RedShiftExecutor(conn=conn, queries=queries.create_queries())
    rs_c.execute()
    cp_marches = CopyFromS3(
        conn=conn,
        **queries.copy_params_marches()
    )
    cp_marches.execute()
    cp_titulaires = CopyFromS3(
        conn=conn,
        **queries.copy_params_titulaires()
    )
    cp_titulaires.execute()
    rs_s = RedShiftExecutor(conn=conn, queries=queries.staging_queries())
    rs_s.execute()
