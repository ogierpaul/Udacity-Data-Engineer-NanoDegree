import configparser
import logging
import os
def get_project_config(path):
    """
    Read the config file containing the paths to the AWS Credentials path and the Project Config file
    ConfigParser reads each file successively
    Two files are separated because aws credentials and project configuration might be stored in a different places
    Args:
        path (str): Config file storing paths of AWS credentials config file and Project Config File

    Returns:
        configparser.ConfigParser: Configuration file
    """
    logger = logging.getLogger()
    logger.info("Reading config")
    config = configparser.ConfigParser()
    config.read(path)
    aws_credentials_path = config.get("PATHS", "AWS_CREDENTIALS_PATH")
    logger.info(f"aws credentials file at {aws_credentials_path} exists: {os.path.isfile(aws_credentials_path)}")
    project_config_path = config.get("PATHS", "PROJECT_CONFIG_PATH")
    logger.info(f"project file at {project_config_path} exists: {os.path.isfile(project_config_path)}")
    config.read(aws_credentials_path)
    config.read(project_config_path)
    logger.info("Done reading config")
    return config

def concat_path(file, filename):
    """
    Concatenate the absolute path to file and filename.
    To use with __file__
    Args:
        file:
        filename (str):

    Returns:
        str:
    """
    fp = os.path.join(
        os.path.dirname(os.path.abspath(file)),
        filename
    )
    return fp
