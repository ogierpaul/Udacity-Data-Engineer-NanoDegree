from p3_datawarehouse_aws_redshift.src import create_main
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

if __name__ == '__main__':
    create_main(config)