# coding=utf-8
"""Allows to query hive from python opening connection to AWS EMR, incl.
tunneling
"""
import boto3


def get_EMR_clusterid(session, emr_name):
    """Returns the cluster_id of the specific EMR cluster by name

    Args:
    session - open boto3 session
    emr_name - the cluster name
    """
    emr_client = session.client('emr')
    emr_list = emr_client.list_clusters()
    working_servers = [el for el in emr_list['Clusters']
                       if el['Status']['State'] == 'WAITING'
                       and el['Name'] == emr_name]
    return working_servers[0]['Id']

def get_EMR_url(session, cluster_id):
    """Returns the cluster url given cluster_id

    Args:
    session - open boto3 session
    cluster_id - the AWS cluster_id
    """
    return session.client('emr').describe_cluster(ClusterId=cluster_id)['Cluster']['MasterPublicDnsName']

def open_tunnel(url, **kwargs):
    from subprocess import Popen, PIPE
    import os
    cmd = '''ssh -i {0} -N -L {3}:{1}:{2} {4}@{1}'''.format(kwargs.get('path_to_key', '~/.ssh/id_rsa.pub'),
                                                            url,
                                                            kwargs.get('destination_port', '10000'),
                                                            kwargs.get('local_port', '10001'),
                                                            kwargs.get('user', 'hadoop'))
    print cmd
    process = Popen(cmd, stdout=PIPE, shell=True, preexec_fn=os.setsid)
    return process

def close_tunnel(process):
    import os, signal
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)

def connect_EMR(func):
    def wrapper(sql, **kwargs):
        from config import CLUSTER_NAME, LOCAL_PORT_HIVE, PROFILE_NAME, KEY_PATH
        from config import SERVER_PORT_HIVE, EC2_USER, LOCAL_HOST_HIVE
        session = boto3.Session(profile_name = PROFILE_NAME)
        cluster_id = get_EMR_clusterid(session, CLUSTER_NAME)
        url = get_EMR_url(session, cluster_id)
        ft_process = None
        try:
            ft_process = open_tunnel(url, path_to_key = KEY_PATH, destination_port = SERVER_PORT_HIVE,
                                    local_port = LOCAL_PORT_HIVE, user = EC2_USER)
            import time
            time.sleep(2)
            result = func(sql, **kwargs)
            return result
        except Exception as e:
            print 'Error %s' % e
        finally:
            close_tunnel(ft_process) if ft_process else False
    return wrapper

def connect_hive(func):#, db_host = 'localhost', port = DEFAULT_LOCAL_PORT):
    """Wrapper to open/close hive connection"""
    def wrapper(sql):
        from pyhive import hive
        #from config import
        conn = None
        try:
            conn = hive.Connection(host = 'localhost', port = '10001')
            result = func(sql, conn = conn)
            return result
        except Exception as e:
            print 'Error %s' % e
        finally:
            conn.close() if conn else False
    return wrapper

@connect_EMR
@connect_hive
def load_hive(sql, **kwargs):
    import pandas as pd
    df = pd.read_sql(sql, kwargs['conn'])
    return df
