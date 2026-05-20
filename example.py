import lib_my

def zen_api_prod(remote_mysql_dbname: str = 'prod_api2', local_mysql_dbname: str = 'zen__prod_api2'):
    obj = lib_my.CopyMysqlDbRemoteToLocal(
        dump_name='zen__prod_api2',

        remote_ssh_hostname='192.168.2.5',
        remote_ssh_username='dev_stem',
        remote_ssh_password='',
        remote_ssh_port=53130,

        remote_ssh_key_filename=r'C:\Users\T\.ssh\key',

        remote_mysql_hostname='ttt.tt.tt',

        remote_mysql_dbname=remote_mysql_dbname,
        remote_mysql_username='admin',
        remote_mysql_password='',

        remote_mysql_dump_compressor='zstd',
        remote_mysql_ignore_tables=[],
        local_mysql_dbname=local_mysql_dbname,
        local_mysql_hostname='192.168.2.87'
    )

    obj.connect()
    obj.dump_remote_and_download()
    obj.unpack()
    obj.restore_local()
