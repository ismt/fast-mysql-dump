import lib_my


# import winsound


def test_site():
    obj = lib_my.CopyMysqlDbRemoteToLocal()

    obj.remote_ssh_hostname = ''
    obj.remote_ssh_username = ''
    obj.remote_ssh_password = ''
    obj.remote_ssh_port = 22

    obj.remote_mysql_hostname = '127.0.0.1'
    obj.remote_mysql_dbname = 'test'
    obj.remote_mysql_username = 'root'
    obj.remote_mysql_password = 'test'
    obj.remote_mysql_dump_compressor = 'zstd'

    obj.remote_mysql_ignore_tables = [
        'table_name',
    ]

    obj.local_mysql_dbname = 'root'
    obj.local_mysql_hostname = '127.0.0.1'
    obj.local_mysql_password = 'test'

    obj.connect()

    obj.dump_remote_and_download()
    obj.unpack()
    # obj.change_row_format('dynamic')
    obj.restore_local()
    obj.clean_dump_files()


test_site()

# winsound.Beep(500, 3000)
