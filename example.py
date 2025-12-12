import lib_my


# import winsound


def test_site():
    obj = lib_my.CopyMysqlDbRemoteToLocal(
        dump_name='zen_api_stag',

        remote_ssh_hostname='192.168.2.1',
        remote_ssh_username='dev_stem',
        remote_ssh_password='',
        remote_ssh_port=53125,

        remote_ssh_key_filename=r'C:\Users\T\.ssh\testttt',

        remote_mysql_hostname='ttt',

        remote_mysql_dbname='stag_api',
        remote_mysql_username='admin',
        remote_mysql_password='',

        remote_mysql_dump_compressor='zstd',
        remote_mysql_ignore_tables=[],
        # local_mysql_dbname = 'zen__stag_api_test_logistic5',
        local_mysql_dbname = 'zen__stag_api',
        local_mysql_hostname = '192.168.2.87'
    )

    obj.connect()

    obj.dump_remote_and_download()
    obj.unpack()
    # obj.change_row_format('dynamic')
    # obj.remove_definer_from_file()
    obj.restore_local()
    obj.clean_dump_files()


test_site()

# winsound.Beep(500, 3000)
