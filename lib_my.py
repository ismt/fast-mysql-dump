import lzma
import os
import re
import subprocess
import time
import uuid
from itertools import repeat

import MySQLdb
import paramiko

import platform


class ConsolePrint:

    def __init__(self):
        self.last_message_time = time.time()

    def print(self, message: str):
        current_time = time.time()

        print(f'{round(current_time - self.last_message_time, 1)}  {message}')

        self.last_message_time = current_time


class CopyMysqlDbRemoteToLocal:
    def __init__(self):
        self.remote_ssh_hostname = ''
        self.remote_ssh_username = ''
        self.remote_ssh_password = ''
        self.remote_ssh_port = 22

        self.remote_mysql_dbname = ''
        self.remote_mysql_hostname = '127.0.0.1'
        self.remote_mysql_username = ''
        self.remote_mysql_password = ''
        self.remote_mysql_port = ''
        self.remote_mysql_dump_path = None
        self.remote_mysql_dump_path_local = None
        self.remote_mysql_dump_path_local_uncompressed = f'tmp/dump.sql'

        self._remote_mysql_dump_compressor = None

        self.remote_mysql_dump_compressor = 'zstandard'

        self.remote_mysql_ignore_tables = list()

        self.local_mysql_dbname = ''
        self.local_mysql_hostname = '127.0.0.1'
        self.local_mysql_username = 'root'
        self.local_mysql_password = 'test'
        self.local_mysql_port = 3306

        os.makedirs('tmp', exist_ok=True)

        self.console = ConsolePrint()

        self.ssh_server = None
        self.sftp = None
        self.local_db = None
        self.local_db_cursor = None

        if platform.system() == 'Linux':
            self.mysql = r'mysql'
            self.lz4 = 'lz4'
            self.zstd = 'zstd'

        else:
            self.mysql = r'C:\Program Files\MariaDB 10.1\bin\mysql.exe'
            self.lz4 = r'.\lz4\lz4'
            self.zstd = r'.\zstd\zstd'

    def connect(self):

        self.ssh_server = paramiko.SSHClient()
        self.ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.ssh_server.connect(
            hostname=self.remote_ssh_hostname,
            username=self.remote_ssh_username,
            password=self.remote_ssh_password,
            port=self.remote_ssh_port,
            compress=True,
            allow_agent=True
        )

        self.sftp = self.ssh_server.open_sftp()

        self.local_db = MySQLdb.connect(
            host=self.local_mysql_hostname,
            port=self.local_mysql_port,
            db=self.local_mysql_dbname,
            user=self.local_mysql_username,
            passwd=self.local_mysql_password,
            charset="utf8mb4",
            connect_timeout=30,
            autocommit=True
        )

        self.local_db_cursor = self.local_db.cursor(MySQLdb.cursors.DictCursor)

    @property
    def remote_mysql_dump_compressor(self):
        return self._remote_mysql_dump_compressor

    @remote_mysql_dump_compressor.setter
    def remote_mysql_dump_compressor(self, value):

        compressors = ['lz4', 'zstandard', 'xz']

        assert value in compressors, f'Нет такого компрессора, Выберите из {compressors}'

        self._remote_mysql_dump_compressor = value
        self.remote_mysql_dump_path_local = f'tmp/dump.sql.{value}'
        self.remote_mysql_dump_path = f'/tmp/8aeac716-3960-421f-9672-ee00a95f7594'

    def dump_remote_and_download(self):

        self.console.print('Начинаем дамп')

        if os.path.isfile(self.remote_mysql_dump_path_local):
            os.remove(self.remote_mysql_dump_path_local)

        if os.path.isfile(self.remote_mysql_dump_path_local_uncompressed):
            os.remove(self.remote_mysql_dump_path_local_uncompressed)

        try:
            stat = self.sftp.stat(self.remote_mysql_dump_path)

            self.console.print('Уже идет процесс дампа или он был некорректно завершен, сбрасываем')

            self.ssh_server.exec_command('killall -2 lz4')
            self.ssh_server.exec_command('killall -2 xz')
            self.ssh_server.exec_command('killall -2 pzstd')

            self.sftp.remove(self.remote_mysql_dump_path)

            time.sleep(1)

        except FileNotFoundError:
            pass

        self.console.print('Дампим базу на сервере')

        ignore_tables = ' '.join(
            f'--ignore-table={self.remote_mysql_dbname}.{item}'
            for item in self.remote_mysql_ignore_tables
        )

        if self._remote_mysql_dump_compressor == 'lz4':
            compressor = 'lz4 -1 -z'

        elif self._remote_mysql_dump_compressor == 'zstandard':
            compressor = 'pzstd -3 -c'

        elif self._remote_mysql_dump_compressor == 'xz':
            compressor = 'xz -1 -c --threads=0'

        else:
            raise ValueError('Не опознан тип сжатия')

        # time.sleep(3)

        stdin, stdout, stderr = self.ssh_server.exec_command(
            f'mysqldump '
            f'--user="{self.remote_mysql_username}" '
            f'--host="{self.remote_mysql_hostname}" '
            f'--password="{self.remote_mysql_password}" '
            f'--max_allowed_packet=1000M '
            f'--extended-insert '
            # f'--flush-logs '
            f'--lock-tables '
            f'--routines '
            f'--quick '
            # f'--no-autocommit '    
            f'{ignore_tables} '
            f'"{self.remote_mysql_dbname}" | {compressor} > {self.remote_mysql_dump_path}'
        )

        for line in stdout:
            print(line.strip('\n'))

        for line in stderr:
            print(line.strip('\n'))

        self.console.print('Качаем с сервера')

        self.sftp.get(
            self.remote_mysql_dump_path,
            self.remote_mysql_dump_path_local
        )

        self.sftp.remove(self.remote_mysql_dump_path)

        self.ssh_server.close()

        self.console.print('Ok')

    def restore_local(self):

        self.drop_local_tables()

        self.console.print('Восстанавливаем')

        subprocess.call(
            f'"{self.mysql}" '
            f'--host={self.local_mysql_hostname} '
            f'--port={self.local_mysql_port} '
            f'--user={self.local_mysql_username} '
            f'--password={self.local_mysql_password} '
            f'--database={self.local_mysql_dbname} '
            f'', stdin=open(self.remote_mysql_dump_path_local_uncompressed),
            shell=True
        )

        self.console.print('Ok')

    def unpack(self):
        self.console.print(f'Распаковываем {self.remote_mysql_dump_path_local}')

        if self._remote_mysql_dump_compressor == 'lz4':
            subprocess.call(
                f'{self.lz4} -d -c "{self.remote_mysql_dump_path_local}" ',
                stdout=open(self.remote_mysql_dump_path_local_uncompressed, 'w'),
                shell=True
            )

        elif self._remote_mysql_dump_compressor == 'zstandard':
            subprocess.call(
                f'{self.zstd} -d -c "{self.remote_mysql_dump_path_local}" ',
                stdout=open(self.remote_mysql_dump_path_local_uncompressed, 'w'),
                shell=True
            )

        elif self._remote_mysql_dump_compressor == 'xz':
            with lzma.LZMAFile(self.remote_mysql_dump_path_local) as fxz:
                with open(file=self.remote_mysql_dump_path_local_uncompressed, mode='wb') as fout:
                    while True:
                        data = fxz.read(10_000_000)

                        if data:
                            fout.write(data)

                        else:
                            break

        else:
            raise ValueError('Не опознан тип сжатия')

        self.console.print('Ok')

    def drop_local_tables(self):
        self.console.print('Удаляем таблицы в локальной базе')

        self.local_db_cursor.execute(
            'show table status  where  Name not in %(table_names)s',
            dict(
                table_names=self.remote_mysql_ignore_tables + ['']
            ))

        res = self.local_db_cursor.fetchall()

        self.local_db_cursor.execute(f'''SET foreign_key_checks = 0''')

        for item in res:
            self.local_db_cursor.execute(f'''drop table `{item['Name']}`''')

        self.local_db_cursor.execute(f'''SET foreign_key_checks = 1''')

        self.console.print('Ok')

    def change_row_format(self, row_format):

        if isinstance(row_format, str):
            self.console.print(f'Смена row_format={row_format}')

            remote_mysql_dump_path_local_uncompressed_tmp = f'{self.remote_mysql_dump_path_local_uncompressed}_tmp'

            with open(
                    file=self.remote_mysql_dump_path_local_uncompressed,
                    mode='r',
                    encoding='utf8'
            ) as fin:
                with open(
                        file=remote_mysql_dump_path_local_uncompressed_tmp,
                        mode='w',
                        encoding='utf8'
                ) as fout:
                    for item in fin:
                        item2 = re.sub(r' ROW_FORMAT=\w+', f' ROW_FORMAT={row_format}', item)

                        fout.write(item2)

            os.remove(self.remote_mysql_dump_path_local_uncompressed)

            os.rename(remote_mysql_dump_path_local_uncompressed_tmp, self.remote_mysql_dump_path_local_uncompressed)

            self.console.print('Ok')


def insert_bath(
        row_list,
        table_name,
        cursor,
        server_type='mysql',
        insert_mode: str = 'insert'
):
    if not row_list:
        return False

    assert server_type in ('mysql', 'sphinx'), 'Не опознан тип сервара базы для вставки'

    assert insert_mode in ('insert', 'replace', 'insert_ignore'), 'Не опознан режим вставки'

    sql_columns = tuple(row_list[0].keys())

    insert_values_sql_part = []
    insert_values = []

    for item in row_list:

        insert_value = list()

        for key, value in item.items():

            if value is None and server_type == 'sphinx':
                value = ''

            if isinstance(value, uuid.UUID):
                value = value.hex

            insert_value.append(value)

        insert_values_sql_part.append('(' + ','.join(repeat('%s', len(insert_value))) + ')')
        insert_values += insert_value

    if insert_mode == 'insert':
        sql_start = 'INSERT'

    elif insert_mode == 'replace':
        sql_start = 'REPLACE'

    elif insert_mode == 'insert_ignore':
        sql_start = 'INSERT IGNORE'

    else:
        raise ValueError('Не  найден  режим  вставки')

    sql = f'''
            {sql_start}  INTO  {table_name}
            ({','.join(sql_columns)})
            VALUES {','.join(insert_values_sql_part)}
    '''

    cursor.execute(sql, insert_values)

    return True


def split_list_to_chunks(l, n):
    # Разбивает лист на серии по несколько элементов
    for i in range(0, len(l), n):
        yield l[i:i + n]
