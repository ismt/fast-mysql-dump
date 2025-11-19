import lzma
import os
import pathlib
import re
import subprocess
import time
import uuid
from itertools import repeat, takewhile
from pathlib import Path

import MySQLdb
import paramiko

import platform

import math

from typing import NamedTuple, Union

import shutil

import zstandard

import getpass


class BColors(NamedTuple):
    # HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class ConsolePrint:

    def __init__(self):
        self.last_message_time = time.time()

    def print(self, message, time_precision: int = 2):
        current_time = time.time()

        seconds = current_time - self.last_message_time

        sec_portion, sec = math.modf(seconds)
        sec_portion = round(sec_portion, time_precision)
        sec_portion = str(sec_portion)[2:].ljust(time_precision, '0')

        print(f'''{time.strftime('%H:%M:%S', time.gmtime(seconds))}.{sec_portion} {message}''')

        self.last_message_time = current_time


class CopyMysqlDbRemoteToLocal:
    def __init__(self, ):
        self.script_path = Path(__file__).parent

        self.remote_ssh_hostname = ''
        self.remote_ssh_username = ''
        self.remote_ssh_password = ''
        self.remote_ssh_port = 22
        self.remote_ssh_key_filename = None

        self.remote_mysql_dbname = ''
        self.remote_mysql_hostname = '127.0.0.1'
        self.remote_mysql_username = ''
        self.remote_mysql_password = ''
        self.remote_mysql_port = ''
        self.remote_mysql_dump_path = None
        self.remote_mysql_dump_path_local = None

        self.dump_name = only_letters_digits_hypen('dump')

        self.remote_mysql_dump_path_local_uncompressed = None

        self.remote_mysql_dump_compressor = 'zstd'

        self.remote_mysql_ignore_tables = list()

        self.local_mysql_dbname = ''
        self.local_mysql_hostname = '127.0.0.1'
        self.local_mysql_username = 'root'
        self.local_mysql_password = 'test'
        self.local_mysql_port = 3306

        self.tmp_dir = Path('./tmp')

        print(f'Дампы лежат {self.tmp_dir.resolve()}')

        self.console = ConsolePrint()

        self.ssh_server = None
        self.sftp = None
        self.local_db = None
        self.local_db_cursor = None

        self.start_console_time = time.perf_counter()

    def connect(self):

        if self.ssh_server is not None:
            return

        self.ssh_server = paramiko.SSHClient()
        self.ssh_server.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # agent = paramiko.Agent()
        # keys = agent.get_keys()

        try:
            self.connect_ssh(enable_agent=True)

        except (paramiko.PasswordRequiredException, paramiko.SSHException) as e:
            # passphrase = getpass.getpass("Введите пароль к SSH-ключу: ")
            passphrase = input("Введите пароль к SSH-ключу: ")

            self.connect_ssh(passphrase=passphrase)

        self.sftp = self.ssh_server.open_sftp()

        self.local_db = MySQLdb.connect(
            host=self.local_mysql_hostname,
            port=self.local_mysql_port,
            db='',
            user=self.local_mysql_username,
            passwd=self.local_mysql_password,
            charset="utf8mb4",
            connect_timeout=30,
            autocommit=True,

        )

        self.local_db_cursor = self.local_db.cursor(MySQLdb.cursors.DictCursor)

        self.local_db_cursor.execute(f'show databases like "{self.local_mysql_dbname}"')

        if not self.local_db_cursor.fetchall():
            self.local_db_cursor.execute(f'create database {self.local_mysql_dbname}')

        self.local_db.close()

        self.local_db = MySQLdb.connect(
            host=self.local_mysql_hostname,
            port=self.local_mysql_port,
            db=self.local_mysql_dbname,
            user=self.local_mysql_username,
            passwd=self.local_mysql_password,
            charset="utf8mb4",
            connect_timeout=30,
            autocommit=True,
            init_command='SET session TRANSACTION ISOLATION LEVEL READ COMMITTED;'

        )

        self.local_db_cursor = self.local_db.cursor(MySQLdb.cursors.DictCursor)

        try:
            self.remote_mysql_dump_compressor_set(self.remote_mysql_dump_compressor)

        except ValueError as e:

            try:
                self.console.print(BColors.RED)

                self.console.print(f'{e}')
                self.console.print(f'Пробуем lz4')

                self.console.print(BColors.ENDC)

                self.remote_mysql_dump_compressor = 'lz4'

                self.remote_mysql_dump_compressor_set(self.remote_mysql_dump_compressor)

            except ValueError as e:

                self.console.print(BColors.RED)

                self.console.print(f'{e}')
                self.console.print(f'Пробуем xz, будет медленней')

                self.console.print(BColors.ENDC)

                self.remote_mysql_dump_compressor = 'xz'

                self.remote_mysql_dump_compressor_set(self.remote_mysql_dump_compressor)

        self.console.print(f'Заливаем локально в базу {self.local_mysql_dbname}')

    def remote_mysql_dump_compressor_set(self, value):

        if value is None:
            self.remote_mysql_dump_compressor_set('xz')

        compressors = ['lz4', 'zstd', 'xz']

        if value not in compressors:
            raise ValueError(f'Не знаю такого компрессора, выберите из {compressors}')

        if not self.remote_util_exists(value):
            raise ValueError(f'Утилиты {value} нет на ssh сервере')

        tmp_dir = self.script_path / 'tmp'

        self.remote_mysql_dump_path_local = (tmp_dir / Path(f'{self.dump_name}.sql.{value}')).as_posix()
        self.remote_mysql_dump_path = Path(f'/tmp/8aeac716-3960-421f-9672-ee00a95f7594').as_posix()

        self.remote_mysql_dump_path_local_uncompressed = tmp_dir / f'{self.dump_name}.sql'

    def dump_remote_and_download(self):

        self.console.print('Начинаем дамп')

        self.clean_dump_files()

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

        if self.remote_mysql_dump_compressor == 'lz4':
            compressor = 'lz4 -5 -z'

        elif self.remote_mysql_dump_compressor == 'zstd':
            compressor = 'pzstd -5 -c'

        elif self.remote_mysql_dump_compressor == 'xz':
            compressor = 'xz -1 -c --threads=0'

        else:
            raise ValueError('Не опознан тип сжатия')

        cmd_mysqldump = (
            f' mysqldump '
            f'--user="{self.remote_mysql_username}" '
            f'--host="{self.remote_mysql_hostname}" '
            f'''--password='{self.remote_mysql_password}' '''
            f'--max_allowed_packet=1000M '
            f'--extended-insert '
            # f'--flush-logs '
            f'--lock-tables '
            f'--routines '
            f'--quick '
            f'--compress '
            f'{ignore_tables} '
            f'"{self.remote_mysql_dbname}" | {compressor} > {self.remote_mysql_dump_path}'
        )

        stdin, stdout, stderr = self.ssh_server.exec_command(cmd_mysqldump, get_pty=True)

        for line in stdout:
            line = line.strip('\n')

            if 'Access denied for user' in line:
                raise ValueError(line)

            else:
                print(line)

        for line in stderr:
            print(line.strip('\n'))

        stat = self.sftp.stat(self.remote_mysql_dump_path)

        self.console.print(f'Качаем с сервера размер {format_int(stat.st_size)} килобайт')

        def callback(transferred, total):

            if time.perf_counter() - self.start_console_time > 3:
                print(f"Скачано {int(transferred * 100 / total)}%")

                self.start_console_time = time.perf_counter()

        self.sftp.get(
            remotepath=self.remote_mysql_dump_path,
            localpath=self.remote_mysql_dump_path_local,
            callback=callback
        )

        self.sftp.remove(self.remote_mysql_dump_path)

        self.ssh_server.close()

        self.console.print('Ok')

    def restore_local(self):

        self.drop_local_tables()

        self.console.print('Восстанавливаем')

        mysql_exe = self.get_mysql_exec()

        args = [
            f'"{mysql_exe.as_posix()}"',
            f'--host={self.local_mysql_hostname}',
            f'--port={self.local_mysql_port}',
            f'--user={self.local_mysql_username}',
            f'--password={self.local_mysql_password}',
            f'  {self.local_mysql_dbname}',
            f'--init_command="SET session TRANSACTION ISOLATION LEVEL READ COMMITTED"',
            f'--skip-ssl',
            f'--compress'
        ]

        command = ' '.join(args)

        subprocess.call(
            command, stdin=open(self.remote_mysql_dump_path_local_uncompressed),
            shell=True
        )

        self.console.print('Ok')

    def unpack(self):
        self.console.print(f'Распаковываем {self.remote_mysql_dump_path_local}')

        if self.remote_mysql_dump_compressor == 'lz4':
            subprocess.call(
                f'{self.get_lz4_exec()} -d -c "{self.remote_mysql_dump_path_local}" ',
                stdout=open(self.remote_mysql_dump_path_local_uncompressed, 'w'),
                shell=True
            )

        elif self.remote_mysql_dump_compressor == 'zstd':
            dctx = zstandard.ZstdDecompressor()

            with (
                open(self.remote_mysql_dump_path_local, 'rb') as ifh,
                open(self.remote_mysql_dump_path_local_uncompressed, 'wb') as ofh
            ):
                dctx.copy_stream(ifh, ofh)

        elif self.remote_mysql_dump_compressor == 'xz':

            subprocess.call(
                f'{self.get_xz_exec()} -d -c "{self.remote_mysql_dump_path_local}" ',
                stdout=open(self.remote_mysql_dump_path_local_uncompressed, 'w'),
                shell=True

            )

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

    def get_zstd_exec(self):

        if platform.system() in ['Linux', 'Darwin']:
            file = 'zstd'

        elif platform.system() in ['Windows']:
            file = r'.\zstd\zstd'

        else:
            raise ValueError('Не знаю такой операционной системы')

        return file

    def get_lz4_exec(self):

        if platform.system() in ['Linux', 'Darwin']:
            file = 'lz4'

        elif platform.system() in ['Windows']:
            file = r'.\lz4\lz4'

        else:
            raise ValueError('Не знаю такой операционной системы')

        return file

    def get_xz_exec(self):

        if platform.system() in ['Linux', 'Darwin']:
            file = 'xz'

        elif platform.system() in ['Windows']:
            file = r'.\xz\xz'

        else:
            raise ValueError('Не знаю такой операционной системы')

        return file

    def get_mysql_exec(self):
        file = ''

        if platform.system() in ['Linux', 'Darwin']:
            file = 'mysql'

        elif platform.system() in ['Windows']:
            files = [
                r'C:\Program Files\MariaDB 10.7\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.6\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.5\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.4\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.3\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.2\bin\mysql.exe',
                r'C:\Program Files\MariaDB 10.1\bin\mysql.exe',
                r'C:\Program Files\MariaDB 12.0\bin\mysql.exe',
                r'.\utils\mysql.exe',
            ]

            for file in files:
                file2 = Path(file)

                if file2.is_file():
                    return file2
        else:
            raise ValueError('Не знаю такой операционной системы')

        return Path(file)

    def clean_dump_files(self):

        self.console.print('Удаляем файлы дампов локально')

        os.makedirs(self.tmp_dir, exist_ok=True)

        shutil.rmtree(self.tmp_dir)

        os.makedirs(self.tmp_dir, exist_ok=True)

        self.console.print('Ok')

    def remote_util_exists(self, util_name):

        stdin, stdout, stderr = self.ssh_server.exec_command(f'whereis "{util_name}"', get_pty=True)

        for line in stdout:
            res = line.strip('\n').strip('\r')

            if res == util_name + ':':
                return False

        for line in stderr:
            print(line.strip('\n'))

        return True

    def connect_ssh(self, passphrase=None, enable_agent=False):

        params = dict(
            hostname=self.remote_ssh_hostname,
            username=self.remote_ssh_username,
            port=self.remote_ssh_port,
            compress=True,

        )

        if self.remote_ssh_password:
            params['allow_agent'] = False
            params['password'] = self.remote_ssh_password

        if enable_agent:
            params['allow_agent'] = True
            params['look_for_keys'] = False

        if self.remote_ssh_key_filename:
            params['key_filename'] = self.remote_ssh_key_filename

        if passphrase:
            params['passphrase'] = passphrase

        self.ssh_server.connect(
            **params,
        )

        t = 5


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


def format_int(value):
    return "{:,}".format(value).replace(',', "'")


def delete_line(file_path: str, line_number: int, start_from_one: bool = True):
    """
    Удаляет строку из файла по номеру, не загружая весь файл в память.

    :param file_path: путь к файлу
    :param line_number: номер строки (если start_from_one=True — 1-я строка = 1)
    :param start_from_one: нумерация строк с 1 (по умолчанию) или с 0
    """

    file_path = Path(file_path)

    tmp_path = Path(__file__).parent / 'tmp' / 'dump_tmp.sql'

    with tmp_path.open(mode='w', encoding='utf-8') as f:
        pass

    tmp_fd = tmp_path

    idx_to_remove = line_number - 1 if start_from_one else line_number

    try:
        with (
            tmp_fd.open("w", encoding="utf-8") as tmp_file,
            file_path.open("r", encoding="utf-8") as orig_file
        ):
            lines_count = count_lines(file_path=file_path)

            for i, line in enumerate(orig_file):
                if i % 1000000 == 0:
                    print(f'Удаляем строку {line_number} {calc_percent(count_all=lines_count, count=i)}')

                if i != idx_to_remove:  # записываем всё кроме удаляемой строки
                    tmp_file.write(line)

        # заменить оригинал
        os.replace(tmp_path, file_path)

    except Exception:
        os.remove(tmp_path)

        raise


def calc_percent(count: int, count_all: int):

    res = 100 * count / count_all

    res = int(res)

    return f'{res}%'


def count_lines(file_path: Union[Path, str], chunk_size: int = 1024 * 1024) -> int:
    """
    Быстрый подсчёт количества строк в файле.

    :param file_path: путь к файлу
    :param chunk_size: размер блока для чтения (по умолчанию 1 MB)
    :return: количество строк
    """

    count = 0
    with open(file_path, "rb") as f:  # бинарный режим быстрее
        while chunk := f.read(chunk_size):
            count += chunk.count(b"\n")

    return count


import re


def only_letters_digits_hypen(s: str) -> str:
    result = re.sub(r'[^\w\d]', '-', s, flags=re.UNICODE)
    result = result.replace('_', '-')
    result = re.sub(r'-+', '-', result).strip('-')

    return result
