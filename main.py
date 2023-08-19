import os
import re
import shutil
import numpy as np
import pandas as pd
import aspose.words as aw
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from bs4 import BeautifulSoup
from requests import get
from pprint import pprint


class HTMLParser:

    def __init__(self, src, dest):
        doc = aw.Document(src)

        doc.save(dest, aw.SaveFormat.HTML)

        with open(dest, encoding='utf-8') as f:
            self.__content = f.read()

        os.remove(dest)

    def prettify(self):
        # To make file pretty because I had a solid line of code.
        self.__content = BeautifulSoup(self.__content, 'html.parser').prettify()

    def clean_content(self):
        # Removing ads and empty nodes.

        patterns = [
            '<p.*>\s*<span.*>\s*Evaluation Only. Created with Aspose.Words. Copyright 2003-202. Aspose Pty Ltd.\s*</span>\s*</p>\s*',
            '<p.*>\s*<span.*>\s*Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/\s*</span>\s*</p>\s*',
            '<p.*>\s*<span.*>\s*<img.*>\s*</span>\s*<span.*>\s*</span>\s*</p>\s*',
            '<span.*>\s*</span>\s*',
            '<p.*>\s*</p>\s*',
            '<div.*>\s*</div>\s*'
        ]

        for pattern in patterns:
            self.__content = re.sub(pattern, '', self.__content)

    @property
    def content(self):
        return self.__content


class RTFParser:

    def __get_rtf_filename(self, url):
        # Url is a string that consists of strings separated by "/"
        # so I split it into strings and get last item(name of the file that was in the url).

        return url.split('/')[-1]

    def get_html_filename(self, filename):
        return filename.split('.')[0] + '.html'

    def __init__(self, df):
        self.__urls = df.doc_url
        self.__filename_content = {}

    def extract_content(self, buff, timeout=5, show_progress=False, show_errors=False):
        path = Path(buff)

        for i, url in enumerate(self.__urls):
            # In case timeout is too long.
            try:
                data = get(url, timeout=timeout)
            except:
                if show_errors:
                    print(f'RTFParser Error: {i}') # Just to see indices with problem urls.
            else:
                # If time of getting file wasn't too long
                # I get name of the file that was in the url and save it.

                rtf_filename = self.__get_rtf_filename(url)
                html_filename = self.get_html_filename(rtf_filename)
                dest_rtf = str(path / rtf_filename)
                dest_html = str(path / html_filename)

                path.mkdir(parents=True, exist_ok=True)

                with open(dest_rtf, 'wb') as f:
                    f.write(data.content)

                parser = HTMLParser(dest_rtf, dest_html)

                parser.prettify()
                parser.clean_content()

                self.__filename_content[html_filename] = parser.content

            if show_progress and i % 100 == 0:
                print(f'RTFParser: {i}')

        shutil.rmtree(path, ignore_errors=True)

    @property
    def filename_content(self):
        return self.__filename_content


class Cleaner:

    def __init__(self, df):
        self.__df = df

    def drop_empty_entries(self):
        self.__df = self.__df[self.__df.doc_url.notna() & self.__df.cause_num.notna()]

    def drop_invalid_causes(self):
        filter_causes = lambda cause_num: bool(re.search('\d+', cause_num)) and len(cause_num) > 3
        self.__df = self.__df[self.__df.cause_num.map(filter_causes)]

    def drop_invalid_urls(self, timeout=5, show_progress=False, show_errors=False):
        indices = []                       # List for indices of valid urls.
        urls = self.__df['doc_url'].values # Getting urls.

        for i, url in enumerate(urls):
            try:
                if get(url, timeout=timeout).status_code == 200: # If I got answer I save index.
                    indices.append(i)
            except:
                if show_errors:
                    print(f'Cleaner Error: {i}')                         # Just to know how many there were errors.

            if show_progress and i % 100 == 0:                   # Just to see the progress.
                print(f'Cleaner: {i}')

        self.__df = self.__df.iloc[indices]

    @property
    def dataframe(self):
        return self.__df


class CausesDB:

    def __make_df(self, path):
        with open(path, encoding='utf-8') as f:
            columns = f.readline().split()
            dtypes = [int, int, int, int, int, str, pd.Timestamp, pd.Timestamp, str, str, int, pd.Timestamp]
            data = []

            for line in f:
                values = line.replace('"', '').strip().split('\t')
                converted_line = []

                for value, dtype in zip(values, dtypes):
                    try:
                        if type(value) == str and not value.strip():
                            converted_line.append(np.nan)
                        else:
                            converted_line.append(dtype(value))
                    except:
                        converted_line.append(np.nan)

                data.append(converted_line)

            return pd.DataFrame(data, columns=columns)

    def __init__(self, path, config, timeout=5, show_progress=False, show_errors=False):
        self.__df = self.__make_df(path)
        self.__df = self.__df.iloc[:100]

        cleaner = Cleaner(self.__df)

        cleaner.drop_empty_entries()
        cleaner.drop_invalid_causes()
        cleaner.drop_invalid_urls(timeout, show_progress, show_errors)

        self.__df = cleaner.dataframe

        self.__conn = mysql.connector.connect(**config)
        self.__cur = self.__conn.cursor()
        self.__config = config

    def __del__(self):
        self.__cur.close()
        self.__conn.close()

    def get_html_content(self, buff, **config):
        self.__parser = RTFParser(self.__df)

        self.__parser.extract_content(buff, **config)

        self.__filename_content = self.__parser.filename_content

    def make_db(self, db_name):
        try:
            self.__cur.execute(f"CREATE DATABASE IF NOT EXIST {db_name} DEFAULT CHARACTER SET 'utf8'")
        except mysql.connector.Error as err:
            print(f'Failed creating database: {err}')
            exit(1)
        else:
            print(f'Database {db_name} created successfully.')
            self.__cur.execute(f'USE {db_name}')

            self.__config['database'] = db_name

    def make_tables(self):
        TABLES = {}

        TABLES['cause'] = '''
            CREATE TABLE cause (
                cause_id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
                cause_num VARCHAR(100) NOT NULL
            );
        '''

        TABLES['cause_document'] = '''
            CREATE TABLE cause_document (
                document_id INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,
                cause_id INTEGER NOT NULL,
                court_code INTEGER,
                judgment_code INTEGER,
                justice_kind DOUBLE,
                category_code DOUBLE NULL,
                status INTEGER,
                doc_url VARCHAR(150) NOT NULL,
                content LONGTEXT NOT NULL,
                FOREIGN KEY (cause_id) REFERENCES cause(cause_id)
            );
        '''

        for table_name in TABLES:
            table_description = TABLES[table_name]

            try:
                print(f'Creating table {table_name}: ', end='')
                self.__cur.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print('already exists.')
                else:
                    print(err.msg)
            else:
                print('OK')

    def fill_tables(self, show_progress=False):
        add_cause = 'INSERT INTO cause (cause_num) VALUES (%s)'
        add_document = '''
                    INSERT INTO cause_document (
                        cause_id,
                        court_code,
                        judgment_code,
                        justice_kind,
                        category_code,
                        status,
                        doc_url,
                        content
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''

        data = self.__df.values.T
        i = 0

        for court_code, judgment_code, justice_kind, category_code, cause_num, doc_url, status in zip(*data):
            try:
                data_cause = (cause_num,)

                self.__cur.execute(add_cause, data_cause)

                cause_id = self.__cur.lastrowid
                filename = self.__parser.get_html_filename(doc_url)
                content = self.__filename_content[filename]
                data_document = (cause_id, court_code, judgment_code,
                                 justice_kind, category_code, status, doc_url, content)

                self.__cur.execute(add_document, data_document)
            except FileNotFoundError:
                print(doc_url)
            except Exception as e:
                print(e)
                print(cause_num)
                break

            i += 1

            if show_progress and i % 100 == 0:
                print(i)

        self.__conn.commit()


        query = '''
    SELECT court_code, judgment_code, justice_kind, category_code, cause_num, doc_url, status, content
        FROM cause
        JOIN cause_document USING(cause_id)
'''
        data = []
        html_content = []

        self.__cur.execute(query)

        for i, row in enumerate(self.__cur):
            data.append(list(row[:-1]))
            html_content.append(row[-1])

            if i % 100 == 0:
                print(i)

        df2 = pd.DataFrame(data, columns=self.__df.columns)
        df2['content'] = html_content


config = {'user': '...', 'password': '...'}
db = CausesDB('documents.csv', config, show_errors=True, show_progress=True)

db.get_html_content('buff', show_errors=True, show_progress=True)
db.make_db('Causes')
db.make_tables()
db.fill_tables(show_progress=True)


# df = pd.read_csv('df2.csv')
#
# parser = RTFParser(df)
# config = {'show_progress': True, 'show_errors': True}
#
# parser.extract_content('buff')
#
# d = parser.filename_content
#
# for filename in list(d.keys())[:5]:
#     pprint(d[filename])
#     print('\n' * 4)