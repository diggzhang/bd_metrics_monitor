import os
import csv
import sys
import json
import urllib3  # urllib3 (1.22)
import requests
import psycopg2
import subprocess
from config import config

urllib3.disable_warnings()
BOT_URL = config(section='dingding')['bot']
REPO_PATH = os.path.dirname(os.path.abspath(__file__)) + config(section='path')['repopath']
HOME_DIR = config(section='path')['home']
MONGO_ALL_FIELDS = config(section='path')['csv']


def ls(path):
    print('cd %s' % path)
    cmd = 'ls ' + path
    subprocess.call(cmd, shell=True)


def home_dir(home_path):
    cmd = 'cd ' + home_path
    subprocess.call(cmd, shell=True)


def pull_repo(path):
    pull_repo_cmd = 'cd ' + path + ' && git pull'
    res = subprocess.call(pull_repo_cmd, shell=True)
    return res


def load_csv(csv_dst):
    csv_file = open(csv_dst, "r")
    reader = csv.reader(csv_file)
    fields_list = []
    for item in reader:
        fields_list.append(*item)
    print("[*] Loading {%d} items." % len(fields_list))
    return fields_list


def pg_create_mongo_fields_table():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        DROP TABLE IF EXISTS mongo_hive_fields;
        """,
        """
        CREATE TABLE mongo_hive_fields (
            mongo_fields_name VARCHAR(255) NOT NULL
        );
        """,
    )

    conn = None

    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def pg_insert_row_into_table(dist):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO mongo_hive_fields(mongo_fields_name)
             VALUES(%s);"""

    conn = None

    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (dist,))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def difference_list():
    """ query difference from the parts table """
    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("""
    select event_track.ek_name, event_track.ek_type
from (
  (
    select
      distinct RIGHT(LEFT(CAST((json_array_elements(rawjsonschema) :: JSON -> 'propName') AS TEXT),-1),-1) as ek_name,
              RIGHT(LEFT(CAST((json_array_elements(rawjsonschema) :: JSON -> 'propType') AS TEXT),-1),-1) AS ek_type
    from pointpool
  ) event_track
  left join
  (
    select mongo_fields_name as ek_name
    from mongo_hive_fields
  ) mongo_fields
  on event_track.ek_name = mongo_fields.ek_name
)
where mongo_fields.ek_name is null
    """)
        rows = cur.fetchall()
        difference_list_is = []
        for row in rows:
            if row[0] != '':
                difference_list_is.append(list(row))
        cur.close()
        print("[*] Difference {%s} items" % len(difference_list_is))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        return difference_list_is


def dingding_bot(bot_url, data):
    headers = {'Content-Type': 'application/json'}
    post_data = {
        "msgtype": "markdown",
        "markdown": data
    }
    r = requests.post(bot_url, headers=headers, data=json.dumps(post_data))
    print(r)


if __name__ == '__main__':
    home_dir(HOME_DIR)
    is_pull_success = pull_repo(REPO_PATH)
    if is_pull_success != 0:
        print("[*] Git pull failed. ")
        sys.exit(1)

    home_dir(HOME_DIR)
    mongo_fields_list = load_csv(MONGO_ALL_FIELDS)
    if mongo_fields_list is [] or len(mongo_fields_list) == 0:
        print("[*] Csv load failed. ")
        sys.exit(1)

    pg_create_mongo_fields_table()

    for item in mongo_fields_list:
        pg_insert_row_into_table(item)

    diff_list = difference_list()

    item_list = []
    for item in diff_list:
        row = "- {} {}\n".format(item[0], item[1])
        item_list.append(row)
    txt = ''.join(item_list)

    warning_text = {
        "title": "fields",
        "text": "### 缺失字段列表 共计 " + str(len(item_list)) + " 个字段 \n " + txt
    }
    dingding_bot(BOT_URL, warning_text)
