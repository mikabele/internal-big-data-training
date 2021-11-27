import sys
import argparse
import hashlib
from typing import Dict

import mysql.connector
import os
import datetime


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data_dir", type=str, required=True)
    parser.add_argument("-t", "--task", type=str, required=True)
    parser.add_argument("-m", "--market", type=str, required=True)
    load_group = parser.add_mutually_exclusive_group(required=True)
    load_group.add_argument("-f", "--full", action="store_true", default=False)
    load_group.add_argument("-i", "--incremental", action="store_true", default=False)
    return parser


def get_config_variables() -> Dict[str, str]:
    config_dict = {}
    with open("../Configs/configs.ini", "r") as config_file:
        for line in config_file.readlines():
            if line.startswith("#"):
                continue
            if line.endswith("\n"):
                line = line[:-1]
            key, value = line.split("=")
            if key in config_dict.keys():
                raise Exception("Duplicate keys in config file")
            config_dict[key] = value
    return config_dict


def get_connection() -> mysql.connector.connection:
    config_dict = get_config_variables()
    return mysql.connector.connect(host=config_dict["HOST"],
                                   user=config_dict["USER"],
                                   password=config_dict["USER_PASSWORD"],
                                   database=config_dict["DATABASE"],
                                   port=int(config_dict["PORT"]),
                                   allow_local_infile=True)


def insert_data(conn: mysql.connector.connection, cur, path_to_file: str,
                check_sum: str, market: str,
                task: str):
    start_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("SELECT COUNT(*) FROM landing_fund_db.landing_fund_table;")
    result = cur.fetchall()
    start_count = 0
    if result:
        start_count = result[0][0]
    cur.execute(
        f"INSERT INTO landing_fund_db.audit_table(filename,`checksum`,start_load_date,market,task,end_load_date,count_of_inserted_rows,is_stored)"
        f"VALUES ('{path_to_file}', '{check_sum}', '{start_timestamp}', '{market}', '{task}',NULL,NULL,FALSE)")
    conn.commit()
    cur.execute(f"LOAD DATA LOCAL INFILE '{path_to_file}' "
                "REPLACE INTO TABLE landing_fund_db.landing_fund_table "
                "FIELDS TERMINATED BY ';'  "
                "ENCLOSED BY '\"'   "
                "IGNORE 1 ROWS "
                "(`time`,`open`,`high`,`low`,`close`,volume)"
                f"SET load_date='{start_timestamp}',"
                f"     market='{market}';")
    conn.commit()
    end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM landing_fund_db.landing_fund_table;")
    result = cur.fetchall()
    end_count = 0
    if result:
        end_count = result[0][0]
    cur.execute(f"UPDATE landing_fund_db.audit_table "
                f"SET end_load_date='{end_timestamp}',"
                f"    count_of_inserted_rows={end_count - start_count}, "
                f"    is_stored=TRUE "
                f"WHERE start_load_date='{start_timestamp}'")
    conn.commit()


def full_load(data_dir: str, task: str, market: str):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE landing_fund_db.landing_fund_table;")
        conn.commit()
        cur.execute("UPDATE landing_fund_db.audit_table "
                    "SET is_stored=FALSE "
                    "WHERE is_stored=TRUE;")
        conn.commit()
        files = os.listdir(data_dir)
        for datafile in files:
            if not os.path.isfile(os.path.join(data_dir, datafile)):
                continue
            with open(data_dir + "/" + datafile, "rb") as opened_file:
                check_sum = hashlib.sha256(opened_file.read()).hexdigest()
            insert_data(conn, cur, data_dir + '/' + datafile, check_sum, market, task)
    except Exception as e:
        print(e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def incremental_load(data_dir: str, task: str, market: str):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        files = os.listdir(data_dir)
        for datafile in files:
            if not os.path.isfile(os.path.join(data_dir, datafile)):
                continue
            with open(data_dir + "/" + datafile, "rb") as opened_file:
                check_sum = hashlib.sha256(opened_file.read()).hexdigest()
            cur.execute(f"SELECT 1 "
                        f"FROM landing_fund_db.audit_table "
                        f"WHERE `checksum`='{check_sum}' AND is_stored=TRUE "
                        f"LIMIT 1;")
            check_log = cur.fetchall()
            if not check_log:
                insert_data(conn, cur, data_dir + '/' + datafile, check_sum, market, task)
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()
        if cur:
            cur.close()


def main():
    parser = get_parser()
    namespace = parser.parse_args(sys.argv[1:])
    data_dir = namespace.data_dir
    task = namespace.task
    market = namespace.market
    full = namespace.full
    incremental = namespace.incremental

    if full:
        full_load(data_dir, task, market)
        return
    if incremental:
        incremental_load(data_dir, task, market)
        return


if __name__ == "__main__":
    main()
