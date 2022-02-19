import psycopg2
import re

def create_postgres_db(db_name, username, password, host, port):
    """Postgresデータベース作成"""
    conn = psycopg2.connect(f'postgresql://{username}:{password}@{host}:{port}/postgres')
    conn.autocommit = True
    sql = f'CREATE DATABASE {db_name}'
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.close()

def check_postgres_db_existence(db_name, username, password, host, port):
    """データベースの存在有無を確認"""
    # まずはデータベースに接続してみる
    try:
        conn = psycopg2.connect(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
    # エラー発生時
    except Exception as e:
        error_str = str(e)  # エラーメッセージ
        error_match = re.search(r'database "\w+" does not exist', error_str)
        # エラーメッセージが`database "\w+" does not exist`ならFalseを返す
        if error_match is not None:
            print(f'There is no database "{db_name}". Create the database before connecting')
            return False
        # エラーメッセージがそれ以外なら例外を返す
        else:
            raise Exception(error_str)
    # エラー発生しなかったらTrueを返す
    else:
        print(f'There is database "{db_name}"')
        conn.close()
        return True