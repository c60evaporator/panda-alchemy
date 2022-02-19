from dotenv import load_dotenv
import os
import sys
from datetime import datetime
import seaborn as sns
from sqlalchemy import text, select, table, bindparam
import sqlalchemy

# ルートディレクトリ（2階層上）を読込元に追加（デバッグ用）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(ROOT)
from panda_alchemy import check_postgres_db_existence, create_postgres_db
from panda_alchemy import PandaAlchemy


###### 各種設定値の読込 ######
load_dotenv()
USERNAME = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('HOST_IP')
PORT = 5432
DB_NAME = 'testdb'

###### アップロード用のデータ読込 ######
iris = sns.load_dataset('iris')  # irisデータセットを使用
iris['dt'] = datetime.now()
TABLE_NAME = 'iris'  # DBでのテーブル名
# 列ごとに型指定(型を厳密に指定したいときに使用)
IRIS_COLS = {
    'sepal_length':'Float',
    'sepal_width':'Float',
    'petal_length':'Float',
    'petal_width':'Float',
    'species':'String',
    #'dt':'DateTime'
    #'dt': sqlalchemy.types.DateTime()
    'dt': sqlalchemy.types.Date()
}

###### アップロード実行と各種動作確認 ######
# データベースの有無確認し、なければ作成
if not check_postgres_db_existence(DB_NAME, USERNAME, PASSWORD, HOST, PORT):
    create_postgres_db(DB_NAME, USERNAME, PASSWORD, HOST, PORT)

# データベースに接続
with PandaAlchemy(USERNAME, PASSWORD, HOST, PORT, DB_NAME) as pdalchemy:
    # テーブルの有無確認し、あれば削除
    if pdalchemy.check_table_existence(TABLE_NAME):
        pdalchemy.drop_table(TABLE_NAME)

    # テーブルの有無確認し、なければ作成
    if not pdalchemy.check_table_existence(TABLE_NAME):
        pdalchemy.create_table_from_dtype_dict(TABLE_NAME, dtype_dict=IRIS_COLS)
        #pdalchemy.create_table_from_df(iris, TABLE_NAME, dtype_dict=IRIS_COLS)

    # データアップロード
    pdalchemy.insert_from_df(iris, TABLE_NAME, dtype_dict=IRIS_COLS)

    # データ取得(生SQL文)
    SPECIES = 'setosa'
    sql = text(f'SELECT * FROM {TABLE_NAME} WHERE species=:species')  # :でプレースホルダ指定
    df = pdalchemy.read_sql_query(sql, dtype_dict=IRIS_COLS, params={'species': SPECIES})  # プレースホルダ変数は`params`引数で指定
    print(f'------Result of "{str(sql)}"------')
    print(df)

    # データ取得(SQL Expression Language構文 https://www.m3tech.blog/entry/sqlalchemy-tutorial)
    sql = (
    select(text('*'))
    .select_from(table(TABLE_NAME))
    .where(text('species') == bindparam('species'))  # bindparamでプレースホルダ指定
    )
    df = pdalchemy.read_sql_query(sql, dtype_dict=IRIS_COLS, params={'species': SPECIES})  # プレースホルダ変数は`params`引数で指定
    print(f'------Result of "{str(sql)}"------')
    print(df)