# 参考
#https://zenn.dev/myuki/books/02fe236c7bc377/viewer/16fd5a)
#https://cpp-learning.com/sqlalchemy-pandas/
#https://www.solima.net/python/archives/249
#https://qiita.com/tomo0/items/a762b1bc0f192a55eae8

from dotenv import load_dotenv
import os
import sys
import seaborn as sns
from datetime import datetime
from sqlalchemy.orm import sessionmaker

# ルートディレクトリ（2階層上）を読込元に追加（デバッグ用）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(ROOT)
from panda_alchemy import check_postgre_db_existence, create_postgre_db
from panda_alchemy import PandaAlchemy
# テーブル定義クラス読込
from iris_table import Iris, get_base_class


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

# データベースの有無確認し、なければ作成
if not check_postgre_db_existence(DB_NAME, USERNAME, PASSWORD, HOST, PORT):
    create_postgre_db(DB_NAME, USERNAME, PASSWORD, HOST, PORT)

# データベースに接続
with PandaAlchemy(USERNAME, PASSWORD, HOST, PORT, DB_NAME) as pdalchemy:
    # テーブルの有無確認し、あれば削除
    table_name = Iris.__tablename__
    if pdalchemy.check_table_existence(table_name):
        pdalchemy.drop_table(table_name)

    # テーブルの有無確認し、なければ作成
    if not pdalchemy.check_table_existence(table_name):
        Base = get_base_class()  # ベースクラス読込
        pdalchemy.create_table_from_declarative_base(Base)  # テーブル作成

    # データアップロード
    pdalchemy.insert_from_df(df=iris, table_name=table_name)

    # セッション開始
    Session = sessionmaker(bind=pdalchemy.engine)
    session = Session()

    # データ取得
    SPECIES = 'setosa'
    sql_statement = session.query(Iris).filter(Iris.species == SPECIES).statement
    df = pdalchemy.read_sql_query(sql_statement)
    print(df)