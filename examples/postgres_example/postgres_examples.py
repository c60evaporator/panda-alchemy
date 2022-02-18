from re import S
from dotenv import load_dotenv
import os
import sys
from datetime import datetime
import seaborn as sns
from sqlalchemy import text, select, table, literal_column, column, bindparam

# ルートディレクトリ（2階層上）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(ROOT)
from panda_alchemy import check_postgre_db_existence, create_postgre_db
from panda_alchemy import PandaAlchemy


load_dotenv()
username = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('HOST_IP')
PORT = 5432
DB_NAME = 'testdb'
TABLE_NAME = 'iris'

# アップロード用のデータ
IRIS_COLS = {
    'sepal_length':'Float',
    'sepal_width':'Float',
    'petal_length':'Float',
    'petal_width':'Float',
    'species':'String',
    'dt':'DateTime'
}
iris = sns.load_dataset('iris')
iris['dt'] = datetime.now()
print(iris)

# データベースの有無確認してなければ作成
if not check_postgre_db_existence(DB_NAME, username, password, host, PORT):
    create_postgre_db(DB_NAME, username, password, host, PORT)

# データベースに接続
pdalchemy = PandaAlchemy(username, password, host, PORT, DB_NAME)

# テーブルの有無確認してあれば削除
if pdalchemy.check_table_existence(TABLE_NAME):
    pdalchemy.drop_table(TABLE_NAME)

# テーブルの有無確認してなければ作成
if not pdalchemy.check_table_existence(TABLE_NAME):
    pdalchemy.create_table_from_dtype_dict(TABLE_NAME, dtype_dict=IRIS_COLS)

# データアップロード
pdalchemy.insert_from_df(iris, TABLE_NAME, dtype_dict=IRIS_COLS)

# データ取得(生SQL文)
SPECIES = 'setosa'
sql = f'SELECT * FROM {TABLE_NAME} WHERE species=:species'
sql = text(sql).bindparams(species=SPECIES) # プレースホルダを渡す
df = pdalchemy.read_sql_query(sql, dtype_dict=IRIS_COLS)
print(f'------Result of "{str(sql)}"------')
print(df)

posts = table('posts', column('subject'), column('user_id'))
# データ取得(SQLAlchemy構文 https://www.m3tech.blog/entry/sqlalchemy-tutorial)
sql = (
  select(text('*'))
  .select_from(table(TABLE_NAME))
  .where(text('species') == bindparam('species'))  # bindparamでプレースホルダ指定
)
df = pdalchemy.read_sql_query(sql, dtype_dict=IRIS_COLS, params={'species': SPECIES})  # プレースホルダ変数は`params`引数で指定
print(f'------Result of "{str(sql)}"------')
print(df)