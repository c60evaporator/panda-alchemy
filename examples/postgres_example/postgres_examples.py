#from panda_alchemy import PandaAlchemy
from dotenv import load_dotenv
import os
import sys


# ルートディレクトリ（2階層上）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(ROOT)
from panda_alchemy import postgres_utils

load_dotenv()
username = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('HOST_IP')
PORT = 5432
DB_NAME = 'testdb'

# データベース作成
#postgres_utils.create_database(DB_NAME, username, password, host, PORT)

# データベースの有無確認
#postgres_utils.check_database_existence(DB_NAME, username, password, host, PORT)