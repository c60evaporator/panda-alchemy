# 概要
PandasのDataFrameと各種データベース(SQLAlchemy対応RDBMS)とのデータやり取りを効率化するライブラリ

# 必要要件
- Python >=3.6
- Pandas >=1.2.4
- SQLAlchemy >=1.4.26
- Psycopg2 >=2.9.3
- python-dotenv >=0.19.2 (examplesコードのみ)
- PyYAML >=6.0 (examplesコードのみ)
- seaborn >=0.11.2 (examplesコードのみ)


# 事前準備: データベースの立ち上げ
事前にデータベースサーバの立ち上げが必要となります。

Dockerを使用すると簡単なので、この方法を紹介します。
本ライブラリのexamplesフォルダ内に、各DBごとにデータベースサーバを立ち上げるためのdocker-compose.yamlを準備しています

## PostgreSQLの場合
PostgresSQLを使用する場合、以下の手順で立ち上げます。

### DBサーバの起動 (最初の1回のみ)
本ライブラリのルートフォルダから以下のコマンドでコンテナ起動

```
cd examples/postgres_example
docker-compose up
```
上記コンテナ起動で、自動でPostgreSQLサーバのDBが立ち上がる

### DBの作成 (最初の1回のみ)
以下のコードを実行し、DBを作成

```python
from panda_alchemy import create_postgres_db

DB_NAME='作成したいDB名'
USERNAME='PostgreSQLサーバのユーザ名'
PASSWORD='PostgreSQLサーバのパスワード'
HOST='PostgreSQLサーバのホスト(IPアドレス)'
PORT='PostgreSQLサーバのポート番号'

create_postgres_db(DB_NAME, USERNAME, PASSWORD, HOST, PORT)
```

#### 参考: pgadminによるDB作成
上のPythonコードの代わりに、以下手順でpgadminでDB手動作成しても良い

・Chrome等のブラウザに、`http://PostgresSQLサーバのIPアドレス:81`と打つ

・PGADMINのユーザ名とパスワードが求められるので、docker-composeに記載した内容(PostgresSQLサーバのアカウントとは異なるので注意)を入れてログイン

・立ち上がった画面左側の「Servers」を右クリック → Create → Serger

・立ち上がったウィンドウに以下を記載し、PostgresSQLサーバに接続
- Generalタブの「Name」に任意のサーバ名を入力
- Connectionタブの「Host name」にPostgresのDockerコンテナ名、「Port」にポート番号5432、「Username」と「Password」にdocker-composeに記載したPostgresSQLサーバのアカウント内容を入力

・左側のタブに作成されたサーバーを右クリックし、 Create → Database

・立ち上がったウィンドウのGeneralタブの「Database」に作成したいデータベース名を入力

## 