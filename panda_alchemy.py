import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Float, Integer, BigInteger, Boolean, String, DateTime

class PandaAlchemy():
    TYPE_PANDAS = {
        'Float': 'float64',
        'Integer': 'int32',
        'BigInteger': 'int64',
        'Boolean': 'bool',
        'String': 'object',
        'DateTime': 'datetime64[ns]'
    }
    # 初期�?
    def __init__(self, username, password, hostname, port, database):
        """
        PandasとPostgreSQLのデータ入出力用クラス

        Parameters
        ----------
        username : str
            PostgreSQLのユーザ名

        password : str
            PostgreSQLのパスワード

        hostname : str
            PostgreSQLのホスト名 (PostgreSQLが動作しているサーバのIPアドレス)
            
        port : int
            PostgreSQLのポート番号 (通常は5432)

        database : str
            PostgreSQL DB名
        """
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.database = database
        # SQLAlchemyのengine作成
        self.engine = self._get_engine(username, password, hostname, port, database)

    def __enter__(self):
        return self

    def __exit__(self):
        """ withブロックから抜けたら時の処理 """
        #self.connection.close()

    def _get_connection(self, username, password, hostname, port, database):
        """
        psycopg2でPostgreSQLサーバサーバに接続
        """
        dsn = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        return psycopg2.connect(dsn)

    def _get_engine(self, username, password, hostname, port, database):
        """
        SQLAlcyemyのengineを取得
        """
        engine_txt = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        return create_engine(engine_txt)

    def _convert_dataframe_dtype(self, df_src, dtype_dict):
        """
        DataFrameの型をdtype_dictに合わせて変換
        """
        # 変換前の型表示
        print('------ Pandas data types before conversion------')
        for i, c in zip(df_src.dtypes.index, df_src.dtypes):
            print(f'{i}  {c}')
        # 日時型以外を変換
        dtype_except_dt = {k: self.TYPE_PANDAS[v] for k, v in dtype_dict.items() if v != 'DateTime'}
        df_dst = df_src.astype(dtype_except_dt)
        # 日時型を変換
        for k, v in dtype_dict.items():
            if v == 'DateTime':
                df_dst[k] = pd.to_datetime(df_dst[k])
        # 変換後の型表示
        print('------ Pandas data types after conversion------')
        for i, c in zip(df_dst.dtypes.index, df_dst.dtypes):
            print(f'{i}  {c}')
        return df_dst

    def _make_sqlalchemy_dtype(self, dtype_dict):
        """
        SQLAlcemy形式の列の型を指定(https://stackoverflow.com/questions/62938757/how-to-force-sqalchemy-float-type-to-real-in-postgres)

        出力されたdictを`pd.to_sql()`の`dtype`引数に渡す
        """
        sqlalchemy_dtype = {}
        for k, v in dtype_dict.items():
            if v == 'Float':
                sqlalchemy_dtype[k] = sqlalchemy.types.Float()
            elif v == 'Integer':
                sqlalchemy_dtype[k] = sqlalchemy.types.Integer()
            elif v == 'BigInteger':
                sqlalchemy_dtype[k] = sqlalchemy.types.BigInteger()
            elif v == 'Boolean':
                sqlalchemy_dtype[k] = sqlalchemy.types.Boolean()
            elif v == 'String':
                sqlalchemy_dtype[k] = sqlalchemy.types.String()
            elif v == 'DateTime':
                sqlalchemy_dtype[k] = sqlalchemy.types.DateTime()
        return sqlalchemy_dtype

    def _convert_types(self, df_src, dtype_dict):
        """
        型指定あるとき、DataFrameを変換 & SQLAlchemyの型形式を作成
        """
        if dtype_dict is not None:
            df_convert = self._convert_dataframe_dtype(df_src, dtype_dict)  # PandasのDataFrameを変換
            sqlalchemy_dtype = self._make_sqlalchemy_dtype(dtype_dict)  # SQLAlchemyの型形式(`to_sql`メソッドのdtype引数に指定)
        else:
            df_convert = df_src  # 型指定ないとき、そのままシャローコピー
            sqlalchemy_dtype = None
        return df_convert, sqlalchemy_dtype

    def create_table_from_dtype_dict(self, table_name, dtype_dict, autoincrement=True):
        """
        型定義dictからテーブル作成

        SQLAlcemyのMetaData形式のテーブル定義を使用(https://laplace-daemon.com/basic-use-of-sqlalchemy/#toc_id_5_1)

        Parameters
        ----------
        table_name : str
            作成したいテーブル名

        dtype_dict : dict[str, str]
            列名と型の組み合わせを指定するdict

            Key: 作成したいフィールド名

            Value: "Float", "Integer", "BigInteger", "Boolean", "String", "DateTime"から選択

        autoincrement : bool, Default=True
            Trueなら、連番のキー列を自動作成(参考:https://qiita.com/EasyCording/items/9eda4064412aa7f73567)
        """
        
        # MetaDataをインスタンス化
        metadata = MetaData()
        # 列一覧を作成
        column_list = []
        # 連番の主キー`id`を追加
        if autoincrement:
            column_list.append(Column('id', Integer, primary_key=True, autoincrement=True))
        # dtype_dictで指定した列を追加
        for k, v in dtype_dict.items():
            if v == 'Float':
                column_list.append(Column(k, Float))
            elif v == 'Integer':
                column_list.append(Column(k, Integer))
            elif v == 'BigInteger':
                column_list.append(Column(k, BigInteger))
            elif v == 'Boolean':
                column_list.append(Column(k, Boolean))
            elif v == 'String':
                column_list.append(Column(k, String))
            elif v == 'DateTime':
                column_list.append(Column(k, DateTime))
        column_list = tuple(column_list)
        # テーブル作成
        table = Table(table_name, metadata, *column_list)
        metadata.create_all(self.engine)
        
        print(f'Table `{table_name}` is made')

    def create_table_from_df(self, df, table_name, dtype_dict=None):
        """
        PandasのDataFrameからテーブルを作成
        """
        # 型指定あるとき、DataFrameを変換 & SQLAlchemyの型形式を作成
        df_convert, sqlalchemy_dtype = self._convert_types(df, dtype_dict)
        # `pandas.DataFrame.to_sql`でPostgresにテーブル作成
        df_convert.to_sql(table_name, self.engine, if_exists='fail', index=False,
                          dtype=sqlalchemy_dtype)
        # データを削除(型指定した空のテーブルのみが残る)
        self.truncate_table(table_name)

        print(f'Table `{table_name}` is made')

    def insert_from_df(self, df, table_name, dtype_dict=None):
        """
        DataFrameからデータ追加
        """
        # 型指定あるとき、DataFrameを変換 & SQLAlchemyの型形式を作成
        df_convert, sqlalchemy_dtype = self._convert_types(df, dtype_dict)
        # `pandas.DataFrame.to_sql`でPostgresテーブルにデータ追加
        df_convert.to_sql(table_name, self.engine, if_exists='append', index=False,
                          dtype=sqlalchemy_dtype)
        
        print(f'Add {len(df_convert)} records to table `{table_name}`')

    def truncate_table(self, table_name):
        """
        テーブルを空にする
        """
        sql = sqlalchemy.text(f"TRUNCATE TABLE {table_name}")
        self.engine.execute(sql)
        print(f'Table `{table_name}` is truncated')

    def drop_table(self, table_name):
        """
        テーブルを空にする
        """
        sql = sqlalchemy.text(f"DROP TABLE {table_name}")
        self.engine.execute(sql)
        print(f'Table `{table_name}` is dropped')