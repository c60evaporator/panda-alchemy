import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column
from sqlalchemy import Float, Integer, BigInteger, Boolean, String, DateTime, Date

class PandaAlchemy():
    # 初期化
    def __init__(self, username, password, host, port, database):
        """
        PandasとPostgreSQLのデータ入出力用クラス

        セッションが残らないようwith構文での使用を推奨

        Parameters
        ----------
        username : str
            PostgreSQLのユーザ名

        password : str
            PostgreSQLのパスワード

        host : str
            PostgreSQLのホスト名 (PostgreSQLが動作しているサーバのIPアドレス)
            
        port : int
            PostgreSQLのポート番号 (通常は5432)

        database : str
            PostgreSQL DB名
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        # SQLAlchemyのengine作成
        self.engine = self._get_engine(username, password, host, port, database)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """ withブロックから抜けたら時の処理 """
        # エンジン破棄
        self.engine.dispose()

    def _get_engine(self, username, password, host, port, database):
        """
        SQLAlcyemyのengineを取得
        """
        engine_txt = f'postgresql://{username}:{password}@{host}:{port}/{database}'
        return create_engine(engine_txt)    

    def _make_sqlalchemy_dtype(self, dtype_dict):
        """
        dtype_dictからSQLAlcemy形式の列の型を指定(https://stackoverflow.com/questions/62938757/how-to-force-sqalchemy-float-type-to-real-in-postgres)

        出力されたdictを`pd.to_sql()`の`dtype`引数に渡す
        """
        sqlalchemy_dtype = {}
        for k, v in dtype_dict.items():
            # valueが文字列の時
            if v == 'Float':
                sqlalchemy_dtype[k] = sqlalchemy.types.Float()
            elif v == 'BigInteger':
                sqlalchemy_dtype[k] = sqlalchemy.types.BigInteger()
            elif v == 'Integer':
                sqlalchemy_dtype[k] = sqlalchemy.types.Integer()
            elif v == 'Boolean':
                sqlalchemy_dtype[k] = sqlalchemy.types.Boolean()
            elif v == 'String':
                sqlalchemy_dtype[k] = sqlalchemy.types.String()
            elif v == 'DateTime':
                sqlalchemy_dtype[k] = sqlalchemy.types.DateTime()
            # valueが`sqlalchemy.types`の型メンバのとき
            elif isinstance(v, sqlalchemy.types.TypeEngine):
                sqlalchemy_dtype[k] = v
            # 型が上記以外のとき、エラーを返す
            else:
                raise Exception(f'Values of `dtype_dict` should be strings or members of `sqlalchemy.types`. A type of {k} is {v}, so it is not available.')
        return sqlalchemy_dtype

    def _make_pandas_dtype(self, dtype_dict):
        """
        dtype_dictからpandas.DataFrame形式の列の型を指定

        出力されたdictを`self._convert_dataframe_dtype`および`pd.read_sql_query`の`dtype`引数に渡す
        """
        dtype_except_dt = {}
        parse_dates = []
        for k, v in dtype_dict.items():
            # valueが文字列、または対応する`sqlalchemy.types`メンバの時
            if v == 'Float' or isinstance(v, Float):
                dtype_except_dt[k] = 'float64'
            elif v == 'BigInteger' or isinstance(v, BigInteger):
                dtype_except_dt[k] = 'int64'
            elif v == 'Integer' or isinstance(v, Integer):
                dtype_except_dt[k] = 'int32'
            elif v == 'Boolean' or isinstance(v, Boolean):
                dtype_except_dt[k] = 'bool'
            elif v == 'String' or isinstance(v, String):
                dtype_except_dt[k] = 'object'
            elif v == 'DateTime' or isinstance(v, DateTime):
                parse_dates.append(k)
            # valueが上記以外の`sqlalchemy.types`メンバのとき、変換を実施しない
            elif isinstance(v, sqlalchemy.types.TypeEngine):
                print(f'Class {type(v)} is not compatible with pandas dtypes, so the dtype of column `{k}` is not converted')
            # valueが上記以外のとき、エラーを返す
            else:
                raise Exception(f'Values of `dtype_dict` should be strings or members of `sqlalchemy.types`. A type of {k} is {v}, so it is not available.')
        return dtype_except_dt, parse_dates

    def _convert_dataframe_dtype(self, df_src, dtype_dict):
        """
        DataFrameの型をdtype_dictに合わせて変換
        """
        # 変換前の型表示
        print('------ Pandas data types before conversion------')
        for i, c in zip(df_src.dtypes.index, df_src.dtypes):
            print(f'{i}  {c}')
        # 変換後の型を取得
        dtype_except_dt, parse_dates = self._make_pandas_dtype(dtype_dict)
        # 日時型以外を変換
        df_dst = df_src.astype(dtype_except_dt)
        # 日時型を変換
        for k in parse_dates:
            df_dst[k] = pd.to_datetime(df_dst[k])
        # 変換後の型表示
        print('------ Pandas data types after conversion------')
        for i, c in zip(df_dst.dtypes.index, df_dst.dtypes):
            print(f'{i}  {c}')
        return df_dst

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

    def create_table_from_declarative_base(self, base_class):
        """
        SQLAlcemyの`declarative_base()`で生成したメタクラスからテーブル作成(https://laplace-daemon.com/basic-use-of-sqlalchemy/#toc_id_5_1)

        複数のテーブルを一括作成可能

        Parameters
        ----------
        base_class : sqlalchemy.ext.declarative.api.DeclarativeMeta
            `declarative_base()`で生成したメタクラス（継承クラスでテーブル定義を記載）

            メタクラスの作成法は下記参照()

        .. code-block:: python

            from sqlalchemy import Column, Float, Integer, String
            from sqlalchemy.ext.declarative import declarative_base

            Base = declarative_base()

            class TableClass(Base):
                __tablename__ = 'table'
                column1 = Column(Integer, primary_key=True, autoincrement=True)
                column2 = Column(Float)
                column3 = Column(String)
            
            pdalchemy = PandaAlchemy(USERNAME, PASSWORD, HOST, PORT, DB_NAME)
            pdalchemy.create_table_from_declarative_base(Base)
        """
        # 既存テーブル一覧取得
        current_tables = self.get_table_dict()

        # テーブル作成(メタクラスの定義を反映)
        base_class.metadata.create_all(self.engine)
        
        # 作成結果を表示
        for table_name, table_data in base_class.metadata.tables.items():
            # 既存テーブルに作成したいテーブルが存在する場合、その旨を表示
            if table_name in current_tables.keys():
                print(f'Table `{table_name}` already exists')
            # 既存テーブルに存在しないテーブルは、新たに作成した旨を表示
            else:
                print(f'Table `{table_name}` is made')

    def create_table_from_dtype_dict(self, table_name, dtype_dict, autoincrement=True, autoincrement_name='id'):
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

            Value: "Float", "Integer", "BigInteger", "Boolean", "String", "DateTime", または`sqlalchemy.types`のメンバから選択

            記載例: 

            >>> dtype_dict={"column1": "Float", "column2":"String", "column3": sqlalchemy.types.Date()} 

        autoincrement : bool, default=True
            Trueなら、連番のキー列を自動作成(参考:https://qiita.com/EasyCording/items/9eda4064412aa7f73567)

        autoincrement_name : str, default="id"
            autoincrementで作成された連番キー例の名称(autoincrement=Trueの時のみ有効)
        """
        
        # MetaDataをインスタンス化
        metadata = MetaData()
        # 列一覧を作成
        column_list = []
        # 連番の主キー`id`を追加
        if autoincrement:
            column_list.append(Column(autoincrement_name, Integer, primary_key=True, autoincrement=True))
        # dtype_dictで指定した列を追加
        for k, v in dtype_dict.items():
            # valueが文字列、または対応する`sqlalchemy.types`メンバの時
            if v == 'Float' or isinstance(v, Float):
                column_list.append(Column(k, Float))
            elif v == 'BigInteger' or isinstance(v, BigInteger):
                column_list.append(Column(k, BigInteger))
            elif v == 'Integer' or isinstance(v, Integer):
                column_list.append(Column(k, Integer))
            elif v == 'Boolean' or isinstance(v, Boolean):
                column_list.append(Column(k, Boolean))
            elif v == 'String' or isinstance(v, String):
                column_list.append(Column(k, String))
            elif v == 'DateTime' or isinstance(v, DateTime):
                column_list.append(Column(k, DateTime))
            # valueが上記以外の`sqlalchemy.types`メンバのとき、`type(v)`を列の型クラスとして使用
            elif isinstance(v, sqlalchemy.types.TypeEngine):
                column_list.append(Column(k, type(v)))
            # valueが上記以外のとき、エラーを返す
            else:
                raise Exception(f'Values of `dtype_dict` should be strings or members of `sqlalchemy.types`. A type of {k} is {v}, so it is not available.')
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
        pandas.DataFrameからDBのテーブルにデータ追加

        pandas.DataFrame.to_sqlを使用(https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)

        Parameters
        ----------
        df : pandas.DataFrame
            追加対象のDataFrame

        table_name : str
            データを追加したいテーブル名

        dtype_dict : dict[str, str], default=None
            列名と型の組み合わせを指定するdict。型を厳密に指定したい場合に使用

            Key: フィールド名

            Value: "Float", "Integer", "BigInteger", "Boolean", "String", "DateTime", または`sqlalchemy.types`のメンバから選択

            記載例: 

            >>> dtype_dict={"column1": "Float", "column2":"String", "column3": sqlalchemy.types.Date()} 
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
        テーブルを削除する
        """
        sql = sqlalchemy.text(f"DROP TABLE {table_name}")
        self.engine.execute(sql)
        print(f'Table `{table_name}` is dropped')

    def get_table_dict(self):
        """
        テーブル一覧をdict形式で取得
        """
        metadata = MetaData()
        metadata.reflect(self.engine)
        return metadata.tables

    def check_table_existence(self, table_name):
        """
        テーブルの存在有無を確認
        """
        table_dict = self.get_table_dict()
        if table_name in table_dict.keys():
            return True
        else:
            return False

    def read_sql_query(self, sql, index_col=None, params=None,
                       parse_dates=None, chunksize=None, dtype_dict=None):
        """
        SQLクエリで取得した内容をpandas.DataFrameに出力

        pandas.read_sql_queryを使用(https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html)

        Parameters
        ----------
        sql : str
            適用するSQL文

        index_col : str or list[str], default=None
            インデックスとして適用するフィールド名(リスト指定した場合MultiIndexとなる)

        params : list, tuple or dict, default=None
            日時型として読み込みたいフィールド名のリスト(`dtype_dict`が指定されていない時のみ有効)

        parse_dates : list[str], default=None
            日時型として読み込みたいフィールド名のリスト(`dtype_dict`が指定されていない時のみ有効)

        chunksize : int, default=None
            列数がchunksizeを上回った時、複数のデータフレームに分けて返す

        dtype_dict : dict[str, str]
            列名と型の組み合わせを指定するdict

            Key: フィールド名

            Value: "Float", "Integer", "BigInteger", "Boolean", "String", "DateTime", または`sqlalchemy.types`のメンバから選択

            記載例: 

            >>> dtype_dict={"column1": "Float", "column2":"String", "column3": sqlalchemy.types.Date()} 
        """
        # dtype_dictが指定されているとき、日時型とそれ以外に分ける
        if dtype_dict is not None:
            dtype_except_dt, parse_dates = self._make_pandas_dtype(dtype_dict)
        else:
            dtype_except_dt = None
        df = pd.read_sql_query(sql=sql, con=self.engine, index_col=index_col, params=params, 
                               parse_dates=parse_dates, chunksize=chunksize, dtype=dtype_except_dt)
        return df