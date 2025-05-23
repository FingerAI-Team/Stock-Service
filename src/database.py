from abc import abstractmethod
import psycopg2 

class DB():
    def __init__(self, config):
        self.config = config 

    @abstractmethod
    def connect():
        pass 

class DBConnection(DB):
    def __init__(self, config):
        super().__init__(config)
    
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config['host'],
            dbname=self.config['db_name'],
            user=self.config['user_id'],
            password=self.config['user_pw'],
            port=self.config['port']
        )
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()
    
class PostgresDB:
    '''
    데이터베이스 CRUD (Create, Read, Update, Delete) 작업에 사용되는 클래스
    '''
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_total_data(self, table_name):
        '''
        테이블에서 전체 데이터를 가져옵니다. 
        args:
        table_name (str)

        returns:
        list[str]  - 자료형 확인 필요: 테이블 전체 데이터 
        '''
        query = f"SELECT * FROM {table_name};"
        self.db_connection.cur.execute(query)
        return self.db_connection.cur.fetchall()

    def get_day_data(self, table_name, date):
        '''
        테이블에서 전일 데이터를 가져옵니다.
        args:
        date (str): 20240130 형식
        '''
        query = f"SELECT * FROM {table_name} WHERE SPLIT_PART(conv_id, '_', 1) = '{date}' ;"
        self.db_connection.cur.execute(query)
        return self.db_connection.cur.fetchall()

    def check_pk(self, table_name, pk_value):
        '''
        테이블에 Primary Key(PK)가 존재하는지 확인합니다. 이미 존재하는 PK인 경우, True를 반환합니다. 
        args:
        data (list): 테이블 행 데이터   ex) [conv_id, date, qa, content, user_id]
        '''
        self.db_connection.conn.commit()
        self.db_connection.cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE conv_id = %s)", (pk_value,))
        result = self.db_connection.cur.fetchone()
        return result[0] if result else False
       

class TableEditor:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def edit_conv_table(self, task, table_name, data_type=None, data=None, col=None, val=None):
        '''
        insert, delete, update
        data_type = raw or table
        '''
        if task == 'insert':
            if data_type == 'table':
                for idx in range(len(data)):
                    self.db_connection.cur.execute(
                        f"INSERT INTO {table_name} (conv_id, date, qa, content, user_id) VALUES (%s, %s, %s, %s, %s)",
                        (data['conv_id'][idx], data['date'][idx], data['q/a'][idx], data['content'][idx], data['user_id'][idx])
                    )
                    self.db_connection.conn.commit()
            elif data_type == 'raw':
                self.db_connection.cur.execute(
                    f"INSERT INTO {table_name} (conv_id, date, qa, content, user_id) VALUES (%s, %s, %s, %s, %s)",
                    tuple(data)
                )
                self.db_connection.conn.commit()
        elif task == 'delete':
            pass 
        elif task == 'update':
            pass

    def edit_cls_table(self, task, table_name, data_type=None, data=None, col=None, val=None):
        if task == 'insert':
            if data_type=='table':
                for idx in range(len(data)):
                    self.db_connection.cur.execute(
                    f"INSERT INTO {table_name} (conv_id, ensemble) VALUES (%s, %s)",
                    (data['conv_id'][idx], data['ensemble'][idx])
                    )
                self.db_connection.conn.commit()
            elif data_type=='raw':
                self.db_connection.cur.execute(
                    f"INSERT INTO {table_name} (conv_id, ensemble) VALUES (%s, %s)",
                    tuple(data)
                )
                self.db_connection.conn.commit()
        elif task == 'delete':
            pass 
        elif task == 'update':
            pass
    
    def edit_clicked_table(self, task, table_name, data_type=None, data=None, col=None, val=None):
        if task == 'insert':
            if data_type=='table':
                for idx in range(len(data)):
                    self.db_connection.cur.execute(
                        f"INSERT INTO {table_name} (conv_id, clicked, user_id) VALUES (%s, %s, %s)",
                        (data['conv_id'][idx], data['clicked'][idx], data['user_id'][idx])
                    )
                self.db_connection.conn.commit()
            elif data_type=='raw':
                self.db_connection.cur.execute(
                    f"INSERT INTO {table_name} (conv_id, clicked, user_id) VALUES (%s, %s, %s)",
                    tuple(data)
                )
                self.db_connection.conn.commit()
        elif task == 'delete':
            pass 
        elif task == 'update':
            pass
