import mysql.connector, pytest, os, time
from dotenv import load_dotenv

# Подключаем файл .env
load_dotenv()

# Создание фикстуры для подключения к БД
@pytest.fixture(scope='module')
def db_connection():
    connection = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        database = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD')
    )
    yield connection
    connection.close

# Проверка ускорения производительности индекса
def test_1_perfomance(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE 'A%';")
    time_no_index = time.time() - start_time
    cursor.fetchall()

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE 'A%';")
    time_with_index = time.time() - start_time
    cursor.fetchall()

    assert time_with_index < time_no_index

    cursor.close()

#Поиск по индексу для фильтра LIKE не работает
def test_2_perfomance(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%Ap%';")
    time_no_index = time.time() - start_time
    cursor.fetchall()

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%Ap%';")
    time_with_index = time.time() - start_time
    cursor.fetchall()

    assert time_with_index < time_no_index

    cursor.close()


#Поиск по индексу для фильтра LIKE не работает, id не является константой
def test_3_perfomance(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE id;")
    time_no_index = time.time() - start_time
    cursor.fetchall()

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    start_time = time.time()
    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE id;")
    time_with_index = time.time() - start_time
    cursor.fetchall()

    assert time_with_index < time_no_index

    cursor.close()

