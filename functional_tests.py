import mysql.connector, pytest, os
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

# Поиск по началу строки
def test_1_functional(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE 'b%';")
    result_no_index = sorted(cursor.fetchall())

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE 'b%';")
    result_with_index = sorted(cursor.fetchall())

    print("Результаты без индекса:", [row[0] for row in result_no_index])
    print("Результаты с индексом:", [row[0] for row in result_with_index])

    assert result_no_index == result_with_index

    cursor.close()


# Поиск по наличию в строке 
def test_2_functional(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%ap%';")
    result_no_index = sorted(cursor.fetchall())

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%ap%';")
    result_with_index = sorted(cursor.fetchall())

    print("Результаты без индекса:", [row[0] for row in result_no_index])
    print("Результаты с индексом:", [row[0] for row in result_with_index])

    assert result_no_index == result_with_index

    cursor.close()

# Поиск по концу строки
def test_3_functional(db_connection):
    cursor = db_connection.cursor()
    # Запрос без индекса на столбце
    cursor.execute("DROP INDEX IF EXISTS idx_fruits ON test_table;")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%y';")
    result_no_index = sorted(cursor.fetchall())

    # Запрос с индексом
    cursor.execute("CREATE INDEX idx_fruits ON test_table(fruits);")
    db_connection.commit()

    cursor.execute("SELECT fruits FROM test_table WHERE fruits LIKE '%y';")
    result_with_index = sorted(cursor.fetchall())

    print("Результаты без индекса:", [row[0] for row in result_no_index])
    print("Результаты с индексом:", [row[0] for row in result_with_index])

    assert result_no_index == result_with_index

    cursor.close()




