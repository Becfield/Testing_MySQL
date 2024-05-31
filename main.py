import mysql.connector, os
from dotenv import load_dotenv

# Подключаем файл .env
load_dotenv()

#Подключение к БД
def get_db_connection():
    connection = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        database = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD')
    )
    return connection

# Функция для заполнения данных
def db_setup(connection):
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS test_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        fruits VARCHAR(30)
        );
    """)


    data = [('apple',), ('banana',), ('orange',), ('grape',), ('pineapple',), ('melon',), ('kiwi',), ('berry',),
        ('cherry',), ('mango',)]

    cursor.executemany("INSERT INTO test_table (fruits) VALUES (%s)", data)

    connection.commit()
    cursor.close()

if __name__ == '__main__':
    i = 0
    while i < 100:
        connection = get_db_connection()
        try:
            db_setup(connection)
            print('База данных заполнена')
            i += 1
        except mysql.connector.Error as err:
            print(f"Ошибка {err}")
        finally:
            connection.close()