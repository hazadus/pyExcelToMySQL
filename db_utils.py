import db_setup
from mysql.connector import connect, Error


def db_query(query: str) -> list:
    """Выполняет любой запрос к БД, вернет полученное из неё значение"""

    try:
        with connect(
                host=db_setup.DB_HOST,
                user=db_setup.DB_USER,
                password=db_setup.DB_PWD,
                database=db_setup.DB_NAME,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                connection.commit()
                return result
    except Error as e:
        print(e)
        return []
