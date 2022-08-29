import pandas as pd
import db_utils
import logging
import sys

# TODO: попробовать версию с SQLite3
# TODO: add decorator - execution time measurement

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("log.txt"),
        logging.StreamHandler(sys.stdout)
    ],
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%d/%m/%Y %H:%M:%S',
)


def excel_file_to_mysql(source_file_name: str, table_name: str) -> None:
    """Из исходного файла в формате XLSX создать таблицу в БД MySQL и заполнить её данными.
    Существующая таблица будет удалена."""

    # 1. Читаем файл Excel
    logging.info("Loading Excel file...")
    source_df = pd.read_excel(source_file_name)

    logging.info(f"Excel file loaded. Columns: ")

    # 2. Получаем список заголовков столбцов
    for column in source_df.columns:
        column = column.replace('\n', ' ')  # В названиях колонок были замечены символы перевода строки, убрать их
        logging.info(f"- {column}")

    # 3. Проверяем наличие таблицы в БД - если есть, удаляем её
    query = f"DROP TABLE IF EXISTS {table_name}"
    result = db_utils.db_query(query)

    # 4. Генерим SQL-код, который создает таблицу
    query_create_table_head = f"""CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT,
    """

    db_column_list = []  # Заголовки таблицы БД (с убранными спец символами)
    df_column_list = []  # Заголовки DataFrame (как есть в Excel)
    # Определяем типы для колонок таблицы в БД, для отсутсвующих здесь будет TINYTEXT:
    db_data_types = {  # TODO: добавить больше типов (FLOAT?)
        "Примечание_диспетчера": "TEXT",
        "Оставшийся_километраж": "INT"
    }

    for column in source_df.columns:
        comment = column
        column = column.replace('\n', '')  # В названиях колонок были замечены символы перевода строки, убрать их
        column = column.replace(' ', '_')  # Пробелы меняем на подчеркивания
        column = column.replace('/', '')
        column = column.replace('.', '')
        column = column.replace(',', '')
        column = column.replace('№', 'Номер')
        db_column_list.append(column)
        df_column_list.append(comment)

        if column in db_data_types:  # Если для колонки определен тип данных вручную, берём его
            column_type = db_data_types[column]
            query_create_table_head += f"\n    {column} {column_type} COMMENT '{comment}',"
        else:
            query_create_table_head += f"\n    {column} TINYTEXT COMMENT '{comment}',"

    query_create_table_tail = f"""
        PRIMARY KEY(id)
    );
    """

    query_create_table = query_create_table_head + query_create_table_tail

    logging.info(query_create_table)

    # 5. Создаем таблицу в БД
    db_utils.db_query(query_create_table)

    # 6. Построчно добавляем все строки в таблицу
    #
    query_insert_row_head = f"""INSERT INTO {table_name} (
    """

    for column in db_column_list:
        query_insert_row_head = f"{query_insert_row_head}\n    {column},"

    query_insert_row_head = query_insert_row_head[:-1] + f"""  # [:-1] чтобы убрать последнюю запятую
    )
    VALUES (
    """

    row = 1
    # https://www.geeksforgeeks.org/different-ways-to-iterate-over-rows-in-pandas-dataframe/
    for ind in source_df.index:
        logging.info(f"Insert row {row} to DB...")
        head = query_insert_row_head
        for column in df_column_list:
            head += f"\n    '{source_df[column][ind]}',"

        query_insert_row_tail = ");"

        query_insert_row = head[:-1] + query_insert_row_tail  # [:-1] чтобы убрать последнюю запятую
        db_utils.db_query(query_insert_row)
        # TODO: проверить результат выполнения запроса на наличие ошибок
        row += 1

    logging.info("Done.")


excel_file_to_mysql("Дислокация вагонов по сети.xlsx", "istk_dislocation")
