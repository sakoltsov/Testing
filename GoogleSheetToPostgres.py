# Установка необходимых библиотек
# pip install gspread 
# pip install oauth2client
# pip install psycopg2
# pip install more-itertools
 
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from itertools import islice

# Сохранение в переменную предварительно полученный токен Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# Указываем название Google таблицы, sheet1 - первый лист
sheet = client.open("dataset").sheet1

# Указываем параметры и подключаемся к PostgreSQL 
pghost = 'localhost' 
pguser = 'etluser' 
pgpass = 'password' 
pgdatabase = 'dataset' 
postgres_conn = psycopg2.connect(dbname=pgdatabase, user=pguser, host=pghost, password=pgpass, port='5432')
cursor = postgres_conn.cursor()

# Получаем и сохраняем в переменную уже записанное в PostgreSQL таблицу google_dataset количество строк
count_sql_dataset = 'SELECT COUNT(*) FROM google_dataset'
cursor.execute(count_sql_dataset)
count_result = cursor.fetchone()
last_num_row_sql_dataset = count_result[0]

# Получаем "длинну листа" = количеству строк в Google таблице и уменьшаем количество на строку заголовка
num_rows_sheets = len(sheet.get_all_values())
num_rows_sheets = num_rows_sheets - 1

# Сохраняем данные из Google таблицы и пропускаем заголовок
iterable_results = iter(sheet.get_all_values())
next(iterable_results)

# Перебираем данные из Google таблицы и вставляем только те строки, которых ещё нет в PostgreSQL, начиная с last_num_row_sql_dataset
for i, value in islice(enumerate(iterable_results), last_num_row_sql_dataset, None):
    user_id = value[0]
    company_id = value[1]
    date = value[2]
    cost = value[3]
    revenue = value[4]
    
    # Собираем полученный результат в запрос для вставки в PostgreSQL таблицу google_dataset
    query = (
        "INSERT INTO google_dataset (user_id, company_id, date, cost, revenue)"
        "VALUES (%s, %s, %s, %s, %s)")
    cursor.execute(query, (
        user_id, company_id, date, cost, revenue))
    postgres_conn.commit()

    # Выводим на экран записанную строку
    print value
