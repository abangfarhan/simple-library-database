from simulate_library import run_simulation
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
import numpy as np
from configparser import ConfigParser

# Steps:
# - Generate data with Faker, simulate_library, kindle books data
# - Accordingly create the necessary tables
# - Connect to PostgreSQL database
# - Remove all tables in the database (prompt first)
# - Run query from .sql file to generate tables
# - With pandas save the data to database

def get_db_url(table_name: str, config_file: str='config.ini') -> str:
    config = ConfigParser()
    config.read(config_file)
    section = 'postgresql'
    username = config.get(section, 'username')
    password = config.get(section, 'password')
    host = config.get(section, 'host')
    port = config.get(section, 'port')
    database_url = f'postgresql://{username}:{password}@{host}:{port}/{table_name}'
    return database_url

if __name__ == '__main__':
    start_time = datetime(2015, 1, 1)
    Faker.seed(42)
    fake = Faker('id_ID')

    library_names = [
        'Library A', 'Library B', 'Library C',
    ]
    df_libraries = pd.DataFrame({'library_name': library_names})
    n_libraries = len(df_libraries)

    max_n_books = 300
    df_books = pd.read_csv('kindle_data-v2-top500.csv')
    df_books = df_books.query('author.notna()')
    df_books = df_books.drop_duplicates('title')
    assert df_books is not None
    df_books = df_books.reset_index(drop=True)[['author', 'title', 'category_name']]

    n_books = min(len(df_books), max_n_books)
    df_books = df_books.head(n_books)

    np.random.seed(15)
    df_books['library_id'] = np.random.randint(n_libraries, size=n_books)

    # for purpose of analysis, we will remove some last part of the simulated data
    finished_ratio = 0.9

    n_users = 300
    num_days = 365
    min_borrow_days = 1
    max_borrow_days = 20
    min_book_qty = 3
    max_book_qty = 10
    arrival_interval = 0.5 # customer arrive every _ hours
    end_time = start_time + timedelta(days=int(num_days * finished_ratio))

    print('Running simulation')
    books, queues, loans = run_simulation(
        n_books = n_books,
        n_users = n_users,
        num_days = num_days,
        min_borrow_duration = min_borrow_days * 24,
        max_borrow_duration = max_borrow_days * 24,
        min_book_qty = min_book_qty,
        max_book_qty = max_book_qty,
        arrival_interval = arrival_interval,
        seed = 300397,
    )

    def hours_to_date(col: pd.Series) -> pd.Series:
        return col.map(lambda h: timedelta(hours=h)) + start_time

    df_queues = pd.DataFrame(queues)
    df_loans = pd.DataFrame(loans)

    df_queues['queue_start'] = hours_to_date(df_queues['queue_start'])
    df_queues['queue_end'] = hours_to_date(df_queues['queue_end'])
    df_loans['loan_start'] = hours_to_date(df_loans['loan_start'])
    df_loans['loan_end'] = hours_to_date(df_loans['loan_end'])
    df_books['total_quantity'] = [book.available_quantity for book in books]

    df_loans = df_loans[df_loans['loan_start'] <= end_time].copy()
    df_loans.loc[df_loans['loan_end'] > end_time, 'loan_end'] = None
    df_queues = df_queues[df_queues['queue_start'] <= end_time]
    df_queues.loc[df_queues['queue_end'] > end_time, 'queue_end'] = None

    user_emails = set()
    while len(user_emails) < n_users:
        user_emails.add(fake.email())

    df_users = pd.DataFrame({
        'user_name': [fake.name() for _ in range(n_users)],
        'email': list(user_emails),
    })
    assert len(df_users['email'].unique()) == n_users

    df_categories = pd.DataFrame({'category_name': df_books['category_name'].unique()})
    categories_id = {category_name: category_id for category_id,category_name in enumerate(df_categories['category_name'].values)}
    df_books['category_id'] = df_books['category_name'].map(categories_id)
    df_books = df_books.drop('category_name', axis=1)

    database_url = get_db_url('simple_library', 'config.ini')
    engine = create_engine(database_url)

    with open('./create_tables.sql', 'r') as f:
        sql_script = text(f.read())

    with engine.connect() as conn:
        print('Creating table schemas')
        conn.execute(sql_script)
        conn.commit()

    print('Saving tables to database')
    # format: (df_variable, id_prefix, table_name)
    dfs = (
        (df_categories, 'category', 'categories'),
        (df_libraries, 'library', 'libraries'),
        (df_books, 'book', 'books'),
        (df_users, 'user', 'users'),
        (df_queues, 'queue', 'queues'),
        (df_loans, 'loan', 'loans'),
    )
    for df,id_prefix,table_name in dfs:
        df = df.reset_index(drop=True)
        df[f'{id_prefix}_id'] = df.index
        cols = df.columns
        # convert index columns to start with 1
        id_cols = cols[cols.str.endswith('_id')]
        for col in id_cols:
            df[col] = df[col] + 1
        df.to_sql(table_name, engine, index=False, if_exists='append')
