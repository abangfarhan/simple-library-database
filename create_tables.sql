drop table if exists libraries cascade;
drop table if exists categories cascade;
drop table if exists books cascade;
drop table if exists users cascade;
drop table if exists queues cascade;
drop table if exists loans cascade;

create table libraries (
    library_id serial primary key,
    library_name varchar(256) not null unique
);

create table categories (
    category_id serial primary key,
    category_name varchar(256) not null unique
);

create table books (
    book_id serial primary key,
    title varchar(256) not null,
    author varchar(256) not null,
    category_id integer not null references categories,
    library_id integer not null references libraries,
    total_quantity integer not null,
    constraint total_quantity_nonnegative check (total_quantity >= 0)
);

create table users (
    user_id serial primary key,
    email varchar(256) not null unique,
    user_name varchar(256) not null
);

create table queues (
    queue_id serial primary key,
    queue_start timestamp not null default now(),
    queue_end timestamp null,
    user_id integer not null references users,
    book_id integer not null references books
);

create table loans (
    loan_id serial primary key,
    book_id integer not null references books,
    user_id integer not null references users,
    queue_id integer null references queues,
    loan_start timestamp not null default now(),
    loan_due timestamp not null generated always as (loan_start + interval '14 days') stored,
    loan_end timestamp null,
    constraint due_date_after_loan_date check (loan_due > loan_start),
    constraint return_date_between_two_dates check (loan_end >= loan_start and loan_end <= loan_due)
);

