# Simple library database system design

## Background

The objective of this project is to design a database system for an e-library
application. The application has several libraries, and each library has a
diverse collection of books. In this application, users can borrow or place
hold on books (if the book is currently not available for borrowing).

PostgreSQL is used to create the database, while Python is used to
fill in the database with dummy data (to make sure that the database design is
going to work well).

## Building the Database

- Make sure PostgreSQL and Python are installed (I only tested on Python 3.10)
- Run `pip install -r requirements.txt` in this directory
- On pgAdmin, create a new empty database named `simple_library`
- Edit the file config.ini and fill in the correct username, password, etc. for
  PostgreSQL
- Run `python generate_data.py` in this directory. Now the database should be
  populated with dummy data.

## Dummy Data Generation

There are three kinds of data to be generated for this database:

1. Book details data
2. User details data
3. Lending/queueing activities data

For book details, I used the [Amazon Kindle Books Dataset
2023](https://www.kaggle.com/datasets/asaniczka/amazon-kindle-books-dataset-2023-130k-books)
(I only used 300 most-reviewed books from the dataset). Meanwhile, for user
details I used the [Faker python library](https://faker.readthedocs.io/) to
generate dummy data.

Generating the lending activities data is not such a straightforward task,
since there are restrictions like "number of borrowed books cannot exceed the
book's total quantity at any given time," or "users cannot borrow more than two
books at the same time," and so on. If the data are simply generated randomly,
then those restrictions would certainly be violated. Therefore, I found the
best method to generate the lending activities data is to simulate the library
application.

The simulation is done via a Python script, which will simulate the flow of
events that will happen if the application were to actually be implemented. For
example, in case of a user requesting to borrow a book, the following events
will be executed:

![]("./images/Pasted image 20231229203401.png")

Meanwhile, the following events will be executed in case of user returning the
borrowed book:

![]("./images/Pasted image 20231229203425.png")

The simulation script can be found on `simulate_library.py`.
