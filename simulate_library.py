from typing import Optional, Callable
from dataclasses import dataclass
from enum import Enum
import numpy as np
from bisect import insort

MAX_BORROW_BOOK = 2
MAX_QUEUE_BOOK = 2
MAX_BORROW_DURATION = 14 * 24

@dataclass
class Queue:
    user_id: int
    book_id: int
    queue_start: float
    queue_end: Optional[float] = None

@dataclass
class Loan:
    user_id: int
    book_id: int
    loan_start: float
    loan_end: Optional[float] = None
    queue_id: Optional[float] = None

@dataclass
class Book:
    available_quantity: int

class RequestBorrowStatus(Enum):
    loaned = 'loaned'
    cancelled = 'cancelled'
    queued = 'queued'

def add_queue(user_id: int, book_id: int, time: float, queues: list[Queue]) -> None:
    queues.append(Queue(user_id, book_id, time))
    # print(f'Queue added: queue_id={len(queues)-1}, {user_id=}, {book_id=}, {time=:.4f}')

def lend_book(
    user_id: int,
    book_id: int,
    time: float,
    loans: list[Loan],
    books: list[Book],
    queue_id: Optional[int] = None,
) -> int:
    ''' Lend book to user. Returns the loan_id. '''
    book = books[book_id]
    book.available_quantity -= 1
    assert book.available_quantity >= 0
    loans.append(Loan(user_id, book_id, time, queue_id=queue_id))
    # print(f'Book loaned: loan_id={len(loans)-1}, {user_id=}, {book_id=}, {time=:.4f}')
    return len(loans) - 1

def user_can_borrow(user_id: int, loans: list[Loan]) -> bool:
    current_loans = get_current_loans(user_id, loans)
    if len(current_loans) >= MAX_BORROW_BOOK:
        return False

    return True

def user_can_request_borrow(user_id: int, queues: list[Queue], loans: list[Loan]) -> bool:
    active_queues = get_active_queues(user_id, queues)
    n_queues = len(active_queues)
    if n_queues >= MAX_QUEUE_BOOK:
        return False

    current_loans = get_current_loans(user_id, loans)
    n_loans = len(current_loans)
    if n_loans >= MAX_BORROW_BOOK:
        return False

    # Without the following rule, a person currently queueing 1 book and
    # holding MAX_BORROW_BOOK, and when the queued book finally made available
    # the queued will never be freed if there's no one else returning that
    # book. Hence we will forbid any borrow request with the following
    # condition.

    if (n_queues + n_loans) >= MAX_BORROW_BOOK:
        return False

    return True

def return_book(
    loan_id: int,
    time: float,
    loans: list[Loan],
    books: list[Book],
    queues: list[Queue],
) -> Optional[int]:
    '''
    Return book to library. Returns next loan_id if the book is loaned to
    someone from the queues, or None if there is no available queue.
    '''
    # print(f'Book returned: {loan_id=}, {time=:.4f}')
    book_id = loans[loan_id].book_id
    books[book_id].available_quantity += 1
    loans[loan_id].loan_end = time

    for queue_id,queue in enumerate(queues):
        user_id = queue.user_id
        if queue.queue_end is not None: continue
        if queue.book_id != book_id: continue
        if not user_can_borrow(user_id, loans): continue

        queue.queue_end = time
        next_loan_id = lend_book(
            user_id,
            book_id,
            time,
            loans,
            books,
            queue_id
        )
        return next_loan_id
    return None

def get_active_queues(user_id: int, queues: list[Queue]) -> list[Queue]:
    active_queues = [queue for queue in queues if queue.user_id == user_id and
                     queue.queue_end is None]
    return active_queues

def get_current_loans(user_id: int, loans: list[Loan]) -> list[Loan]:
    current_loans = [loan for loan in loans if loan.user_id == user_id and
                     loan.loan_end is None]
    return current_loans

def request_borrow(
    user_id: int,
    book_id: int,
    time: float,
    loans: list[Loan],
    queues: list[Queue],
    books: list[Book]
) -> tuple[int, RequestBorrowStatus]:
    if not user_can_request_borrow(user_id, queues, loans):
        return (-1, RequestBorrowStatus.cancelled)

    if books[book_id].available_quantity <= 0:
        add_queue(user_id, book_id, time, queues)
        return (-1, RequestBorrowStatus.queued)

    loan_id = lend_book(user_id, book_id, time, loans, books)
    return (loan_id, RequestBorrowStatus.loaned)

@dataclass
class Event:
    time: float
    detail: str
    execute: Callable[[list['Event']], None]

def insert_event(event: Event, events: list[Event]) -> None:
    '''
    Insert event to events, and keep the list sorted by .time
    '''
    insort(events, event, key=lambda item: item.time)

def create_next_return_event(
    loan_id: int,
    time: float,
    loans: list[Loan],
    books: list[Book],
    queues: list[Queue],
    min_borrow_duration: float,
    max_borrow_duration: float,
    events: list[Event],
) -> None:
    borrow_duration = np.clip(np.random.uniform(low=min_borrow_duration, high=max_borrow_duration), 0, MAX_BORROW_DURATION)
    loan_end = time + borrow_duration
    next_return_event = create_return_event(loan_id, loan_end, loans, books, queues, min_borrow_duration, max_borrow_duration)
    insert_event(next_return_event, events)

def create_return_event(
    loan_id: int,
    loan_end: float,
    loans: list[Loan],
    books: list[Book],
    queues: list[Queue],
    min_borrow_duration: float,
    max_borrow_duration: float,
) -> Event:
    def return_event(events):
        next_loan_id = return_book(loan_id, loan_end, loans, books, queues)
        if next_loan_id is None: return
        create_next_return_event(next_loan_id, loan_end, loans, books, queues, min_borrow_duration, max_borrow_duration, events)
    detail = f'return_event({loan_id=}, {loan_end=})'
    return Event(loan_end, detail, return_event)

def create_request_borrow_event(
    time: float,
    loans: list[Loan],
    books: list[Book],
    queues: list[Queue],
    n_users: int,
    n_books: int,
    min_borrow_duration: float,
    max_borrow_duration: float,
) -> Event:
    user_id = np.random.randint(n_users)
    book_id = np.random.randint(n_books)
    def request_borrow_event(events):
        (loan_id, status) = request_borrow(user_id, book_id, time, loans, queues, books)
        if status == RequestBorrowStatus.loaned:
            create_next_return_event(loan_id, time, loans, books, queues, min_borrow_duration, max_borrow_duration, events)
    detail = f'request_borrow_event({user_id=}, {book_id=})'
    return Event(time, detail, request_borrow_event)

def run_simulation(
    n_books: int,
    n_users: int,
    num_days: int, # simulate how many days?
    min_borrow_duration: float,
    max_borrow_duration: float,
    min_book_qty: int,
    max_book_qty: int,
    arrival_interval: float, # customers arrive every how many hours (on average)?
    seed: Optional[int]=None,
) -> tuple[
    list[Book],
    list[Queue],
    list[Loan],
]:
    assert n_books > 0
    assert n_users > 0
    assert num_days > 0
    assert min_borrow_duration >= 0
    assert max_borrow_duration > 0
    assert min_book_qty >= 0
    assert max_book_qty > 0
    assert arrival_interval > 0
    assert min_book_qty <= max_book_qty
    assert min_borrow_duration <= max_borrow_duration

    np.random.seed(seed)
    arrival_per_hour = 1 / arrival_interval
    sample_size = int(num_days * 24 * arrival_per_hour)
    print(f'{sample_size=}')

    # we use Poisson distribution with lambda = arrival_per_hour
    arrival_intervals = np.random.exponential(1/arrival_per_hour, size=sample_size)
    arrival_times = arrival_intervals.cumsum()

    queues: list[Queue] = []
    loans: list[Loan] = []

    book_quantities = np.random.randint(min_book_qty, max_book_qty + 1, size=n_books)
    books = [Book(qty) for qty in book_quantities]

    events = [
        create_request_borrow_event(
            time,
            loans,
            books,
            queues,
            n_users,
            n_books,
            min_borrow_duration,
            max_borrow_duration,
        )
        for time in arrival_times
    ]

    executed_events = []
    num_episode = 0
    while events:
        if num_episode % 500 == 0:
            print('#', end='')
        event = events.pop(0)
        event.execute(events)
        executed_events.append(event)
        num_episode += 1
    print()
    print(f'{num_episode=}')

    has_problem = False

    print('Checking that all books are returned...')
    for i,(book,total_quantity) in enumerate(zip(books, book_quantities)):
        if book.available_quantity != total_quantity:
            print(f'- {i}, {book.available_quantity=}, {total_quantity=}')
            has_problem = True

    print('Checking that all queues are cleared up...')
    for i,queue in enumerate(queues):
        if queue.queue_end is None:
            print('-', i, queue)
            has_problem = True

    print('Checking that all loans have loan_end...')
    for i,loan in enumerate(loans):
        if loan.loan_end is None:
            print('-', i, loan)
            has_problem = True

    if has_problem:
        raise RuntimeError('The simulation has problems!')

    return (books, queues, loans)

if __name__ == '__main__':
    import pandas as pd

    min_borrow_days = 1
    max_borrow_days = 14
    books, queues, loans = run_simulation(
        n_books = 491,
        n_users = 300,
        num_days = 365,
        min_borrow_duration = min_borrow_days * 24,
        max_borrow_duration = max_borrow_days * 24,
        min_book_qty = 3,
        max_book_qty = 10,
        arrival_interval = 0.5, # customer arrive every _ hours
        seed = 300397,
    )

    df_queues = pd.DataFrame(queues)
    df_loans = pd.DataFrame(loans)
    print(f'{df_queues.shape=}')
    print(f'{df_loans.shape=}')

    df_loans['borrow_duration'] = df_loans['loan_end'] - df_loans['loan_start']
    df_queues['queue_duration'] = df_queues['queue_end'] - df_queues['queue_start']

