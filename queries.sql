select count(*) from loans;

select count(*) from queues;

select count(*) from users;

select count(*) from books;

select count(*) from categories;

select count(distinct loan_start::date) from loans;

select count(distinct author) from books;

-- top 5 categories
select category_name, count(*) as loan_count
from loans
left join books using(book_id)
left join categories using(category_id)
group by category_name
order by loan_count desc
limit 5;

-- top 5 authors by number of borrows
select author, count(*) as loan_count
from loans
left join books using(book_id)
group by author
order by loan_count desc
limit 5;

-- average borrow duration
select
	avg(loan_end::date - loan_start::date) as average_duration,
	min(loan_end::date - loan_start::date) as minimum_duration,
	max(loan_end::date - loan_start::date) as maximum_duration
from loans
where loan_end is not null;

-- most borrowed books each month
select date, title, count
from (
	select
		to_char(loan_start, 'yyyy-mm') as date,
		book_id,
		count(*) as count,
		rank() over(partition by to_char(loan_start, 'yyyy-mm')
					order by count(*) desc,
					book_id desc) as rank
	from loans
	group by date, book_id
)
left join books
	using(book_id)
where rank = 1;

-- most borrowed categories each month
select date, category_name, count
from (
	select
		to_char(loan_start, 'yyyy-mm') as date,
		category_id,
		count(*) as count,
		rank() over(partition by to_char(loan_start, 'yyyy-mm')
					order by count(*) desc,
					category_id desc) as rank
	from loans
	left join books
		using(book_id)
	group by date, category_id
)
left join categories
	using(category_id)
where rank = 1;

-- why only "Literature & Fiction"?

-- number of books in each category
select category_name, count(*) as count
from books
left join categories
	using(category_id)
group by category_name
order by count desc
limit 5;

-- books that are borrowed the longest
select title, author, avg_loan_duration
from (
	select book_id,
		avg(loan_end - loan_start) as avg_loan_duration
	from loans
	where loan_end is not null
	group by book_id
	order by avg_loan_duration desc
	limit 10
)
left join books using(book_id);

-- avg, min & max queue waiting time
select
	avg(queue_end - queue_start) as average,
	min(queue_end - queue_start) as minimum,
	max(queue_end - queue_start) as maximum
from queues
where queue_end is not null;
