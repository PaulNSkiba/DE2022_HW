/*
 Завдання на SQL до лекції 02.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT 	c."name" , count(*) cnt
FROM	public.film_category fc 
		JOIN public.film f ON fc.film_id = f.film_id 
		JOIN public.category c ON fc.category_id = c.category_id 
GROUP BY c."name" 		
ORDER BY cnt desc; 




/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

SELECT 	a.first_name, a.last_name, COUNT(*) cnt
FROM 	public.actor a 
		JOIN public.film_actor fa ON a.actor_id = fa.actor_id 
		JOIN public.inventory i ON i.film_id = fa.film_id 
		JOIN public.rental r ON r.inventory_id = i.inventory_id 
GROUP BY a.first_name, a.last_name
ORDER BY cnt DESC
LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

SELECT 	category 
FROM 	public.sales_by_film_category sbfc 
ORDER BY total_sales DESC
LIMIT 1;
		


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT 	f.title 
FROM 	public.film f 
		LEFT JOIN public.inventory i ON f.film_id = i.film_id 
WHERE 	i.film_id IS null; 		

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

SELECT 	a.first_name, a.last_name, count(fc.*) cnt
FROM 	public.actor a 
		JOIN public.film_actor fa ON a.actor_id = fa.actor_id 
		JOIN public.film_category fc ON fa.film_id = fc.film_id 
		JOIN public.category c ON c.category_id = fc.category_id 
WHERE	c."name" = 'Children'	
GROUP BY a.first_name, a.last_name
ORDER BY cnt DESC
LIMIT 3;		


/*
6.
Вивести міста з кількістю активних та неактивних клієнтів
(в активних customer.active = 1).
Результат відсортувати за кількістю неактивних клієнтів за спаданням.
*/

SELECT  c2.city, SUM(CASE WHEN c.active = 1 THEN 1 ELSE 0 END) active_customer, SUM(CASE WHEN c.active = 1 THEN 0 ELSE 1 END) not_active_customer
FROM	public.customer c 
		JOIN public.address a ON c.address_id = a.address_id 
		JOIN public.city c2 ON a.city_id = c2.city_id 
GROUP BY c2.city
ORDER BY not_active_customer DESC;

