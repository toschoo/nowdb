use db100;

insert into client(client_key, client_name) (9000001, 'Popeye the Sailor');
insert into product(prod_key, prod_desc, prod_price) (100001, 'Spinach, 450g net', 1.99);
insert into product(prod_key, prod_desc, prod_price) (100002, 'Candy Cigarettes, 20', 2.49);

insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-17T09:35:12', 1, 1.99);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-19T10:15:01', 2, 3.98);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-20T17:12:55', 3, 5.97);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-22T08:27:32', 1, 1.99);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-25T12:09:59', 1, 1.99);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100001, '1929-01-26T21:19:44', 2, 3.98);
insert into sales (edge, origin, destin, timestamp, weight, weight2)
                  ('buys', 9000001, 100002, '1929-01-22T08:27:51', 1, 2.49); 
