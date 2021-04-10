drop schema retail if exists;
create schema retail; use retail;

create type client (
  id uint pk,
  name text
);

/* this is a product
 * id   : the primary key
 * title: what you call the product
 * price: what it costs
 */
create type product (
  id uint pk,
  title text,
  price float
);

create edge buys ( /* here is an edge */
  origin client origin,
  destin product destin,
  stamp time stamp, -- this is an attribute
  quantity int,
  amount float
);

-- now we insert something
insert into client (id, name)
            values (1, 'otto');

/* yet another insert */
insert into product (id, title, price)
            values (1, 'pizza \'speciale\'\nFamily Size\tYou love it! Or money back!', 5.99);

insert into buys (origin, destin, stamp, quantity, amount)
            values (1, 1, '2021-03-01T17:30:00', 5, 29.95); 
-- end of file
/*
   beyond the end...
*/
