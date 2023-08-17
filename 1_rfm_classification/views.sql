-- create views
-- view orderitems
CREATE OR REPLACE VIEW analysis.orderitems AS
SELECT * 
FROM production.orderitems;

-- view orders
CREATE OR REPLACE VIEW analysis.orders AS
SELECT * 
FROM production.orders;

--view orderstatuses
CREATE OR REPLACE VIEW analysis.orderstatuses AS
SELECT * 
FROM production.orderstatuses;

-- view products
CREATE OR REPLACE VIEW analysis.products AS
SELECT * 
FROM production.products;

-- view users
CREATE OR REPLACE VIEW analysis.users AS
SELECT * 
FROM production.users;