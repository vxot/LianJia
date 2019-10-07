CREATE VIEW price_change_com AS SELECT
p.house_id AS house_id,
p.pre_price AS pre_price,
p.price AS price,
( p.price - p.pre_price ) AS priceChange,
Round( ( p.price - p.pre_price ) / p.pre_price * 100, 2 ) AS fudu,
p.change_time AS change_time
FROM
	price_change p


CREATE VIEW district_area AS SELECT
d1.id id,
d2.NAME district,
d1.NAME area,
d1.url url
FROM
	district d1,
	district d2
WHERE
	d2.id = d1.parent