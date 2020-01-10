"""
SELECT DISTINCT
  c.name AS "c_name",
  c.age AS "c_age"
FROM
  customer AS c
INNER JOIN product AS p ON p.id = c.product_id
WHERE
  c_age >= 18
  AND
  c_age IS NOT NULL
ORDER BY
  c_age DESC
LIMIT 100
;
"""

INNER JOIN table1 on table2.col_name = table1.col_name;
