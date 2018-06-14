-- query of line 24 (index.js):
SELECT
	COUNT(DISTINCT `num_customer_id`) as `users_count`
FROM
	`tracking`.`conversions`
WHERE
	(
		`conv_type` = 'optin' OR
		`conv_type` = 'upsell' OR
		`conv_type` = 'sale'
	) AND
	`num_customer_id` IS NOT NULL AND
	`num_customer_id`<> '' AND
	`num_customer_id`<> 'undefined'

-- query of line 32 (index.js):
SELECT DISTINCT
	`num_customer_id`
FROM
	`tracking`.`conversions`
WHERE
	`conv_type` = 'optin' AND
	`num_customer_id` IS NOT NULL AND
	`num_customer_id`<> '' AND
	`num_customer_id`<> 'undefined'
ORDER BY `date_time` LIMIT ?, ?

-- query of line 34 (index.js):
SELECT
	MIN(`OD`.`date_time`) AS `optin_date`
FROM
	(
		(
			SELECT
				`date_time`
			FROM
				`tracking`.`conversions`
			WHERE
				`num_customer_id` = ? AND
				`conv_type` = 'optin'
		) UNION (
			SELECT
				`date_time`
			FROM
				`tracking`.`conversions_archive`
			WHERE
				`num_customer_id` = ? AND
				`conv_type` = 'optin'
		)
	) AS `OD`;
SELECT
	`sales`.`date_time` AS `sale_date`,
	`sales`.`tr` AS `sale_summ`
FROM
	(
		(
			SELECT
				`date_time`,
				`tr`
			FROM
				`tracking`.`conversions`
			WHERE
				`num_customer_id` = ? AND
				`tr` > 0 AND (
					`conv_type` = 'sale' OR
					`conv_type` = 'upsell'
				)
		) UNION (
			SELECT
				`date_time`,
				`tr`
			FROM
				`tracking`.`conversions_archive`
			WHERE
				`num_customer_id` = ? AND
				`tr` > 0 AND (
					`conv_type` = 'sale' OR
					`conv_type` = 'upsell'
				)
		)
	) AS `sales`
ORDER BY `sale_date` ASC;