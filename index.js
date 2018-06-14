var mysql		= require('sync-mysql');
var fs			= require('fs');
var format		= require('date-format');
var connection	= new mysql({
	host     : '',
	user     : '',
	password : '',
	database : '',
	multipleStatements: true
});

var table_name = '_conversions',
	last_date = '', // ' AND `date_time` > \'2017-11-09 10:38:49\' '
	users_count,
	steps_count,
	limit = 1000;

// fs.exists(table_name + '.csv', function(exists) {
// 	if(exists) {
// 		fs.unlinkSync(table_name + '.csv');
// 	}
// });

var users_count = connection.query("SELECT COUNT(DISTINCT `num_customer_id`) as `users_count` FROM `tracking`.`conversions` WHERE (`conv_type` = 'optin' OR `conv_type` = 'upsell' OR `conv_type` = 'sale') AND `num_customer_id` IS NOT NULL AND `num_customer_id`<> '' AND `num_customer_id`<> 'undefined'" + last_date)[0]['users_count'];

steps_count = getStepsCount(users_count , limit);
console.log('limit=', limit, ' users_count=', users_count, ' steps_count=', steps_count, '\n');

var j = 0;

for (var i = 0; i < steps_count; i++) {
	ids = connection.query("SELECT DISTINCT `num_customer_id` FROM `tracking`.`conversions` WHERE `conv_type` = 'optin' AND `num_customer_id` IS NOT NULL AND `num_customer_id`<> '' AND `num_customer_id`<> 'undefined' " + last_date + " ORDER BY `date_time` LIMIT ?, ?", [ i * limit, limit ]);
	ids.forEach(function(value, index, bigArr){
		results = connection.query("SELECT MIN(`OD`.`date_time`) AS `optin_date` FROM ( (SELECT `date_time` FROM `tracking`.`conversions` WHERE `num_customer_id` = '" + value.num_customer_id + "' AND `conv_type` = 'optin') UNION (SELECT `date_time` FROM `tracking`.`conversions_archive` WHERE `num_customer_id` = '" + value.num_customer_id + "' AND `conv_type` = 'optin') ) AS `OD`; SELECT `sales`.`date_time` AS `sale_date`, `sales`.`tr` AS `sale_summ` FROM ( (SELECT `date_time`, `tr` FROM `tracking`.`conversions` WHERE `num_customer_id` = '" + value.num_customer_id + "' AND `tr` > 0 AND (`conv_type` = 'sale' OR `conv_type` = 'upsell')) UNION (SELECT `date_time`, `tr` FROM `tracking`.`conversions_archive` WHERE `num_customer_id` = '" + value.num_customer_id + "' AND `tr` > 0 AND (`conv_type` = 'sale' OR `conv_type` = 'upsell')) ) AS `sales` ORDER BY `sale_date` ASC;");

		var optin_date = results[0].length > 0 ? new Date( results[0][0].optin_date ) : '';

		if (results[1].length > 0) {
			var s_date = new Date(results[1][0].sale_date);
			writeToCSV({
				"user ID": value.num_customer_id,
				"opt-in date": format('yyyy-MM-dd hh:mm:ss', optin_date),
				"first sale date": format('yyyy-MM-dd hh:mm:ss', s_date),
				"7-days since opt-in LTV": getSummByInterval(optin_date, 7, results[1]).toFixed(2),
				"14-days since opt-in LTV": getSummByInterval(optin_date, 14, results[1]).toFixed(2),
				"30-days since opt-in LTV": getSummByInterval(optin_date, 30, results[1]).toFixed(2),
				"90-days since opt-in LTV": getSummByInterval(optin_date, 90, results[1]).toFixed(2),
				"7-days since first sale LTV": getSummByInterval(s_date, 7, results[1]).toFixed(2),
				"14-days since first sale LTV": getSummByInterval(s_date, 14, results[1]).toFixed(2),
				"30-days since first sale LTV": getSummByInterval(s_date, 30, results[1]).toFixed(2),
				"90-days since first sale LTV": getSummByInterval(s_date, 90, results[1]).toFixed(2),
				"Full LTV": getSumm(results[1]).toFixed(2)
			});
		} else {
			writeToCSV({
				"user ID": value.num_customer_id,
				"opt-in date": format('yyyy-MM-dd hh:mm:ss', optin_date)
			});
		}

		j++
		console.log(j);
	});
}


function getStepsCount(users_count , limit){
	return Math.ceil( users_count / limit );
}

function getSummByInterval(start, interval, arr){
	var summ	= 0,
	start	= new Date(start),
	end		= addDays(start, interval);
	end = new Date(end);
	arr.forEach(function(value){
		var sale_date = new Date(value.sale_date);
		if ( sale_date >= start && sale_date <= end ){
			summ += value.sale_summ;
		}
	});
	return summ;
}

function getSumm(arr){
	var summ	= 0;
	arr.forEach(function(value){
		summ += value.sale_summ;
	});
	return summ;
}

function addDays(date, days) {
	var result = new Date(date);
	result.setDate(result.getDate() + days);
	return result;
}


function writeToCSV(obj){

	obj["first sale date"] = typeof obj["first sale date"] !== 'undefined' ? obj["first sale date"] : '';
	obj["7-days since opt-in LTV"] = typeof obj["7-days since opt-in LTV"] !== 'undefined' ? obj["7-days since opt-in LTV"] : 0;
	obj["14-days since opt-in LTV"] = typeof obj["14-days since opt-in LTV"] !== 'undefined' ? obj["14-days since opt-in LTV"] : 0;
	obj["30-days since opt-in LTV"] = typeof obj["30-days since opt-in LTV"] !== 'undefined' ? obj["30-days since opt-in LTV"] : 0;
	obj["90-days since opt-in LTV"] = typeof obj["90-days since opt-in LTV"] !== 'undefined' ? obj["90-days since opt-in LTV"] : 0;
	obj["7-days since first sale LTV"] = typeof obj["7-days since first sale LTV"] !== 'undefined' ? obj["7-days since first sale LTV"] : 0;
	obj["14-days since first sale LTV"] = typeof obj["14-days since first sale LTV"] !== 'undefined' ? obj["14-days since first sale LTV"] : 0;
	obj["30-days since first sale LTV"] = typeof obj["30-days since first sale LTV"] !== 'undefined' ? obj["30-days since first sale LTV"] : 0;
	obj["90-days since first sale LTV"] = typeof obj["90-days since first sale LTV"] !== 'undefined' ? obj["90-days since first sale LTV"] : 0;
	obj["Full LTV"] = typeof obj["Full LTV"] !== 'undefined' ? obj["Full LTV"] : 0;

	var data = '';
	for (prop in obj){
		data += obj[prop] + ( (prop !== 'Full LTV') ? ',' : '\n' );
	}
	fs.appendFileSync(table_name + '.csv', data);
	console.log('User with ID ' + obj['user ID'] + ' recorded');
}