var	http = require('http');
var post_data = {
	'101': {
		'colume':1
	},
	'102': {
		'colume':1
	}
};
console.log(post_data);

post_data = JSON.stringify(post_data);
console.log(post_data);

var post_options = {
	    host: '192.168.1.102',
	    port: '8500',
	    method: 'POST',
	    path: '/batchProcess'
};

var post_req = http.request(post_options, function(res) {
	console.log('响应开始****');
	console.log('Status:' + res.statusCode);
	res.setEncoding('utf8');
	res.on('data', function (chunk) {
		console.log('Response: ' + chunk);			        
	});
	res.on('end',function(){
		console.log('响应结束****');
	});

});

post_req.write(post_data);


post_req.end();