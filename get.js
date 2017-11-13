var	http = require('http');

var post_options = {
	    host: '192.168.1.102',
	    port: '8500',
	    path: '/process',
	    method: 'GET',
	    headers:{
	    	key: '0'
	    }
};

var req = http.request(post_options, function(res) {
	console.log('响应开始****');
	console.log('Status:' + res.statusCode);
	res.setEncoding('utf8');
	res.on('data', function (chunk) {
		console.log('Response: ' + chunk);
		// onetimetoken_data = JSON.parse(chunk);			        
	});
	res.on('end',function(){
		console.log('响应结束****');
	});
});

req.on('error',function(err){
	console.error(err);
});
req.end();