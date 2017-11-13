var	http = require('http');
var i=0;
doPost();
function doPost(){
	var post_data = {
		'key':i,
		'value':{
			'column1':'name'+i,
			'column2':'psw'+i
		}
	}
	console.log(i);
	post_data = JSON.stringify(post_data);

	var post_options = {
		    host: '192.168.1.102',
		    port: '8500',
		    method: 'POST',
		    path: '/process'
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
			i++;

			if(i==1002){
				return;
			}
			doPost();
		});
	});

	post_req.write(post_data);
	post_req.end();
}

function post(){
/*	var post_data = {
		'key': '102',
		'value': {
			'column1': 'value2',
			'column2': 'value2'
		}
	};*/
	for(var i=0;i<1;i++){
		var post_data = {
			'key':i,
			'value':{
				'column1':'name'+i,
				'column2':'psw'+i
			}
		}
		doPost(post_data);
		console.log(i);
	}

}
//post();