<%@ page language="java" contentType="text/html; charset=GB18030"
         pageEncoding="GB18030"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=GB18030">
    <script src="../js/echarts.common.min.js"></script>
    <script src="../js/jquery.js"></script>

</head>
<body>
<h2>评论形容次统计</h2>
${words}
<!-- 为ECharts准备一个具备大小（宽高）的Dom -->
<div id="main" style="width: 100%;height:800px;"></div>
<script type="text/javascript">

    $.ajax({
        url:'../spark/wordCount',
        dataType:"json",
        success:function(data,textStatus,jqXHR){

            var words = [];
            var nums = [];


            for(var i = 0;i < data.words.length; i++){
                words[i] = data.words[i][0];
                nums[i] = data.words[i][1];
            }


            // 基于准备好的dom，初始化echarts实例
            var myChart = echarts.init(document.getElementById('main'));

            // 指定图表的配置项和数据
            var option = {
                title: {
                    text: '形容词统计表'
                },
                tooltip: {},
                legend: {
                    data:['词频']
                },
                xAxis: {
                    data: words
                },
                yAxis: {},
                series: [{
                    name: '词频',
                    type: 'bar',
                    data: nums
                }]
            };

            // 使用刚指定的配置项和数据显示图表。
            myChart.setOption(option);

        },
        error:function(xhr,textStatus){
            alert('错误')
        },
        complete:function(){
        }

    })


    //    myChart.setOption({
    //        series : [
    //            {
    //                name: '访问来源',
    //                type: 'pie',
    //                radius: '55%',
    //                data:[
    //                    {value:235, name:'视频广告'},
    //                    {value:274, name:'联盟广告'},
    //                    {value:310, name:'邮件营销'},
    //                    {value:335, name:'直接访问'},
    //                    {value:400, name:'搜索引擎'}
    //                ]
    //            }
    //        ]
    //    })
</script>
</body>
</html>
