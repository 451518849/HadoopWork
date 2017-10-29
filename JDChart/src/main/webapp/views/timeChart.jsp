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
<h2>������ͳ��</h2>
${words}
<!-- ΪECharts׼��һ���߱���С����ߣ���Dom -->
<div id="main" style="width: 100%;height:800px;"></div>
<script type="text/javascript">

    $.ajax({
        url:'../spark/monthCount',
        dataType:"json",
        success:function(data,textStatus,jqXHR){

            var words = [];
            var nums = [];


            for(var i = 0;i < data.months.length; i++){
                words[i] = data.months[i][0];
                nums[i] = data.months[i][1];
            }


            // ����׼���õ�dom����ʼ��echartsʵ��
            var myChart = echarts.init(document.getElementById('main'));

            // ָ��ͼ��������������
            option = {
                title: {
                    text: '���·�ͳ��������',
                    left: 'center'
                },
                tooltip: {
                    trigger: 'item',
                    formatter: '{a} <br/>{b} : {c}'
                },
                legend: {
                    left: 'left',
                    data: ['������']
                },
                xAxis: {
                    type: 'category',
                    name: 'ʱ��',
                    splitLine: {show: false},
                    data: words
                },
                grid: {
                    left: '3%',
                    right: '4%',
                    bottom: '3%',
                    containLabel: true
                },
                yAxis: {
                    type: 'log',
                    name: '����'
                },
                series: [
                    {
                        name: '3��ָ��',
                        type: 'line',
                        data: nums
                    },
                ]
            };


            // ʹ�ø�ָ�����������������ʾͼ��
            myChart.setOption(option);

        },
        error:function(xhr,textStatus){
            alert('����')
        },
        complete:function(){
        }

    })


    //    myChart.setOption({
    //        series : [
    //            {
    //                name: '������Դ',
    //                type: 'pie',
    //                radius: '55%',
    //                data:[
    //                    {value:235, name:'��Ƶ���'},
    //                    {value:274, name:'���˹��'},
    //                    {value:310, name:'�ʼ�Ӫ��'},
    //                    {value:335, name:'ֱ�ӷ���'},
    //                    {value:400, name:'��������'}
    //                ]
    //            }
    //        ]
    //    })
</script>
</body>
</html>
