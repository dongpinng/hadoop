<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
<script src="jquery/jquery-3.2.1.min.js"></script>
<script src="js/bootstrap.min.js"></script>
<script type="text/javascript" src="js/jquery.base64.js"></script>
<script type="text/javascript" src="js/tableExport.js"></script>
<script type="text/javascript" src="js/base64.js"></script>  
<script type="text/javascript" src="js/jspdf.js"></script>  
<script type="text/javascript" src="js/sprintf.js"></script>  
<title>O(∩_∩)O~</title>
</head>
<body>
	<div style='text-align: center'>
		<button class="btn btn-large btn-primary" type="button" onclick="$('#data').tableExport({type: 'excel', escape: 'false'});">导出</button>
		<a id="down">下载</a>
		<fieldset>
			<legend>每日查询</legend>
			<label>贵州拦截数据</label> <input id="start_time" type="text" name="start_time"
				placeholder="请输入开始时间">——<input id="end_time" type="text" name="end_time"
				placeholder="请输入结束时间">
			<button class="btn" onclick="getXjData()">查询</button>
		</fieldset>
		<table id="data"class="table table-striped table-bordered">
			<thead>
				<tr>
					<th>page</th>
					<th>time</th>
					<th>pv</th>
					<th>uv</th>
					<th>orderUV</th>
					<th>newUV</th>
				</tr>
			</thead>
			<tbody>
			</tbody>
		</table>
	</div>
</body>
<script>
	function getXjData() {
		var time = new Date().toLocaleString().slice(0, 10);
		var start_time = document.getElementById("start_time").value;
		var end_time = document.getElementById("end_time").value
		if (start_time==""&&end_time=="") {
			alert("不可以为空");
			return 0;
		}
		var url = "gzIntercept";
		var params = {
			"start_time" : start_time,
			"end_time" : end_time
		};
		$("tbody").html("");
		$.post(url, params, function(result) {
			var temp = "";
			for (i = 0; i < result.length; i++) {
				temp += "<tr><td>拦截页面</td><td>" + result[i].time
						+ "</td><td>" + result[i].pv
						+ "</td><td>" + result[i].uv
						+ "</td><td>"+result[i].orderUV
						+ "</td><td>"+result[i].newUV
						+ "</tr>";
			}
			$("tbody").html(temp);
		});
	}
	</script>

</html>