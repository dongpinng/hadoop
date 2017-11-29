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
			<label>江苏移动数据</label> <input id="product" type="text" name="product"
				placeholder="请输入产品代号..."> <span class="help-block">Ps:炫佳乐园(0020412cp0004),炫力动漫(0020412cp0005)</span>
			<button class="btn" onclick="getXjData()">查询</button>
		</fieldset>
		<table id="data"class="table table-striped table-bordered">
			<thead>
				<tr>
					<th>Time</th>
					<th>UserID</th>
					<th>Counts</th>
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
		var product = document.getElementById("product").value;
		if (product == "") {
			alert("不可以为空");
			return 0;
		}
		var url = "xjdata";
		var params = {
			"product" : product
		};
		$("tbody").html("");
		$.post(url, params, function(result) {
			var temp = "";
			for (i = 0; i < result.length; i++) {
				temp += "<tr><td>" + time + "</td><td>" + result[i].userid
						+ "</td><td>" + result[i].counts + "</td></tr>";
			}
			$("tbody").html(temp);
		});
	}
	</script>

</html>