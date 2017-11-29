package com.xj.common.controller;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpRequest;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.xj.common.entity.QueryPojo;
import com.xj.common.entity.QueryResponse;
import com.xj.common.service.QueryService;


@Controller
public class QueryController {
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	@Resource
	private QueryService qService;
	
	@RequestMapping("/xjdata")
	@ResponseBody
	public List<QueryResponse> getData(HttpServletRequest request,HttpServletResponse response){
		String product = request.getParameter("product");
		QueryPojo  pojo = new QueryPojo();
		pojo.setEnd_time(sdf.format(new Date()));
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -30);
		pojo.setStart_time(sdf.format(c.getTime()));
		pojo.setProduct(product);
		qService.getxjData(pojo);
		return qService.getxjData(pojo);
	}
	
	@RequestMapping("/index")
	public String show(){
		return "hello";
	}
}
