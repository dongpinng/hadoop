package com.xj.original.controller;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.xj.original.entity.QueryResponse;
import com.xj.original.service.QueryService2;



@Controller
public class QueryController2 {
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	List<QueryResponse> qr = null;
	@Scheduled(cron = " 0 0 15 * * *")
	public void getddd(){
		String end_time = sdf.format(new Date());
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -1);
		String start_time = sdf.format(c.getTime());
		qr = qService.getxjData(start_time,end_time);
	}
	@Resource
	private QueryService2 qService;
	
	
	//贵州拦截数据
	@RequestMapping("/gzIntercept")
	@ResponseBody
	public List<QueryResponse> getGzIntercept(HttpServletRequest request,HttpServletResponse response){
		return qr;
	}
	
	@RequestMapping("/index2")
	public String show2(){
		return "gzIntercept";
	}
}
