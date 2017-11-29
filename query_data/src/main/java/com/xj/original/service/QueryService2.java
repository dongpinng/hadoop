package com.xj.original.service;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.xj.original.dao.QueryDao2;
import com.xj.original.entity.QueryPojo;
import com.xj.original.entity.QueryResponse;


@Service
public class QueryService2 {
	@Resource
	private QueryDao2 qDao;
	public List<QueryResponse> getxjData(String start_time,String end_time){
		return qDao.getXjData(start_time, end_time);
	};
}
