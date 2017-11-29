package com.xj.common.service;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.xj.common.dao.QueryDao;
import com.xj.common.entity.QueryPojo;
import com.xj.common.entity.QueryResponse;

@Service
public class QueryService {
	@Resource
	private QueryDao qDao;
	public List<QueryResponse> getxjData(QueryPojo pojo){
		return qDao.getXjData(pojo);
	};
}
