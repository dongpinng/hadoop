package com.xj.original.dao;

import java.util.List;

import com.xj.original.entity.QueryPojo;
import com.xj.original.entity.QueryResponse;


public interface QueryDao2 {
	List<QueryResponse> getXjData(String start_time,String end_time);
}
