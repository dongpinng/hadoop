package com.xj.common.dao;

import java.util.List;

import com.xj.common.entity.QueryPojo;
import com.xj.common.entity.QueryResponse;

public interface QueryDao {
	List<QueryResponse> getXjData(QueryPojo pojo);
}
