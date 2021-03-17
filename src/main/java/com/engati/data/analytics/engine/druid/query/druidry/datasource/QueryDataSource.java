package com.engati.data.analytics.engine.druid.query.druidry.datasource;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class QueryDataSource extends DruidDataSource{
  private static final String QUERY_DATA_SOURCE = "query";
  private Object query;

  public QueryDataSource(@NonNull Object query) {
    this.type = QUERY_DATA_SOURCE;
    this.query = query;
  }
}
