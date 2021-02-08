package com.engati.data.analytics.engine.ingestionHandler;

public interface DruidSqlResponseService {

  String getDruidSqlResponse(Long customerId, Long botRef, String druidSqlQuery);
}
