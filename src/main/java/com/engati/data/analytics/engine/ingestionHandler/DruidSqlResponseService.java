package com.engati.data.analytics.engine.ingestionHandler;

import com.google.gson.JsonArray;
import org.springframework.http.ResponseEntity;

public interface DruidSqlResponseService {

  ResponseEntity<JsonArray> getDruidSqlResponse(Long customerId, Long botRef, String druisSqlQuery);
}
