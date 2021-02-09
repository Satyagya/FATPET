package com.engati.data.analytics.engine.execute;

import com.google.gson.JsonArray;

public interface DruidQueryExecutor {

  JsonArray getResponseFromDruid(String druidJsonQuery, Integer botRef, Integer customerId);
}
