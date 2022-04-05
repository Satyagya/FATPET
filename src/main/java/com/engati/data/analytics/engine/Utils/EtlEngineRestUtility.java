package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.constants.constant.Constants;
import com.fasterxml.jackson.databind.JsonNode;
import net.minidev.json.JSONObject;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface EtlEngineRestUtility {

    String DUCK_DB_EXECUTE_QUERY = Constants.DUCK_DB_EXECUTE_QUERY;
    String DUCK_DB_EXECUTE_QUERY_DETAILS = Constants.DUCK_DB_EXECUTE_QUERY_DETAILS;

    @POST(value = DUCK_DB_EXECUTE_QUERY)
    Call<JsonNode> executeQuery(@Body JSONObject jsonObject);

    @POST(value = DUCK_DB_EXECUTE_QUERY_DETAILS)
    Call<JSONObject> executeQueryDetails(@Body JSONObject jsonObject);

}
