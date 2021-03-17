package com.engati.data.analytics.engine.retrofit;

import com.google.gson.JsonArray;
import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Query;

import javax.ws.rs.QueryParam;

public interface DruidServiceRetrofit {

  @POST("/druid/v2/?pretty")
  Call<JsonArray> getResponseFromDruid(@Body RequestBody druidJsonQuery);

  @POST("/druid/v2/sql?pretty")
  Call<JsonArray> getResponseForDruidSqlFromDruid(@Body RequestBody druidSqlJsonQuery);

  @GET("/druid/indexer/v1/tasks")
  Call<JsonArray> getAllIngestionTasks(@Query("state") String state,
      @Query("createdTimeInterval") String createdTimeInterval);
}
