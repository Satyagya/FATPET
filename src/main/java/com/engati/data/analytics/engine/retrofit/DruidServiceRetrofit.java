package com.engati.data.analytics.engine.retrofit;

import com.google.gson.JsonArray;
import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface DruidServiceRetrofit {

  @POST("/druid/v2/?pretty")
  Call<JsonArray> getResponseFromDruid(@Body RequestBody druidJsonQuery);
}
