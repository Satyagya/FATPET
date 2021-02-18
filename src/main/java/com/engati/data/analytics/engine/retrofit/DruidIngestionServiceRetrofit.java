package com.engati.data.analytics.engine.retrofit;

import com.google.gson.JsonObject;
import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface DruidIngestionServiceRetrofit {

  @POST("/druid/indexer/v1/task")
  Call<JsonObject> ingestDataToDruid(@Body RequestBody ingestionSpecs);
}

