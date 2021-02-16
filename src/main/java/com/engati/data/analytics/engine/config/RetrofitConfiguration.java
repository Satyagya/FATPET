package com.engati.data.analytics.engine.config;

import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.concurrent.TimeUnit;

@Component
public class RetrofitConfiguration {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Gson gson = new Gson();

  @Bean(name = "retrofitDruidApi")
  public Retrofit retrofitDruidApi(DruidQueryConfiguration druidQueryConfiguration) {
    Retrofit.Builder builder = new Retrofit.Builder().baseUrl(druidQueryConfiguration.getUrl()).client(
        new OkHttpClient().newBuilder()
            .connectTimeout(druidQueryConfiguration.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(druidQueryConfiguration.getReadTimeout(), TimeUnit.MILLISECONDS).build())
        .addConverterFactory(GsonConverterFactory.create(gson))
        .addCallAdapterFactory(RxJava2CallAdapterFactory.create());
    return builder.build();
  }

  @Bean(name = "retrofitDruidApiService")
  public DruidServiceRetrofit retrofitDruidApiService(
      @Qualifier(value = "retrofitDruidApi") Retrofit retrofitDruidApi) {
    return retrofitDruidApi.create(DruidServiceRetrofit.class);
  }
}
