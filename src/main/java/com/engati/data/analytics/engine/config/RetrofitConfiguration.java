package com.engati.data.analytics.engine.config;

import com.engati.data.analytics.engine.retrofit.DruidIngestionServiceRetrofit;
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
  public Retrofit retrofitDruidApi(DruidConfiguration druidConfiguration) {
    Retrofit.Builder builder = new Retrofit.Builder().baseUrl(druidConfiguration.getUrl()).client(
        new OkHttpClient().newBuilder()
            .connectTimeout(druidConfiguration.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(druidConfiguration.getReadTimeout(), TimeUnit.MILLISECONDS).build())
        .addConverterFactory(GsonConverterFactory.create(gson))
        .addCallAdapterFactory(RxJava2CallAdapterFactory.create());
    return builder.build();
  }

  @Bean(name = "retrofitDruidApiService")
  public DruidServiceRetrofit retrofitDruidApiService(
      @Qualifier(value = "retrofitDruidApi") Retrofit retrofitDruidApi) {
    return retrofitDruidApi.create(DruidServiceRetrofit.class);
  }

  @Bean(name = "retrofitDruidIngestionApi")
  public Retrofit retrofitDruidIngestionApi(DruidIngestionConfiguration druidIngestionConfiguration) {
    Retrofit.Builder builder = new Retrofit.Builder().baseUrl(druidIngestionConfiguration.getUrl()).client(
        new OkHttpClient().newBuilder()
            .connectTimeout(druidIngestionConfiguration.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(druidIngestionConfiguration.getReadTimeout(), TimeUnit.MILLISECONDS).build())
        .addConverterFactory(GsonConverterFactory.create(gson))
        .addCallAdapterFactory(RxJava2CallAdapterFactory.create());
    return builder.build();
  }

  @Bean(name = "retrofitDruidIngestionApiService")
  public DruidIngestionServiceRetrofit retrofitDruidIngestionApiService(
      @Qualifier(value = "retrofitDruidIngestionApi") Retrofit retrofitDruidIngestionApi) {
    return retrofitDruidIngestionApi.create(DruidIngestionServiceRetrofit.class);
  }
}
