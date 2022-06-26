package com.engati.data.analytics.engine.configuration.retrofit;

import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.Utils.PdeRestUtility;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.util.concurrent.TimeUnit;

@Configuration
@ConditionalOnClass(Retrofit.class)
@Slf4j
public class RetrofitConfiguration {
  @Bean(name = Constants.RETROFIT_DUCKDB_ENGINE_API)
  public Retrofit retrofitIntegrationHubApi(EtlEngineConfiguration etlEngineConfiguration,
      @Qualifier(value = Constants.DUCKDB_ENGINE_HTTP_CLIENT) OkHttpClient okHttpClient) {
    Retrofit.Builder builder = constructRetrofitBuilder(okHttpClient);
    builder.baseUrl(etlEngineConfiguration.getUrl())
        .addConverterFactory(JacksonConverterFactory.create(constructObjectMapper()))
        .addCallAdapterFactory(RxJava2CallAdapterFactory.create());
    return builder.build();
  }

  @Bean(name = Constants.RETROFIT_DUCKDB_ENGINE_REST_SERVICE)
  public EtlEngineRestUtility retrofitIntegrationHubRestService(
      @Qualifier(value = Constants.RETROFIT_DUCKDB_ENGINE_API) Retrofit retrofitIntegrationHubApi) {
    return retrofitIntegrationHubApi.create(EtlEngineRestUtility.class);
  }

  @Bean(name = Constants.DUCKDB_ENGINE_HTTP_CLIENT)
  public OkHttpClient okHttpIHClient(EtlEngineConfiguration etlEngineConfiguration) {

    return createHttpClient(etlEngineConfiguration.getLogLevel(),
        etlEngineConfiguration.getConnectTimeout(),
        etlEngineConfiguration.getReadTimeout());
  }

  @Bean(name = Constants.RETROFIT_PDE_API)
  public Retrofit retrofitDAEApi(ProductDiscoveryEngineConfiguration daeConfiguration,
      @Qualifier(value = Constants.PDE_CLIENT) OkHttpClient okHttpClient) {
    Retrofit.Builder builder = constructRetrofitBuilder(okHttpClient);
    builder.baseUrl(daeConfiguration.getUrl())
        .addConverterFactory(JacksonConverterFactory.create(constructObjectMapper()))
        .addCallAdapterFactory(RxJava2CallAdapterFactory.create());
    return builder.build();
  }

  @Bean(name = Constants.RETROFIT_PDE_SERVICE)
  public PdeRestUtility retrofitDAEService(
      @Qualifier(value = Constants.RETROFIT_PDE_API) Retrofit retrofitDAEApi) {
    return retrofitDAEApi.create(PdeRestUtility.class);
  }

  @Bean(name = Constants.PDE_CLIENT)
  public OkHttpClient okHttpDAEClient(ProductDiscoveryEngineConfiguration productDiscoveryEngineConfiguration) {
    return createHttpClient(productDiscoveryEngineConfiguration.getLogLevel(), productDiscoveryEngineConfiguration.getConnectTimeout(),
        productDiscoveryEngineConfiguration.getReadTimeout());
  }

  private OkHttpClient createHttpClient(String logLevel, Integer connectTimeout,
      Integer readTimeout) {
    HttpLoggingInterceptor httpLoggingInterceptor = new HttpLoggingInterceptor(log::debug);

    httpLoggingInterceptor.setLevel(HttpLoggingInterceptor.Level.valueOf(logLevel));
    OkHttpClient.Builder builder = new OkHttpClient.Builder().addInterceptor(httpLoggingInterceptor)
        .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
        .readTimeout(readTimeout, TimeUnit.MILLISECONDS)
        .writeTimeout(readTimeout, TimeUnit.MILLISECONDS);
    builder.addInterceptor(chain -> chain.proceed(chain.request().newBuilder().build()));

    return builder.build();
  }

  private Retrofit.Builder constructRetrofitBuilder(OkHttpClient okHttpClient) {
    Retrofit.Builder builder = new Retrofit.Builder();
    builder.client(okHttpClient);
    return builder;
  }


  private ObjectMapper constructObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
    return objectMapper;
  }
}