package com.engati.data.analytics;

import com.engati.data.analytics.engine.constants.constant.Constants;
import net.minidev.json.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Created by akanksha
 **/

@SpringBootApplication
@ComponentScan(basePackages = {"com.engati.data.analytics"})
public class DataAnalyticsApplication {

  public static void main(String[] args) {
      SpringApplication.run(DataAnalyticsApplication.class, args);
  }

}
