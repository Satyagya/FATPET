package com.engati.data.analytics.engine.common;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.nethum.errorhandling.exception.error.AppCode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

@Getter
@ToString
@AllArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
@JsonDeserialize(using = DataAnalyticsEngineStatusCode.StatusCodeEnumDeserializer.class)
public enum DataAnalyticsEngineStatusCode implements AppCode<DataAnalyticsEngineStatusCode> {

  SUCCESS(1000, "SUCCESS"),
  PROCESSING_ERROR(999, "PROCESSING_ERROR");

  private final int code;
  private final String desc;

  @Override
  public DataAnalyticsEngineStatusCode valueOf(int statusCode) {
    for (DataAnalyticsEngineStatusCode status: values()) {
      if(status.code == statusCode) {
        return status;
      }
    }
    throw new IllegalArgumentException("No matching constant for [" + statusCode + "]");
  }

  public static class StatusCodeEnumDeserializer extends
      StdDeserializer<DataAnalyticsEngineStatusCode> {
    public StatusCodeEnumDeserializer() {
      super(DataAnalyticsEngineStatusCode.class);
    }

    @Override
    public DataAnalyticsEngineStatusCode deserialize(JsonParser jp, DeserializationContext context)
        throws IOException {
      final JsonNode jsonNode = jp.readValueAsTree();
      int code = jsonNode.get("code").asInt();
      String desc = jsonNode.get("desc").asText();

      for (DataAnalyticsEngineStatusCode statusCode : DataAnalyticsEngineStatusCode.values()) {
        if (statusCode.getCode() == code && statusCode.getDesc().equals(desc)) {
          return statusCode;
        }
      }
      throw new DataAnalyticsServiceException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
  }
}
