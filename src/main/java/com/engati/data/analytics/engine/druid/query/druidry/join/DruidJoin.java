package com.engati.data.analytics.engine.druid.query.druidry.join;

import com.engati.data.analytics.engine.druid.query.druidry.datasource.DruidDataSource;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class DruidJoin {
  private static final String JOIN = "join";
  private String type;
  private DruidDataSource left;
  private DruidDataSource right;
  private String rightPrefix;
  private String condition;
  private String joinType;

  @Builder
  private DruidJoin(@NonNull DruidDataSource left, @NonNull DruidDataSource right,
      @NonNull String rightPrefix, @NonNull String condition,
      @NonNull String joinType) {
    this.type = JOIN;
    this.left = left;
    this.right = right;
    this.rightPrefix = rightPrefix;
    this.condition = condition;
    this.joinType = joinType;
  }
}
