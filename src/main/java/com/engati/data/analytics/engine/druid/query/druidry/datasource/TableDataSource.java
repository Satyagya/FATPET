package com.engati.data.analytics.engine.druid.query.druidry.datasource;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class TableDataSource extends DruidDataSource {
  private static final String TABLE_DATA_SOURCE = "table";
  private String name;

  public TableDataSource(@NonNull String name) {
    this.type = TABLE_DATA_SOURCE;
    this.name = name;
  }
}
