package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.constants.constant.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.apachecommons.CommonsLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.*;

@Slf4j
@Component
public class CommonUtils {

  public static Connection connection;
  public static final ObjectMapper MAPPER = new ObjectMapper();

  @PostConstruct
  public void getDuckDBConnection() {
    {
      try {
        log.info("Going to create a DuckDB connection");
        Class.forName("org.duckdb.DuckDBDriver");
        connection = DriverManager.getConnection(Constants.DUCKDB_CONNECTION_URI);
      } catch (Exception e) {
        log.error("Failed to connect to DuckDB", e);
      }
    }
  }


  public Set<Long> executeQuery(String query) {
    Statement statement = null;
    Set<Long> querySet = new LinkedHashSet<>();
    try {
      statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(query);
      ResultSetMetaData md = resultSet.getMetaData();
      int columns = md.getColumnCount();
      while (resultSet.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), resultSet.getObject(i));
        }
        querySet.add((Long) row.get(Constants.CUSTOMER_ID));
      }
    } catch (SQLException e) {
      log.error("Exception encountered while executing Query: {}", query, e);
    }
    return querySet;
  }


  public Map<Long, Map<String, Object>> executeQueryForDetails(String query, String key) {
    Statement statement = null;
    Map<Long, Map<String, Object>> querySet = new LinkedHashMap<>();
    try {
      statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(query);
      ResultSetMetaData md = resultSet.getMetaData();
      int columns = md.getColumnCount();

      while (resultSet.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns;   ++i) {
          row.put(md.getColumnName(i), resultSet.getObject(i));
        }
        querySet.put((Long) row.get(key), row);
      }
    } catch (SQLException e) {
      log.error("Exception encountered while executing Query for fetching details: {}", query, e);
    }
    return querySet;
  }

  public static String getStringValueFromJsonPath(Object object, String jsonPath) {
    String value = null;
    try {
      String jsonString = MAPPER.writeValueAsString(object);
      value = JsonPath.read(jsonString, jsonPath).toString();
    } catch (Exception e) {
      log.debug("Error while getting value from the jsonPath:{} with exception:{}", jsonPath,
              e.getMessage());
    }
    return value;
  }

}
