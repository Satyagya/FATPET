package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.constants.constant.Constants;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

@Slf4j
public class CommonUtils {

  public static Connection getDuckDBConnection() {
    Connection connection = null;
    {
      try {
        connection = DriverManager.getConnection(Constants.DUCKDB_CONNECTION_URI);
      } catch (SQLException e) {
        log.error("Failed to connect to DuckDB", e);
      }
    }
    return connection;
  }


  public static Set<Long> executeQuery(String query) {
    Statement statement = null;
    Set<Long> querySet = new LinkedHashSet<>();
    try {
      Connection conn = CommonUtils.getDuckDBConnection();
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(query);
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();
      while (rs.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), rs.getObject(i));
        }
        querySet.add((Long) row.get(Constants.CUSTOMER_ID));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return querySet;
  }


  //  public List<Map<String, Object>> executeQueryForDetails(String query) {
  public static Map<Long, Map<String, Object>> executeQueryForDetails(String query, String key) {
    Statement statement = null;
    Map<Long, Map<String, Object>> querySet = new LinkedHashMap<>();
//    List<Map<String, Object>> test = new ArrayList<>();
    try {
      Connection conn = CommonUtils.getDuckDBConnection();
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(query);
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();

      while (rs.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), rs.getObject(i));
        }
//        test.add(row);
        querySet.put((Long) row.get(key), row);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return querySet;
    // return test;
  }


}
