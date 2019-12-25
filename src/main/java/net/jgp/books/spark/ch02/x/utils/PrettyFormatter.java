package net.jgp.books.spark.ch02.x.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pretty formatter for a JDBC resultset
 * 
 * @author jgp
 */
public class PrettyFormatter {
  private static Logger log = LoggerFactory.getLogger(PrettyFormatter.class);

  private ResultSet resultSet;
  private boolean updated = false;
  private StringBuilder sb;

  public PrettyFormatter() {
    this.sb = new StringBuilder();
  }

  public PrettyFormatter set(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.updated = true;
    return this;
  }

  public void show() {
    if (updated) {
      if (!updateBuffer()) {
        return;
      }
    }
    System.out.println(sb.toString());
  }

  private boolean updateBuffer() {
    ResultSetMetaData rsmd;
    try {
      rsmd = resultSet.getMetaData();
    } catch (SQLException e) {
      log.error(
          "Could not extract metadata from result set: {}",
          e.getMessage(), e);
      return false;
    }

    int columnCount;
    try {
      columnCount = rsmd.getColumnCount();
    } catch (SQLException e) {
      log.error(
          "An exception was raised while trying to get the count of columns of the resultset: {}",
          e.getMessage(), e);
      return false;
    }

    List<PrettyFormatterColumn> columns = new ArrayList<>();
    PrettyFormatterColumn pfc;
    try {
      for (int i = 1; i <= columnCount; i++) {
        pfc = new PrettyFormatterColumn();
        pfc.setHeading(rsmd.getColumnName(i));
        pfc.setType(rsmd.getColumnType(i));
        pfc.setTypeName(rsmd.getColumnTypeName(i));
        pfc.setColumnWidth(rsmd.getColumnDisplaySize(i));
        columns.add(pfc);
      }
    } catch (SQLException e) {
      log.error(
          "An exception was raised while accessing the metadata of the resultset: {}",
          e.getMessage(), e);
      return false;
    }

    // Formatting
    String columnHeading = "";
    String rowSeparator = "";
    for (int i = 0; i < columnCount; i++) {
      pfc = columns.get(i);
      columnHeading = columnHeading + "|" + pfc          .getColumnName();
      rowSeparator = rowSeparator + "+" + pfc.getDashes();
    }

    columnHeading += "|";
    rowSeparator += "+";

    sb.append(rowSeparator);
    sb.append('\n');
    sb.append(columnHeading);
    sb.append('\n');
    sb.append(rowSeparator);
    sb.append('\n');

    try {
      while (resultSet.next()) {
        // Retrieve by column name
        sb.append('|');
        for (int i = 0; i < columnCount; i++) {
          pfc = columns.get(i);
          sb.append(pfc.getFormattedValue(resultSet));
          sb.append('|');
        }
        sb.append('\n');
      }
    } catch (SQLException e) {
      log.error(
          "An exception was raised while navigating in the resultset: {}",
          e.getMessage(), e);
    }

    sb.append(rowSeparator);
    sb.append('\n');
    return true;
  }

}
