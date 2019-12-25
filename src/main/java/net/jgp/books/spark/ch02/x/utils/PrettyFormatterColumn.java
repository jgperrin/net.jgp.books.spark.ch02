package net.jgp.books.spark.ch02.x.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrettyFormatterColumn {
  private static  Logger log = LoggerFactory.getLogger(PrettyFormatterColumn.class);

  private int maxColumnWidth = 20;
  private String columnName;
  private boolean isString;
  private int columnWidth = 0;

  public void setHeading(String columnName) {
    this.columnName = columnName;
    setColumnWidth(columnName.length());
  }

  public void setColumnWidth(int width) {
    if (columnWidth > width) {
      return;
    }
    if (width > this.maxColumnWidth) {
      this.columnWidth = this.maxColumnWidth;
    } else {
      this.columnWidth = width;
    }
  }

  public void setType(int columnType) {
    switch (columnType) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NCLOB:
      case Types.NVARCHAR:
      case Types.SQLXML:
        this.isString = true;
        break;

      default:
        this.isString = false;
        break;
    }
  }

  public void setTypeName(String columnTypeName) {
    setColumnWidth(columnTypeName.length());
  }

  public String getColumnName() {
    String res = String.format("%-" + columnWidth + "s", this.columnName);

    if (res.length() > columnWidth) {
      return res.substring(0, columnWidth);
    } else {
      return res;
    }
  }

  public String getDashes() {
    return dashes(columnWidth);
  }

  private String dashes(int l) {
    String dashes = "";
    for (int i = 0; i < l; i++) {
      dashes += '-';
    }
    return dashes;
  }

  public String getFormattedValue(ResultSet resultSet) {
    String res;
    try {
      res = resultSet.getString(columnName);
    } catch (SQLException e) {
      log.error(
          "Could not get a value for column [{}] from the current resultset: {}, assuming null",
          columnName, e.getMessage(), e);
      res = "null";
    }

    String format = "%";
    if (isString) {
      format += '-';
    }
    format += columnWidth + "s";
    res = String.format(format, res);
    if (res.length() > columnWidth) {
      return res.substring(0, columnWidth);
    } else {
      return res;
    }
  }
}
