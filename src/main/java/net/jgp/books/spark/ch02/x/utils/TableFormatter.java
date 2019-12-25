/*
 * iNamik TableFormatter
 * Copyright (C) 2005 David Farrell (davidpfarrell@yahoo.com)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package net.jgp.books.spark.ch02.x.utils;

/**
 * TableFormatter Created on Oct 18, 2005
 * 
 * @author Dave
 */
public interface TableFormatter {
  static final int ALIGN_DEFAULT = 0;
  static final int ALIGN_LEFT = 1;
  static final int ALIGN_CENTER = 2;
  static final int ALIGN_RIGHT = 3;

  static final int VALIGN_DEFAULT = 4;
  static final int VALIGN_TOP = 5;
  static final int VALIGN_CENTER = 6;
  static final int VALIGN_BOTTOM = 7;

  TableFormatter nextRow();

  TableFormatter nextCell();

  TableFormatter nextCell(int align, int valign);

  TableFormatter addLine();

  TableFormatter addLine(String text);

  int getColumnCount();

  int getRowCount();

  int getColumnWidth(int columnIndex);

  int getRowHeight(int rowIndex);

  int getTableWidth();

  int getTableHeight();

  String[] getFormattedCell(int rowIndex, int columnIndex);

  String[] getFormattedRow(int rowIndex);

  String[] getFormattedTable();
}
