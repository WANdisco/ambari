/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ambari.view.hive20.internal.dto;

/**
 * this will be returned as a part of TableMeta which table info is called.
 * It includes the part of DetailedTableInfo which contain statistics related data.
 */
public class TableStats {
  public static final String NUM_FILES = "numFiles";
  public static final String NUM_ROWS = "numRows";
  public static final String COLUMN_STATS_ACCURATE = "COLUMN_STATS_ACCURATE";
  public static final String RAW_DATA_SIZE = "rawDataSize";
  public static final String TOTAL_SIZE = "totalSize";

  private Boolean isTableStatsEnabled;
  private Integer numFiles;
  private Integer numRows;
  private String columnStatsAccurate;
  private Integer rawDataSize;
  private Integer totalSize;

  public Boolean getTableStatsEnabled() {
    return isTableStatsEnabled;
  }

  public void setTableStatsEnabled(Boolean tableStatsEnabled) {
    isTableStatsEnabled = tableStatsEnabled;
  }

  public Integer getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(Integer numFiles) {
    this.numFiles = numFiles;
  }

  public String getColumnStatsAccurate() {
    return columnStatsAccurate;
  }

  public void setColumnStatsAccurate(String columnStatsAccurate) {
    this.columnStatsAccurate = columnStatsAccurate;
  }

  public Integer getRawDataSize() {
    return rawDataSize;
  }

  public void setRawDataSize(Integer rawDataSize) {
    this.rawDataSize = rawDataSize;
  }

  public Integer getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(Integer totalSize) {
    this.totalSize = totalSize;
  }

  public Integer getNumRows() {
    return numRows;
  }

  public void setNumRows(Integer numRows) {
    this.numRows = numRows;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TableStats{");
    sb.append("isStatsEnabled='").append(isTableStatsEnabled).append('\'');
    sb.append(", numFiles='").append(numFiles).append('\'');
    sb.append(", numRows='").append(numRows).append('\'');
    sb.append(", columnStatsAccurate='").append(columnStatsAccurate).append('\'');
    sb.append(", rawDataSize='").append(rawDataSize).append('\'');
    sb.append(", totalSize='").append(totalSize).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
