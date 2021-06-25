package com.rayful.bulk.index;

public class TimestampColumn {
	public static final int TIMESTMP_DATETIME = 1;
	public static final int TIMESTMP_STRING = 2;
	
	
	private String ms_columnName;
	private int mi_columnType;
	
	public TimestampColumn ( String sColumnName, int iType) {
		ms_columnName = sColumnName;
		mi_columnType = iType;
	}
	
	public String getColumnName () {
		return ms_columnName;
	}
	
	public int getColumnType () {
		return mi_columnType;
	}
}
