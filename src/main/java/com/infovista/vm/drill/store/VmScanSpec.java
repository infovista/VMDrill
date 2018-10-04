package com.infovista.vm.drill.store;


import com.fasterxml.jackson.annotation.JsonIgnore;

public class VmScanSpec {
	protected long timestampStart =-1;
	protected long timestampEnd = -1;
	private String tableName;
	private String displayRate = "";


	public Filter filter;
	
	public VmScanSpec(String tableName,long timestamp_start,long timestamp_end,String dr, Filter filter) {
		
		this.tableName = tableName;
		this.timestampStart = timestamp_start;
		this.timestampEnd = timestamp_end;
		this.filter = filter;
		this.displayRate = dr;
		
	}

	public long getTimestampStart() {
		return timestampStart;
	}

	public void setTimestampStart(long timestamp_start) {
		this.timestampStart = timestamp_start;
	}

	public long getTimestampEnd() {
		return timestampEnd;
	}

	public void setTimestampEnd(long timestamp_end) {
		this.timestampEnd = timestamp_end;
	}

	public String getDisplayRate() {
		return displayRate;
	}

	public void setDisplayRate(String displayRateValue) {
		this.displayRate = displayRateValue;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public VmScanSpec(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}
	
	 @Override
	  public String toString() {
		 return "VmScanSpec [tableName=" + tableName
				 + ", timestamp_start = "+ timestampStart
				 + ", timestamp_end = " + timestampEnd
				 + ", displayRate = " +displayRate;
	 }
	 
	 @JsonIgnore
	 public Filter getFilter() {
		 return filter;
	 }

}
