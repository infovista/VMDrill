package com.infovista.vm.drill.store;

import com.infovista.vistamart.datamodel.ws.v8.IndicatorType;

public class IndicatorDesc {
	IndicatorType type;
	String wid;
	public IndicatorDesc(IndicatorType type, String wid) {
		this.type = type;
		this.wid = wid;
	}
	
	public String getWid() {
		return wid;
	}

}
