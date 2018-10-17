package com.infovista.vm.drill.store;

import com.infovista.vistamart.datamodel.ws.v8.IndicatorType;

public class IndicatorDesc {
	IndicatorType type;
	long id;
	public IndicatorDesc(IndicatorType type, long id) {
		this.type = type;
		this.id = id;
	}
	
	public long getId() {
		return id;
	}

}
