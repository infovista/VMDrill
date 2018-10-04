package com.infovista.vm.drill.store;

import java.util.LinkedHashMap;
import java.util.Map;

public class VmTableDef {
	
	public Map<String,IndicatorDesc> indicators;
	
		public VmTableDef() {
		indicators = new LinkedHashMap<String, IndicatorDesc>();
		
	}

	public Map<String, IndicatorDesc> getIndicators() {
		return indicators;
	}

}
