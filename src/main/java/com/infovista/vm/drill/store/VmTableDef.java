package com.infovista.vm.drill.store;

import java.util.Map;
import java.util.TreeMap;

public class VmTableDef {
	
	public Map<String,IndicatorDesc> indicators;
	
		public VmTableDef() {
		indicators = new TreeMap<String, IndicatorDesc>(String.CASE_INSENSITIVE_ORDER);
		
	}

	public Map<String, IndicatorDesc> getIndicators() {
		return indicators;
	}

}
