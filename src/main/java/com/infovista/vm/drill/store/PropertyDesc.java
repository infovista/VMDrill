package com.infovista.vm.drill.store;


import com.infovista.vistamart.datamodel.ws.v8.PropertyType;

public class PropertyDesc {

	String wid;
	PropertyType type;
	boolean isMultValued ;
	public PropertyDesc(String wid, PropertyType type, boolean isMultiValued) {
		this.wid = wid;
		this.type = type;
		this.isMultValued = isMultiValued;
	}


}
