package com.infovista.vm.drill.store;


import com.infovista.vistamart.datamodel.ws.v8.PropertyType;

public class PropertyDesc {

	long id;
	PropertyType type;
	boolean isMultValued ;
	public PropertyDesc(long id, PropertyType type, boolean isMultiValued) {
		this.id = id;
		this.type = type;
		this.isMultValued = isMultiValued;
	}


}
