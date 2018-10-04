package com.infovista.vm.drill.store;


import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.AbstractSchema;

import com.infovista.vistamart.datamodel.ws.v8.DataModelException;
import com.infovista.vistamart.datamodel.ws.v8.Indicator;
import com.infovista.vistamart.datamodel.ws.v8.IndicatorCriteria;
import com.infovista.vistamart.datamodel.ws.v8.InstanceCriteria;
import com.infovista.vistamart.datamodel.ws.v8.InstanceState;
import com.infovista.vistamart.datamodel.ws.v8.Vista;
import com.infovista.vistamart.datamodel.ws.v8.VistaCriteria;
import com.infovista.vistamart.datamodel.ws.v8.VistaList;

public class VmSchema extends AbstractSchema {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VmSchema.class);
	private VmStoragePlugin plugin;
	
	public VmSchema(VmStoragePlugin plugin, String name) {
		super(Collections.emptyList(),name);
		this.plugin = plugin;
	}

	@Override
	public String getTypeName() {
		return VmStorageConfig.NAME;
	}
	
	public Set<String> getTableNames() 
	{
		HashSet<String> tables = new HashSet<>();

		VistaCriteria vs1 = new VistaCriteria();
		vs1.setNameNotLike("VistaMart%");
		InstanceCriteria insc1 = new InstanceCriteria();
		insc1.setState(InstanceState.ACTIVE);
		insc1.setNameNotLike("VistaMart%");
		vs1.getTopInstances().add(insc1);
		try {
			VistaList list = plugin.getService().getVistas(vs1);
			for(Vista vista2 : list.getData()) {

				tables.add(vista2.getName());
				//check if indicators exist for this vista
				VistaCriteria vc = new VistaCriteria();
				vc.setAncestors(true);
				vc.setID(vista2.getID());
				IndicatorCriteria indic = new IndicatorCriteria();
				indic.setVistaIn(vc);
				indic.getUsedInstances().add(new InstanceCriteria());
				List<Indicator> listIndic = plugin.getService().getIndicators(indic).getData();
				if(!listIndic.isEmpty())
					tables.add(vista2.getName()+VmTable.DATA_NAME_SUFFIX);
			}

		} catch (DataModelException e) {
			throw UserException.connectionError(e)
			.message("DataModel error while accessing metadata")
			.build(logger);
		}
		return tables;

	}

	public Table getTable(String name) {
		VmScanSpec scanSpec = new VmScanSpec(name);
		VmTable table = new VmTable(plugin, getName(), scanSpec);
		if(name.endsWith(VmTable.DATA_NAME_SUFFIX))
			table.SetIsData(true);
		return table;
	}

}
