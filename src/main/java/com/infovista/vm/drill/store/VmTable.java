package com.infovista.vm.drill.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import com.infovista.vistamart.datamodel.ws.v8.DataModelException;
import com.infovista.vistamart.datamodel.ws.v8.DataModelV80;
import com.infovista.vistamart.datamodel.ws.v8.Indicator;
import com.infovista.vistamart.datamodel.ws.v8.IndicatorCriteria;
import com.infovista.vistamart.datamodel.ws.v8.InstanceCriteria;
import com.infovista.vistamart.datamodel.ws.v8.Property;
import com.infovista.vistamart.datamodel.ws.v8.PropertyCriteria;
import com.infovista.vistamart.datamodel.ws.v8.PropertyList;
import com.infovista.vistamart.datamodel.ws.v8.PropertyType;
import com.infovista.vistamart.datamodel.ws.v8.VistaCriteria;
import com.infovista.vm.drill.store.TypeManager.TypeLong;
import com.infovista.vm.drill.store.TypeManager.TypeTimestamp;
import com.infovista.vm.drill.store.TypeManager.TypeVarchar;

public class VmTable extends DynamicDrillTable {
	static final String DATA_NAME_SUFFIX = "_data";
	static final String TIMESTAMP_COLUMN_NAME = "dateTime";
	static final String DR_COLUMN_NAME = "timePeriod";
	static final String PROXY_OF_COLUMN_NAME = "proxyOf";
	static final String NAME_COLUMN_NAME = "name";
	static final String TAG_COLUMN_NAME = "tag";
	static final String ID_COLUMN_NAME = "id";
	VmStoragePlugin myplugin ;
	private String tableName;

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VmTable.class);

	public VmTable(VmStoragePlugin plugin, String schemaName, VmScanSpec scanSpec) {
		super(plugin, schemaName,scanSpec);
		myplugin = plugin;
		this.tableName = scanSpec.getTableName();
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) 
	{
		List<String> names = new ArrayList<>();
		List<RelDataType> types = new ArrayList<>();
		Map<String,PropertyDesc> cachedData = new LinkedHashMap<String, PropertyDesc>();
		TreeSet<String> listNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		PropertyCriteria pc = null;
		VistaCriteria vc = null;
		DataModelV80 service = myplugin.getService();
		// Display Rate
		names.add(DR_COLUMN_NAME);
		TypeManager tm = new TypeVarchar();
		RelDataType type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,false);
		types.add(type);
		listNames.add(DR_COLUMN_NAME);
		
		// instance name
		names.add(VmTable.NAME_COLUMN_NAME);
		tm = new TypeVarchar();
		type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,true);
		types.add(type);
		listNames.add(NAME_COLUMN_NAME);
		
		// instance ID
		names.add(VmTable.ID_COLUMN_NAME);
		tm = new TypeLong();
		type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,false);
		types.add(type);
		listNames.add(ID_COLUMN_NAME);
		
		// instance tag
		names.add(VmTable.TAG_COLUMN_NAME);
		tm = new TypeVarchar();
		type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,false);
		types.add(type);
		listNames.add(TAG_COLUMN_NAME);
		
		// timesTamp
		names.add(TIMESTAMP_COLUMN_NAME);
		tm = new TypeTimestamp(); 
		type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,false);
		types.add(type);
		listNames.add(TIMESTAMP_COLUMN_NAME);
		
		//ProxyOf
		names.add(VmTable.PROXY_OF_COLUMN_NAME);
		tm = new TypeVarchar();
		type =  tm.getRelDataType(typeFactory);
		type = typeFactory.createTypeWithNullability(type,true);
		types.add(type);
		listNames.add(PROXY_OF_COLUMN_NAME);
		
		//fetch properties
		pc = new PropertyCriteria(); 
		vc = new VistaCriteria();
		vc.setName(tableName);
		vc.setAncestors(true);
		pc.setVistaIn(vc);
		try {
			PropertyList l = service.getProperties(pc);
			for(Property prop : l.getData()) {

				String colName = prop.getName().trim();
				if(listNames.contains(colName)) {
					// name collision , add wid to the name
					colName = colName+"_"+prop.getWID();
				}
				names.add(colName);
				if(prop.isMultivalued()) {
					type =  new TypeManager.TypeVarchar().getRelDataType(typeFactory);
				}else {
					type = getSqlTypeFromDataModelType(typeFactory, prop.getType());
				}
				type = typeFactory.createTypeWithNullability(type, true);
				types.add(type);
				listNames.add(colName);
				cachedData.put(colName, new PropertyDesc(prop.getWID(),prop.getType(), prop.isMultivalued()));
			}
			myplugin.propertiesInTables.put(tableName, cachedData);

		} catch (Exception e) {
			throw UserException.connectionError(e)
			.message("Error while getting row types")
			.build(logger);
		}
		
		// fetch list of indicators
		vc = new VistaCriteria();
		vc.setAncestors(true);
		vc.setName(tableName);
		IndicatorCriteria indic = new IndicatorCriteria();
		indic.setVistaIn(vc);
		indic.getUsedInstances().add(new InstanceCriteria());
		try {
			List<Indicator> listIndic = myplugin.getService().getIndicators(indic).getData();

			VmTableDef indicatorsDef;
			indicatorsDef = new VmTableDef();
			for(Indicator indicator : listIndic) {
				String colName = indicator.getLabel();
				if(colName == null)
					colName = indicator.getName();
				colName = colName.replace("°", "").trim();
				
				if(listNames.contains(colName)) {
					// name collision , add wid to the name
					colName = colName+"_"+indicator.getWID();
				}
				listNames.add(colName);
				indicatorsDef.getIndicators().put(colName, new IndicatorDesc(indicator.getType(),indicator.getWID()));
				names.add(colName);

				RelDataType typein = TypeManager.getTypeManager(indicator.getType()).getRelDataType(typeFactory);
				typein = typeFactory.createTypeWithNullability(typein, true);
				types.add(typein);
			}
			myplugin.indicatorsByDataTable.put(tableName, indicatorsDef);
		} catch (DataModelException e) {
			throw UserException.connectionError(e)
			.message("Error while getting row types for indicators")
			.build(logger);
		}		
		return typeFactory.createStructType(types, names);
	}

	private RelDataType getSqlTypeFromDataModelType(RelDataTypeFactory typeFactory, PropertyType  type) {
		return TypeManager.getTypeManager(type).getRelDataType(typeFactory);
	}

}
