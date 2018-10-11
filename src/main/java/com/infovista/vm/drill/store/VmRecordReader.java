package com.infovista.vm.drill.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;


import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.infovista.vistamart.datamodel.ws.v8.DataModelException;
import com.infovista.vistamart.datamodel.ws.v8.DataModelV80;
import com.infovista.vistamart.datamodel.ws.v8.DataValueFilter;
import com.infovista.vistamart.datamodel.ws.v8.DataValueType;
import com.infovista.vistamart.datamodel.ws.v8.DisplayRate;
import com.infovista.vistamart.datamodel.ws.v8.IndicatorCriteria;
import com.infovista.vistamart.datamodel.ws.v8.InstanceCriteria;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataCriteria;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataInputColumn;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataInputColumnType;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataOutputColumn;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataOutputColumnType;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataResponse;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataResponseColumn;
import com.infovista.vistamart.datamodel.ws.v8.PropValueCriteria;
import com.infovista.vistamart.datamodel.ws.v8.PropertyCriteria;
import com.infovista.vistamart.datamodel.ws.v8.ValueComparator;
import com.infovista.vistamart.datamodel.ws.v8.ValueComparatorArgument;
import com.infovista.vistamart.datamodel.ws.v8.ValueComparatorType;
import com.infovista.vistamart.datamodel.ws.v8.VistaCriteria;
import com.infovista.vm.drill.store.Filter.FilterDesc;

public class VmRecordReader extends AbstractRecordReader {
	private final static Logger logger = LoggerFactory.getLogger(VmRecordReader.class);
	protected VmGroupScan groupScan;
	protected MatrixDataResponse md = null;
	protected int totalCount = 0;
	protected boolean allreads;
	protected List<TypeManager> types = null;
	MatrixDataCriteria mdc  = null;
	DataModelV80 service = null;
	int pageSize = 1000;
	protected String tableName;
	protected VmStoragePlugin plugin;
	protected ValueVector vectorForDR = null;
	protected List<String> colNames;
	public  VmRecordReader(VmGroupScan scan) {
		this.groupScan = scan;
		List<SchemaPath> projectedColumns = scan.getColumns();
		if(projectedColumns == null) {
			projectedColumns = GroupScan.ALL_COLUMNS;
		}
		setColumns(projectedColumns);
		tableName = groupScan.getTableName();
		plugin = groupScan.getPlugin();
		pageSize = ((VmStorageConfig)plugin.getConfig()).getPageSize();
	}
	
	protected void init() {
		allreads = false;
		totalCount = 0;
		types = new ArrayList<>();
		service = plugin.getService();
		colNames = Lists.newArrayList();
		if (!isStarQuery()) {
			for (SchemaPath p : this.getColumns()) {
				colNames.add(p.getRootSegmentPath());
			}
		}
	}
	@Override
	public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
		init();
		Filter filter = null;
		VmScanSpec scanSpec = groupScan.getVmScanSpec();
		if(scanSpec!=null) {
			filter = scanSpec.getFilter();
		}
		Map<String,PropertyDesc> propertiesDef = plugin.propertiesInTables.get(tableName);
		VmTableDef tabledef = plugin.indicatorsByDataTable.get(tableName);
		try {
			mdc = new MatrixDataCriteria();
			Date now = new Date();
			mdc.setRangeStart(0L);
			mdc.setRangeEnd((long)pageSize);
			
			if(groupScan.getVmScanSpec().getTimestampStart()!= -1)
				mdc.setTimeRangeStart(newCalendarAt(groupScan.getVmScanSpec().getTimestampStart()));
			else
				mdc.setTimeRangeStart(newCalendarAt(now));
			if(groupScan.getVmScanSpec().getTimestampEnd() != -1)
				mdc.setTimeRangeEnd(newCalendarAt(groupScan.getVmScanSpec().getTimestampEnd()));
			else
				mdc.setTimeRangeEnd(newCalendarAt(now));
			
			String displayRate = scanSpec.getDisplayRate();
			DisplayRate dr = VmStoragePlugin.drMapName.get(displayRate);
			if(dr != null)
				mdc.setDisplayRate(dr);
			else
			{
				throw new ExecutionSetupException("list of supported values for column  "+VmTable.DR_COLUMN_NAME+" is "+VmStoragePlugin.listOfDispalyRates);
			}
			InstanceCriteria ic = new InstanceCriteria();
//			ic.setState(InstanceState.ACTIVE);
			VistaCriteria vc = new VistaCriteria();
			vc.setName(tableName);
			ic.getVistas().add(vc);
			mdc.getInstancesIn().add(ic);

			MatrixDataInputColumn mdic;
			MatrixDataOutputColumn outCol;
			//	if(isStarQuery()) {

			// conditions on attributes
			if(filter!= null) {
				List<FilterDesc> filterDescList = filter.filters.get(VmTable.TAG_COLUMN_NAME);
				if(filterDescList != null) {
					for(FilterDesc desc : filterDescList){
						if(desc.compareOp.equals("equal") ) {
							ic.setTag((String)desc.value);
						}
						else if(desc.compareOp.equals("like")) {
							ic.setTagLike((String)desc.value);
						}else if(desc.compareOp.equals("in")) {
							@SuppressWarnings("unchecked")
							List<String> listTag = ((ArrayList<Object>)desc.value).stream().map (i -> i.toString()).collect(Collectors.toList());
							ic.getTagIn().addAll(listTag);
						}
					}
				}
				filterDescList = filter.filters.get(VmTable.NAME_COLUMN_NAME);
				if(filterDescList != null) {
					for(FilterDesc desc : filterDescList){
						if(desc.compareOp.equals("equal") ) {
							ic.setName((String)desc.value);
						}
						else if(desc.compareOp.equals("like")) {
							ic.setNameLike((String)desc.value);
						}
						else if(desc.compareOp.equals("in")) {
							@SuppressWarnings("unchecked")
							List<String> listNames = ((ArrayList<Object>)desc.value).stream().map (i -> i.toString()).collect(Collectors.toList());
							ic.getNameIn().addAll(listNames);
						}
					}
				}
				filterDescList = filter.filters.get(VmTable.ID_COLUMN_NAME);
				if(filterDescList != null) {
					for(FilterDesc desc : filterDescList){
						if(desc.compareOp.equals("equal") ) {
							Long value = null;
							if(desc.value instanceof String) {
								String valueString = (String)desc.value;
								if(valueString.startsWith("'"))
									valueString = valueString.substring(1,valueString.length()-1);
								value = Long.parseLong(valueString);
							}
							if(desc.value instanceof Integer)
								value = Long.valueOf(((Integer)desc.value).longValue());
							else if(desc.value instanceof Long)
								value = (Long)desc.value;
							if(value != null)
								ic.setID(value);
							break;
						}
						else if(desc.compareOp.equals("in")) {
							@SuppressWarnings("unchecked")
							List<Long> listId = ((ArrayList<Object>)desc.value).stream().map (i -> (i instanceof Long )? (Long)i:Long.valueOf(((Integer)i).longValue()) ).collect(Collectors.toList());
							ic.getIDIn().addAll(listId);
						}
					}
				}
				filterDescList = filter.filters.get(VmTable.PROXY_OF_COLUMN_NAME);
				if(filterDescList != null) {
					for(FilterDesc desc : filterDescList){
						if(desc.compareOp.equals("equal") ) {
							InstanceCriteria parentCriteria = new InstanceCriteria();
							parentCriteria.setTag((String)desc.value);
							ic.setParentIn(parentCriteria);
							break;
						}
						else if(desc.compareOp.equals("in")) {
							@SuppressWarnings("unchecked")
							List<String> listTags = ((ArrayList<Object>)desc.value).stream().map (i -> i.toString()).collect(Collectors.toList());
							InstanceCriteria parentCriteria = new InstanceCriteria();
							parentCriteria.getTagIn().addAll(listTags);
							ic.setParentIn(parentCriteria);
							break;
						}
					}
				}

			}
			//attributes
			for(Entry<String,MatrixDataInputColumnType> attrentry : VmStoragePlugin.attributesInTable.entrySet()) {
				if(!isStarQuery() && !colNames.contains(attrentry.getKey()))
					continue;
				addMatrixDataColumn(mdc, attrentry.getKey(), attrentry.getValue());
				/*mdic = new MatrixDataInputColumn();
				mdc.getInput().add(mdic);
				mdic.setColumnName(attrentry.getKey());
				mdic.setType(attrentry.getValue());
				outCol = new MatrixDataOutputColumn();
				mdc.getOutput().add(outCol);
				outCol.setType(MatrixDataOutputColumnType.INPUT);
				outCol.setColumnName(attrentry.getKey());*/
			}
			//properties
			for(Entry<String, PropertyDesc> propEntry : propertiesDef.entrySet()) {
				if(!isStarQuery() && !colNames.contains(propEntry.getKey()))
					continue;
				mdic = new MatrixDataInputColumn();
				String columnName = propEntry.getKey();

				mdc.getInput().add(mdic);
				mdic.setColumnName(columnName);

				PropertyDesc desc = propEntry.getValue();
				if(desc.isMultValued)
					mdic.setJoinMultiValuesPropValues(true);
				mdic.setType(MatrixDataInputColumnType.PROP_VALUE_HISTORY);

				PropertyCriteria pc = new PropertyCriteria();

				pc.setWID(propEntry.getValue().wid);
				mdic.setProperty(pc);
				outCol = new MatrixDataOutputColumn();
				mdc.getOutput().add(outCol);

				outCol.setType(MatrixDataOutputColumnType.INPUT);

				outCol.setColumnName(columnName);
				if(filter != null) {
					List<FilterDesc> filterDescList = filter.filters.get(columnName);
					PropValueCriteria pvc;	
					if(filterDescList != null) {
						for(FilterDesc filtDesc : filterDescList){
							ValueComparator valComp = buildValueComparator(filtDesc);
							if(valComp == null)
								continue;
							pvc = new PropValueCriteria();
							pvc.setValueComparator(valComp);
			 				PropertyCriteria pcForFilter = new PropertyCriteria();
			 				pcForFilter.setWID(propEntry.getValue().wid);
							pvc.getPropertiesIn().add(pcForFilter);
							ic.getPropValues().add(pvc);
						}
					}
				}
			}
			// indicators
			buildIndicatorsColumns(colNames, tabledef, filter, ic);
			
			md = service.getMatrixData(mdc);
			fillMutator(output, md, tabledef,propertiesDef);
			/*int index = 0;
			TypeManager tm;
			for(MatrixDataResponseColumn col : md.getColumns()) {
				String columnName = col.getColumnName();
				PropertyDesc desc = propertiesDef.get(columnName);
				if(desc== null) {
					//attribute
					if(columnName.equals(VmTable.ID_COLUMN_NAME)) {
						tm = new TypeManager.TypeLong();
					}else
						tm = new TypeManager.TypeVarchar();
				}else {
					//properties
					if(desc.isMultValued) {
						tm = new TypeManager.TypeVarchar();
					}
					else {
						tm = TypeManager.getTypeManager(desc.type);
					}
				}
				types.add(tm);
				MajorType type = tm.getMajorType();
				MaterializedField field = MaterializedField.create(columnName, type);
				Class<? extends ValueVector> clazz = (Class<? extends ValueVector>)TypeHelper.getValueVectorClass(
						tm.getMinorType(), type.getMode());
				ValueVector vector = output.addField(field, clazz);

				tm.setValueVector(index, vector);
				index++;
			}*/
		}catch(Exception e) {
			throw new ExecutionSetupException("Error on record reader setup", e);
		}



	}
	
	protected void addMatrixDataColumn(MatrixDataCriteria mdc, String columnName, MatrixDataInputColumnType type) {
		MatrixDataInputColumn mdic = new MatrixDataInputColumn();
		mdc.getInput().add(mdic);
		mdic.setColumnName(columnName);
		mdic.setType(type);
		MatrixDataOutputColumn outCol = new MatrixDataOutputColumn();
		mdc.getOutput().add(outCol);
		outCol.setType(MatrixDataOutputColumnType.INPUT);
		outCol.setColumnName(columnName);
	}

	@Override
	public int next() {
		int nbreads = 0;
		if(allreads) {
			return 0;
		}
		byte[] record = groupScan.getVmScanSpec().getDisplayRate().getBytes(Charsets.UTF_8);
		for(; nbreads< md.getRows().size();totalCount++,nbreads++) {
			//add DR for DATA tables
			if(vectorForDR != null) {
				VarCharVector.Mutator mutator = (VarCharVector.Mutator)vectorForDR.getMutator();			
				mutator.setSafe(nbreads, record, 0, record.length);
			}
			for (TypeManager type : types) {
				type.setSafe(md.getRows().get(nbreads), nbreads);
			}
		}
		if(nbreads== pageSize) {
			mdc.setRangeStart((long)totalCount);
			mdc.setRangeEnd(totalCount+ (long)pageSize);

			try {
				md = service.getMatrixData(mdc);
			} catch (DataModelException e) {
				return 0;
			}

		}else {
			allreads = true;
		}
		return nbreads;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	XMLGregorianCalendar newCalendarAt(Date date) throws DatatypeConfigurationException {
		GregorianCalendar gCalendar = new GregorianCalendar();
		gCalendar.setTimeInMillis(date.getTime());
		return  DatatypeFactory.newInstance().newXMLGregorianCalendar(gCalendar);

	}
	XMLGregorianCalendar newCalendarAt(long mls) throws DatatypeConfigurationException {
		GregorianCalendar gCalendar = new GregorianCalendar();
		gCalendar.setTimeInMillis(mls);
		return  DatatypeFactory.newInstance().newXMLGregorianCalendar(gCalendar);

	}
	
	protected ValueComparator buildValueComparator(FilterDesc  desc) {
		ValueComparator vcomp = new ValueComparator();
		ValueComparatorArgument vca = new ValueComparatorArgument();
		try {
		if(desc.value instanceof Number) {
			vca.setValue(((Number)desc.value).doubleValue());
		}		
		else if(desc.value instanceof String){
			vca.setSvalue((String)desc.value);
		}else if(desc.value != null)
			return null;
		
		if(desc.compareOp.equals("equal") ) {				
			vcomp.setType(ValueComparatorType.EQUAL);
			vcomp.getArguments().add(vca);
		}
		else
		if(desc.compareOp.equals("not_equal")) {
			vcomp.setType(ValueComparatorType.NOT_EQUAL);
			vcomp.getArguments().add(vca);
		}else if(desc.compareOp.equals("greater_than_or_equal_to")) {
			vcomp.setType(ValueComparatorType.GREATER_OR_EQUAL);
			vcomp.getArguments().add(vca);
		}else if(desc.compareOp.equals("greater_than")) {
			vcomp.setType(ValueComparatorType.GREATER);
			vcomp.getArguments().add(vca);
		}else if(desc.compareOp.equals("less_than_or_equal_to")) {
			vcomp.setType(ValueComparatorType.LESS_OR_EQUAL);
			vcomp.getArguments().add(vca);
		}else if(desc.compareOp.equals("less_than")) {
			vcomp.setType(ValueComparatorType.LESS);
			vcomp.getArguments().add(vca);
		}else if(desc.compareOp.equals("isnotnull")) {
			vcomp.setType(ValueComparatorType.NOT_NULL);
		}else if(desc.compareOp.equals("isnull")) {
			vcomp.setType(ValueComparatorType.NULL);
		}
		else
			return null;
		}catch(Exception e) {
			logger.error("error while building ValueComparator operation "+desc.compareOp +" value "+desc.value, e);
			return null;
		}
		return vcomp;
	}
	
	private void buildIndicatorsColumns(List<String> colNames, VmTableDef tabledef, Filter filter, InstanceCriteria ic) {
		DataValueFilter dvf;
		IndicatorCriteria indic = null;
		for(Entry<String, IndicatorDesc> indEntry : tabledef.getIndicators().entrySet()) {
			if(isStarQuery() || colNames.contains(indEntry.getKey()))
			{				
				String passedName = indEntry.getKey();
				addMatrixDataColumnForIndicator(mdc, indEntry.getKey(), indEntry.getValue().getWid(), MatrixDataInputColumnType.DATA_VALUE);					
		
				//filter on values
				if(filter!= null && mdc.getTimeRangeEnd().equals(mdc.getTimeRangeStart())) {
					List<FilterDesc> filterDescList = filter.filters.get(passedName);
					if(filterDescList != null) {
						for(FilterDesc desc : filterDescList){
							ValueComparator vcomp = buildValueComparator(desc);
							if(vcomp == null)
								continue;							
							dvf = new DataValueFilter();
							dvf.setDataType(DataValueType.VALUE);
							indic = new IndicatorCriteria();
							indic.setWID(indEntry.getValue().getWid());
							dvf.setIndicator(indic);
							dvf.setDisplayRate(mdc.getDisplayRate());
							dvf.setTimestamp(mdc.getTimeRangeStart());
							dvf .setValueComparator(vcomp);
							ic.getDataValues().add(dvf);								
						}
					}
				}
			}

		}
	}
	private void addMatrixDataColumnForIndicator(MatrixDataCriteria mdc, String columnName, String wid, MatrixDataInputColumnType type) {

		MatrixDataInputColumn mdic = new MatrixDataInputColumn();
		mdc.getInput().add(mdic);
		mdic.setColumnName(columnName);
		mdic.setType(type);
		IndicatorCriteria indc = new IndicatorCriteria();
		indc.setWID(wid);
		mdic.setIndicator(indc);
		MatrixDataOutputColumn outCol = new MatrixDataOutputColumn();
		mdc.getOutput().add(outCol);
		outCol.setType(MatrixDataOutputColumnType.INPUT);
		outCol.setColumnName(columnName);
	}
	
	private void fillMutator(OutputMutator output,MatrixDataResponse md,VmTableDef tabledef, Map<String,PropertyDesc> propertiesDef) throws SchemaChangeException {
		int index = 0; //index in data response
		TypeManager tm;
		// insert DR
		tm = new  TypeManager.TypeVarchar();
		MajorType type = tm.getMajorTypeRequired();
		MaterializedField field = MaterializedField.create(VmTable.DR_COLUMN_NAME, type);
		Class<? extends ValueVector> clazz = (Class<? extends ValueVector>)TypeHelper.getValueVectorClass(
				tm.getMinorType(), type.getMode());
		vectorForDR= output.addField(field, clazz);

		for(MatrixDataResponseColumn col : md.getColumns()) {
			//attributes
			String columName = col.getColumnName();
			if(columName.equals(VmTable.NAME_COLUMN_NAME))
				tm = new TypeManager.TypeVarchar();
			else if(columName.equals(VmTable.TIMESTAMP_COLUMN_NAME)) {
				tm = new TypeManager.TypeTimestamp();
			}
			else if(columName.equals(VmTable.ID_COLUMN_NAME)) {
				tm = new TypeManager.TypeLong();
			}
			else if(columName.equals(VmTable.TAG_COLUMN_NAME)) {
				tm = new TypeManager.TypeVarchar();
			}
			else if(columName.equals(VmTable.PROXY_OF_COLUMN_NAME)) {
				tm = new TypeManager.TypeVarchar();
			}
			else {
				IndicatorDesc desc = tabledef.getIndicators().get(columName);
				if(desc != null) {
					//indicator
					tm = TypeManager.getTypeManager(desc.type);
				}else {
					PropertyDesc  descI = propertiesDef.get(columName);
					if(descI != null) {
						tm = TypeManager.getTypeManager(descI.type);
					}
					else
						tm = new TypeManager.TypeVarchar();
				}
			}
			types.add(tm);
			type = tm.getMajorType();
			field = MaterializedField.create(columName, type);
			clazz = (Class<? extends ValueVector>)TypeHelper.getValueVectorClass(
					tm.getMinorType(), type.getMode());
			ValueVector vector = output.addField(field, clazz);

			tm.setValueVector(index, vector);
			index++;

		}
	}
}
