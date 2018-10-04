package com.infovista.vm.drill.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;

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
import com.infovista.vistamart.datamodel.ws.v8.ValueComparator;
import com.infovista.vistamart.datamodel.ws.v8.VistaCriteria;
import com.infovista.vm.drill.store.Filter.FilterDesc;

public class VmDataRecorReader extends VmRecordReader {


	public VmDataRecorReader(VmGroupScan scan) {
		super(scan);
	}

	@Override
	public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
		init();
		
		Filter filter = null;
		VmScanSpec scanSpec = groupScan.getVmScanSpec();
		if(scanSpec!=null) {
			filter = scanSpec.getFilter();
		}

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
			String displayRate = groupScan.getVmScanSpec().getDisplayRate();
			DisplayRate dr = VmStoragePlugin.drMapName.get(displayRate);
			if(dr != null)
				mdc.setDisplayRate(dr);
			else
			{
				throw new ExecutionSetupException("list of supported values for column  "+VmTable.DR_COLUMN_NAME+" is "+VmStoragePlugin.listOfDispalyRates);
			}
			InstanceCriteria ic = new InstanceCriteria();
			VistaCriteria vc = new VistaCriteria();
			vc.setName(tableName.substring(0,tableName.indexOf(VmTable.DATA_NAME_SUFFIX)));
			ic.getVistas().add(vc);
			mdc.getInstancesIn().add(ic);
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
				filterDescList = filter.filters.get(VmTable.ID_COLUMN_NAME);
				if(filterDescList != null) {
					for(FilterDesc desc : filterDescList){
						if(desc.compareOp.equals("equal") ) {
							Long value = null;
							if(desc.value instanceof Integer)
								value = Long.valueOf(((Integer)desc.value).longValue());
							else if(desc.value instanceof Long)
								value = (Long) desc.value;
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
			}

			if(isStarQuery() || colNames.contains(VmTable.NAME_COLUMN_NAME))
			{
				addMatrixDataColumn(mdc, VmTable.NAME_COLUMN_NAME, MatrixDataInputColumnType.INS_NAME);
			}
			if(isStarQuery() || colNames.contains(VmTable.ID_COLUMN_NAME))
			{
				addMatrixDataColumn(mdc, VmTable.ID_COLUMN_NAME, MatrixDataInputColumnType.INS_ID);
			}
			if(isStarQuery() || colNames.contains(VmTable.TAG_COLUMN_NAME))
			{	
				addMatrixDataColumn(mdc, VmTable.TAG_COLUMN_NAME, MatrixDataInputColumnType.INS_TAG);
			}
			//timestamp
			if(isStarQuery() || colNames.contains(VmTable.TIMESTAMP_COLUMN_NAME)) {
				addMatrixDataColumn(mdc, VmTable.TIMESTAMP_COLUMN_NAME, MatrixDataInputColumnType.TIMESTAMP);
			}

			buildIndicatorsColumns(colNames, tabledef, filter, ic);

// request data from VistaMart
			md = service.getMatrixData(mdc);
			fillMutator(output, md, tabledef);
		}
		catch(ExecutionSetupException e) {throw e;}
		catch(Exception e) {
			throw new ExecutionSetupException("Error on record reader setup", e);
		}
	}

	private void fillMutator(OutputMutator output,MatrixDataResponse md,VmTableDef tabledef) throws SchemaChangeException {
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
			else {
				IndicatorDesc desc = tabledef.getIndicators().get(columName);
				if(desc != null) {
					tm = TypeManager.getTypeManager(desc.type);
				}else
					tm = new TypeManager.TypeVarchar();
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
}
