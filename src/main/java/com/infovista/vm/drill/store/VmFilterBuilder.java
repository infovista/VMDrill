package com.infovista.vm.drill.store;

import java.util.ArrayList;
import java.util.Arrays;


import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.infovista.vm.drill.store.Filter.FilterDesc;

public class VmFilterBuilder  extends AbstractExprVisitor<VmScanSpec, Void, RuntimeException> {
	//private final static Logger logger = LoggerFactory.getLogger(VmFilterBuilder.class);
	final private VmGroupScan groupScan;
	final private LogicalExpression le;

	private boolean allExpressionsConverted = true;

	public boolean isAllExpressionsConverted() {
		return allExpressionsConverted;
	}

	VmFilterBuilder(VmGroupScan groupScan, LogicalExpression le){
		this.groupScan = groupScan;
		this.le = le;
	}

	public VmScanSpec parseTree() {
		VmScanSpec parsedSpec = le.accept(this, null);
		if(parsedSpec != null) {
			parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getVmScanSpec(),parsedSpec);
		}
		return parsedSpec;
	}

	@Override
	public VmScanSpec visitUnknown(LogicalExpression e, Void value)throws RuntimeException {
		allExpressionsConverted = false;
		return null;
	}

	@Override
	public VmScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
		return visitFunctionCall(op, value);
	}

	public VmScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException{
		VmScanSpec nodeScanSpec = null;
		String functionName = call.getName();
		ImmutableList<LogicalExpression> args = call.args;
		if(SingleFunctionProcessor.isCompareFunction(functionName)) {
			SingleFunctionProcessor processor = SingleFunctionProcessor.process(call);
			if(processor.isSuccess()) {
				nodeScanSpec = createVmScanSpec(processor.getFunctionName(),processor.getPath(),processor.getValue());
			}
		} else {
			switch(functionName) {
			case "booleanAnd":
				VmScanSpec firstScanSpec = args.get(0).accept(this,null);
				for (int i = 1; i < args.size(); ++i) {
					VmScanSpec nextScanSpec = args.get(i).accept(this, null);
					if(firstScanSpec != null && nextScanSpec != null) {
						nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
					}else {
						allExpressionsConverted = false;
						// only on booleanAnd
						nodeScanSpec = firstScanSpec == null ? nextScanSpec: firstScanSpec;
					}
					firstScanSpec = nodeScanSpec;
				}
				break;
			case "booleanOr":
				nodeScanSpec = processOrOperator(args);
				break;
			default:
				break;
			}
		}
		if (nodeScanSpec == null) {
			allExpressionsConverted = false;
		}
		return nodeScanSpec;
	}

	private VmScanSpec createVmScanSpec(String functionName, String fieldName, Object fieldValue) {
		if(fieldName.equals(VmTable.TIMESTAMP_COLUMN_NAME)) {
			switch (functionName) {
			case "greater_than_or_equal_to":
			case "greater_than":
				long start = ((Long)fieldValue).longValue();
				return new VmScanSpec(groupScan.getTableName(),start,-1,"",new Filter());
			case "less_than_or_equal_to":
			case "less_than":
				long end = ((Long)fieldValue).longValue();
				return new VmScanSpec(groupScan.getTableName(),-1,end,"",new Filter());
			case "equal":
				 start = ((Long)fieldValue).longValue();
				 return new VmScanSpec(groupScan.getTableName(),start,start,"",new Filter());
			default:
				return null;	
			}
		}
		if(fieldName.equals(VmTable.DR_COLUMN_NAME)) {
			switch (functionName) {
			case "equal":
				return new VmScanSpec(groupScan.getTableName(),-1,-1,((String)fieldValue).trim(),new Filter());
			default :
				return null;
			}
		}
		Filter filter = new Filter();
		Filter.FilterDesc desc = new FilterDesc();
		desc.compareOp = functionName;
		desc.value = fieldValue;

		filter.filters.put(fieldName, new ArrayList<FilterDesc>(Arrays.asList(desc)));
		VmScanSpec scanspec = new VmScanSpec(groupScan.getTableName());
		scanspec.filter = filter;

		return scanspec;
	}

	private VmScanSpec createVmScanSpec(String functionName,SchemaPath field,Object fieldValue) {
		String fieldName = field.getRootSegmentPath();
		return createVmScanSpec(functionName, fieldName, fieldValue);
	}

	private VmScanSpec mergeScanSpecs(String functionName, VmScanSpec leftScanSpec, VmScanSpec rightSpec) {
		Filter newFilter = null;
		long startTimestamp = -1;
		long endTimestamp = -1;
		String dr = "";
		switch(functionName) {
		case "booleanAnd" :
			startTimestamp = Math.max(leftScanSpec.timestampStart, rightSpec.timestampStart);
			if(leftScanSpec.timestampEnd == -1) {
				endTimestamp = rightSpec.timestampEnd;
			}
			else if(rightSpec.timestampEnd == -1) {
				endTimestamp = leftScanSpec.timestampEnd;
			}else {
				endTimestamp = Math.min(leftScanSpec.timestampEnd, rightSpec.timestampEnd);
			}
			String leftDr = leftScanSpec.getDisplayRate();
			String rightDr = rightSpec.getDisplayRate();
			if(!leftDr.isEmpty()  && !rightDr.isEmpty() && !leftDr.equals(rightDr)) {
				dr = "";
			}else {
				dr = ( leftDr.isEmpty())? rightDr: leftDr;
			}
			if(leftScanSpec.getFilter() == null) newFilter = rightSpec.getFilter();
			else if(rightSpec.getFilter()== null) newFilter = leftScanSpec.getFilter();
			else newFilter = Filter.mergeFilters(rightSpec.filter, leftScanSpec.filter);
			break;
			default :
				return null;
		}
		return new VmScanSpec(groupScan.getTableName(),startTimestamp,endTimestamp,dr,newFilter);
	}

	VmScanSpec processOrOperator(ImmutableList<LogicalExpression> args) {
		String fieldName = null;
		ArrayList<Object> inArrays = null;
		if(!(args.get(0) instanceof FunctionCall &&((FunctionCall)args.get(0)).getName().equals("equal")))
			return null;
		FunctionCall fcall = (FunctionCall)args.get(0);

		SingleFunctionProcessor processor = SingleFunctionProcessor.process(fcall);
		if(processor.isSuccess()) {
			fieldName = processor.getPath().getRootSegmentPath();
			if(fieldName.equals(VmTable.TIMESTAMP_COLUMN_NAME))
			{
				return null;
			}
			inArrays = new ArrayList<Object>(Arrays.asList(processor.getValue()));
		}
		else {
			return null;
		}
		for (int i = 1; i < args.size(); ++i) {
			if(args.get(i) instanceof FunctionCall ) {
				fcall = (FunctionCall)args.get(i);
				if(fcall.getName().equals("equal")) {
					processor = SingleFunctionProcessor.process(fcall);
					if(processor.isSuccess()) {
						if(!processor.getPath().getRootSegmentPath().equals(fieldName)) {		
							return null;
						}
						inArrays.add(processor.getValue());
					}
				}
			}
		}
		// build ScanSpec
		return createVmScanSpec("in", fieldName, inArrays) ;
	}
}
