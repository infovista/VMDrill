package com.infovista.vm.drill.store;

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import com.google.common.base.Preconditions;

public class VmScanBatchCreator implements BatchCreator<VmSubScan> {

	@Override
	public ScanBatch getBatch(ExecutorFragmentContext context, VmSubScan subScan,
			List<RecordBatch> children) throws ExecutionSetupException {
		Preconditions.checkArgument(children.isEmpty());
		List<RecordReader> readers = new LinkedList<>();
		String tableName = subScan.getGroupScan().getTableName();
		if(subScan.getGroupScan().getPlugin().indicatorsByDataTable.containsKey(tableName))
			readers.add(new VmDataRecorReader(subScan.getGroupScan()));
		else
			readers.add(new VmRecordReader(subScan.getGroupScan()));
		return new ScanBatch(subScan, context, readers);
	}

}
