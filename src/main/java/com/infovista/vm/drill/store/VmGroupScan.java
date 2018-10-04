package com.infovista.vm.drill.store;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;


public class VmGroupScan extends AbstractGroupScan implements SubScan{

	private VmStoragePlugin plugin;
	private List<SchemaPath> columns;
	private VmScanSpec vmScanSpec;

	private boolean filterPushedDown = false;

	public VmGroupScan(VmStoragePlugin plugin, String userName,VmScanSpec scanSpec, List<SchemaPath> columns) {
		super(userName);
		this.vmScanSpec = scanSpec;
		this.plugin = plugin;
		this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;

	}

	private  VmGroupScan( VmGroupScan from) {
		super(from);
		this.plugin = from.plugin;
		this.columns = from.columns;
		this.vmScanSpec = from.vmScanSpec;
		this.filterPushedDown = from.filterPushedDown;
	}

	@Override
	public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
		// TODO Auto-generated method stub

	}

	@Override
	public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
		return new VmSubScan(this);
	}

	@Override
	public int getMaxParallelizationWidth() {
		return 1;
	}

	@Override
	public String getDigest() {
		return toString();
	}

	@Override
	public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
		Preconditions.checkArgument(children.isEmpty());
		return new VmGroupScan(this);
	}

	@Override
	public GroupScan clone(List<SchemaPath> schemaColumns) {
		VmGroupScan clone = new VmGroupScan(this);
		if(schemaColumns != ALL_COLUMNS || columns == null ) {
			clone.columns = schemaColumns;
		}
		return clone;
	}
	@Override
	public ScanStats getScanStats() 
	{
		if(vmScanSpec.getFilter() == null)
		return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 1000, 1, 1000);
		else return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 500, 1, 1000);
	}

	@Override
	public boolean canPushdownProjects(List<SchemaPath> columns) {
		return true;
	}

	public VmStoragePlugin getPlugin() {
		return plugin;
	}


	public List<SchemaPath> getColumns() {
		return columns;
	}
	@JsonIgnore
	public boolean isFilterPushedDown() {
		return filterPushedDown;
	}
	@JsonIgnore
	public void setFilterPushedDown(boolean b) {
		this.filterPushedDown = true;
	}
	public VmScanSpec getVmScanSpec() {
		return vmScanSpec;
	}

	public void setVmScanSpec(VmScanSpec vmScanSpec) {
		this.vmScanSpec = vmScanSpec;
	}
	public String getTableName() {
		return getVmScanSpec().getTableName();
	}

}
