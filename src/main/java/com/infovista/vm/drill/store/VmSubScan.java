package com.infovista.vm.drill.store;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;




public class VmSubScan extends AbstractBase implements SubScan {

	private VmGroupScan groupScan;
	
	public VmSubScan(VmGroupScan groupScan) {
		this.groupScan = groupScan;
	}
	@JsonIgnore
	public VmGroupScan getGroupScan() {
		return groupScan;
	}
	@Override
	public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
		return physicalVisitor.visitSubScan(this, value);
	}
	@Override
	public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
		 Preconditions.checkArgument(children.isEmpty());
		 return new VmSubScan(groupScan);
	}
	@Override
	public int getOperatorType() {
		// TODO Auto-generated method stub
		return -1;
	}
	@Override
	public Iterator<PhysicalOperator> iterator() {
		 return Collections.emptyIterator();
	}
	public List<SchemaPath> getColumns() {
		return groupScan.getColumns();
	}
	

	
}
