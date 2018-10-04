package com.infovista.vm.drill.store;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import com.google.common.collect.ImmutableList;


public abstract class VmPushFilterIntoScan extends StoragePluginOptimizerRule {

	private VmPushFilterIntoScan(RelOptRuleOperand operand, String description) {
		super(operand, description);
	}

	public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new VmPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "VmPushFilterIntoScan:Filter_On_Scan") {
		@Override
		public void onMatch(RelOptRuleCall call) {
			final ScanPrel scan = (ScanPrel) call.rel(1);
			final FilterPrel filter = (FilterPrel) call.rel(0);
			final RexNode condition = filter.getCondition();

			GroupScan groupscan = scan.getGroupScan();
			if(!(groupscan instanceof VmGroupScan))
				return;
			VmGroupScan vmgroupscan = (VmGroupScan)groupscan;
			if(vmgroupscan.isFilterPushedDown())
				return;
			doPushFilterToScan(call, filter,null,scan,vmgroupscan,condition);
		}
		
		@Override
		public boolean matches(RelOptRuleCall call) {
				final ScanPrel scan = (ScanPrel) call.rel(1);
				if (scan.getGroupScan() instanceof VmGroupScan) {
					return super.matches(call);
				}
				return false;
		}
	};

	public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new VmPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))), "VmPushFilterIntoScan:Filter_On_Project") {
		@Override
		public void onMatch(RelOptRuleCall call) {
			final ScanPrel scan = (ScanPrel) call.rel(2);
			final ProjectPrel project = (ProjectPrel) call.rel(1);
			final FilterPrel filter = (FilterPrel) call.rel(0);

			VmGroupScan groupscan = (VmGroupScan) scan.getGroupScan();
			if(groupscan.isFilterPushedDown())
				return;
			// convert the filter to one that references the child of the project
			final RexNode condition =  RelOptUtil.pushPastProject(filter.getCondition(), project);
			doPushFilterToScan(call, filter, project, scan, groupscan, condition);

		}
		@Override
		public boolean matches(RelOptRuleCall call) {
				final ScanPrel scan = (ScanPrel) call.rel(2);
				if (scan.getGroupScan() instanceof VmGroupScan) {
					return super.matches(call);
				}
				return false;
		}

	};
	
	protected void doPushFilterToScan(final RelOptRuleCall call, final FilterPrel filter, final ProjectPrel project, final ScanPrel scan, final VmGroupScan groupScan, final RexNode condition) {
		final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
		final VmFilterBuilder vmFilterBuilder = new VmFilterBuilder(groupScan, conditionExp);
		final VmScanSpec newScanSpec = vmFilterBuilder.parseTree();
		if(newScanSpec == null) {
			return;//no filter pushdown ==> No transformation.
		}
		//groupScan.original.setVmScanSpec(newScanSpec);
		final VmGroupScan newGroupsScan = new VmGroupScan(groupScan.getPlugin(), groupScan.getUserName(), newScanSpec, groupScan.getColumns());
		newGroupsScan.setFilterPushedDown(true);
		final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
		// Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
		final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode)newScanPrel));

		call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
	}


}
