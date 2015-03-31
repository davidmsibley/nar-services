package gov.usgs.cida.nar.service.plan;
import gov.usgs.cida.nar.transform.ExcelLeadingZeroIdFixTransform;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilterStep;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.plan.PlanStep;
import java.util.List;

public class FixLeadingZeroIdInExcelStepBuilder {
    public static PlanStep build(final List<PlanStep> prevSteps, final String SITE_FLOW_ID_COL_NAME, final String SITE_QW_ID_COL_NAME) {
	ColumnGrouping originals = prevSteps.get(prevSteps.size() - 1).getExpectedColumns();

	FilterStep leadingZeroIdExcelFixStep = new FilterStep(new NudeFilterBuilder(originals)
		.addFilterStage(new FilterStageBuilder(originals)
			.addTransform(new SimpleColumn(SITE_FLOW_ID_COL_NAME), new ExcelLeadingZeroIdFixTransform(originals.get(originals.indexOf(SITE_FLOW_ID_COL_NAME))))
			.addTransform(new SimpleColumn(SITE_QW_ID_COL_NAME), new ExcelLeadingZeroIdFixTransform(originals.get(originals.indexOf(SITE_QW_ID_COL_NAME))))
			.buildFilterStage())
		.buildFilter());

	return leadingZeroIdExcelFixStep;
    }
}
