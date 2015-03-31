package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nar.service.SosAggregationService;
import gov.usgs.cida.nude.resultset.ResultSetUtils;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ExcelLeadingZeroIdFixTransformTest {

    private static final Logger log = LoggerFactory.getLogger(WaterYearTransform.class);
    public static final Column QW_ID_COLUMN = new SimpleColumn(SosAggregationService.SITE_QW_ID_IN_COL);
    public static final Column FLOW_ID_COLUMN = new SimpleColumn(SosAggregationService.SITE_FLOW_ID_IN_COL);

    public static ColumnGrouping inputColumns = null;
    public static Iterable<TableRow> inputSampleDataset = null;
    public static Iterable<TableRow> expectedSampleDataset = null;

    public static Iterable<TableRow> inputWaterYearDataset = null;
    public static Iterable<TableRow> expectedWaterYearDataset = null;

    public ExcelLeadingZeroIdFixTransformTest() {
    }

    @BeforeClass
    public static void setUpClass() {

	inputColumns = new ColumnGrouping(Arrays.asList(new Column[]{
	    QW_ID_COLUMN,
	    FLOW_ID_COLUMN,
	    new SimpleColumn("CONSTIT"),
	    new SimpleColumn("MODTYPE"),
	    new SimpleColumn("procedure")
	}));

	inputSampleDataset = ResultSetUtils.createTableRows(inputColumns, new String[][]{
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.143605948E7"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "4585894.2"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "7674767.59"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8882042.96"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.325656857E7"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "5864574.53"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8600366.26"}
	});

	expectedSampleDataset = ResultSetUtils.createTableRows(inputColumns, new String[][]{
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.143605948E7"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "4585894.2"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "7674767.59"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8882042.96"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.325656857E7"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "5864574.53"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8600366.26"}
	});

	inputWaterYearDataset = ResultSetUtils.createTableRows(inputColumns, new String[][]{
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.325656857E7"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.143605948E7"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "4585894.2"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "7674767.59"},
	    new String[]{"01646580", "01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8882042.96"}
	});

	expectedWaterYearDataset = ResultSetUtils.createTableRows(inputColumns, new String[][]{
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.325656857E7"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "1.143605948E7"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "4585894.2"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "7674767.59"},
	    new String[]{"'01646580", "'01646581", "http://cida.usgs.gov/def/NAR/property/Q", "annual_flow", "8882042.96"}
	});
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of transform method, of class WaterYearTransform.
     */
    @Test
    public void testTransform() {
	Column[] columns = new Column[]{QW_ID_COLUMN, FLOW_ID_COLUMN};
	final String ID_1 = "09834";// one zero-prefixed
	final String PREFIXED_ID_1 = ExcelLeadingZeroIdFixTransform.EXCEL_LEADING_CHAR + ID_1;
	final String ID_2 = "98340";//one not zero-prefixed
	final String PREFIXED_ID_2 = ExcelLeadingZeroIdFixTransform.EXCEL_LEADING_CHAR + ID_2;
	String[][] inputToExpectedOutput = new String[][]{
	    {ID_1, PREFIXED_ID_1},
	    {ID_2, PREFIXED_ID_2}
	};
	for (Column col : columns) {
	    for (String[] inputToExpectedOutputEntry : inputToExpectedOutput) {
		String input = inputToExpectedOutputEntry[0];
		String expected = inputToExpectedOutputEntry[1];
		TableRow row = new TableRow(col, input);
		ExcelLeadingZeroIdFixTransform instance = new ExcelLeadingZeroIdFixTransform(col);
		String result = instance.transform(row);
		assertEquals(expected, result);
	    }
	}
    }

    /**
     * Test of transform method, of class WaterYearTransform.
     *
     * @throws java.sql.SQLException
     */
    @Test
    public void testSampleDataset() throws SQLException {
	log.debug("sampleDataset");
	ResultSet expected = new IteratorWrappingResultSet(expectedSampleDataset.iterator());
	ResultSet in = new IteratorWrappingResultSet(inputSampleDataset.iterator());
	ResultSet actual = new NudeFilterBuilder(inputColumns)
		.addFilterStage(new FilterStageBuilder(inputColumns)
			.addTransform(QW_ID_COLUMN, new ExcelLeadingZeroIdFixTransform(QW_ID_COLUMN))
			.addTransform(FLOW_ID_COLUMN, new ExcelLeadingZeroIdFixTransform(FLOW_ID_COLUMN))
			.buildFilterStage())
		.buildFilter().filter(in);
	assertTrue(ResultSetUtils.checkEqualRows(expected, actual));
    }

}
