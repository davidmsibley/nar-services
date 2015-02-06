package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.io.File;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author jiwalker
 */
public class CachedResultSetTest {

	private static final Column primaryCol = new SimpleColumn("primary");
	private static final Column valueCol = new SimpleColumn("valueCol");
	private static ColumnGrouping cg;
	static {
		List<Column> columns = new LinkedList<>();
		columns.add(primaryCol);
		columns.add(valueCol);
		cg = new ColumnGrouping(columns);
	}
	
	private ResultSet makeResultSet() {
		StringTableResultSet rs = new StringTableResultSet(cg);
		for (int i=0; i<10; i++) {
			rs.addRow(makeRow(""+i, "test"+i));
		}
		return rs;
	}
	
	private TableRow makeRow(String prim, String val) {
		Map<Column, String> row = new HashMap<>();
		row.put(primaryCol, prim);
		row.put(valueCol, val);
		return new TableRow(cg, row);
	}

	/**
	 * Test of serialize method, of class CachedResultSet.
	 */
	@Test
	public void testSerialize() throws Exception {
		ResultSet rset = makeResultSet();
		File file = File.createTempFile("test", "tmp");
		CachedResultSet.serialize(rset, file);
		CachedResultSet instance = new CachedResultSet(file);
		assertThat(instance.getMetaData().getColumnCount(), is(equalTo(2)));
		int i = 0;
		while (instance.next()) {
			TableRow row = TableRow.buildTableRow(instance);
			String value = row.getValue(valueCol);
			assertThat("test" + i++, is(equalTo(value)));
		}
	}

}
