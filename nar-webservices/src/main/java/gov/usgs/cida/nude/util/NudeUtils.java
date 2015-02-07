package gov.usgs.cida.nude.util;

import com.google.common.collect.Lists;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.BeanMap;

/**
 * These methods should probably be moved to the respective NUDE classes
 * 
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class NudeUtils {
	
	public static Map<Column, String> makeRowMap(Object bean) {
		Map<Column, String> result = new HashMap<>();
		Map<String, Object> row = new BeanMap(bean);
		for (String colName : row.keySet()) {
			result.put(new SimpleColumn(colName), row.get(colName).toString());
		}
		return result;
	}
	
	public static ColumnGrouping makeColumnGrouping(Object bean) {
		return makeColumnGrouping(makeRowMap(bean));
	}
	
	public static ColumnGrouping makeColumnGrouping(Map<Column, String> row) {
		List<Column> columns = Lists.newLinkedList(row.keySet());
		ColumnGrouping cg = new ColumnGrouping(columns);
		return cg;
	}
	
	public static TableRow makeTableRow(Object bean) {
		return makeTableRow(makeRowMap(bean));
	}

	public static TableRow makeTableRow(Map<Column, String> row) {
		return new TableRow(makeColumnGrouping(row), row);
	}
	
	
}
