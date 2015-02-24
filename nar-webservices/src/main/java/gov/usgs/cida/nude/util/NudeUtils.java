package gov.usgs.cida.nude.util;

import com.google.common.collect.Lists;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.nude.time.DateRange;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These methods should probably be moved to the respective NUDE classes
 * 
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class NudeUtils {
	
	private static final Logger log = LoggerFactory.getLogger(NudeUtils.class);
	
	/**
	 * Notice, this uses recursion, so will not be good for very nested Objects.
	 * @param bean Bean to make into a map, all descendents must be beans
	 * @return Map containing columns and values
	 */
	public static Map<Column, String> makeFlatRowMap(Object bean) {
		Map<Column, String> result = new HashMap<>();
		BeanMap row = new BeanMap(bean);
		for (Object colName : row.keySet()) {
			Object obj = row.get(colName);
			
			
			if (isValue(obj)) {
				result.put(new SimpleColumn(colName.toString()), makeString(obj));
			} else {
				try {
					Map<String, String> describe = BeanUtils.describe(obj);
					if (!"class java.lang.Class".equals(describe.get("class"))) {
						result.putAll(makeFlatRowMap(obj));
					}
				} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
					log.debug("Skipping bean in flattening", ex);
				}
			}
		}
		return result;
	}
	
	public static ColumnGrouping makeColumnGrouping(Object bean) {
		return makeColumnGrouping(makeFlatRowMap(bean));
	}
	
	public static ColumnGrouping makeColumnGrouping(Map<Column, String> row) {
		List<Column> columns = Lists.newLinkedList(row.keySet());
		ColumnGrouping cg = new ColumnGrouping(columns);
		return cg;
	}
	
	public static TableRow makeTableRow(Object bean) {
		return makeTableRow(makeFlatRowMap(bean));
	}

	public static TableRow makeTableRow(Map<Column, String> row) {
		return new TableRow(makeColumnGrouping(row), row);
	}
	
	/**
	 * Test whether the object can be used in a TableRow as a single cell
	 * when toString() is called.
	 * @param obj Object to test 
	 * @return true if obj is one of the value types
	 */
	public static boolean isValue(Object obj) {
		return (obj instanceof String ||
				obj instanceof Character ||
				obj instanceof Boolean ||
				obj instanceof Number ||
				obj instanceof DateTime ||
				obj instanceof DateRange);
	}
	
	/**
	 * Since DateRange is meant to store many date but I only care about the
	 * start and end, putting this functionality here.  Produces an ISO8601 valid
	 * interval representing the range
	 * @param range
	 * @return 
	 */
	private static String makeDateRangeString(DateRange range) {
		return range.getBegin().toString() + "/" + range.getEnd().toString();
	}
	
	/**
	 * Rather than make a makeString method for each isValue
	 * @param obj
	 * @return 
	 */
	private static String makeString(Object obj) {
		String result = "";
		if (obj instanceof DateRange) {
			result = makeDateRangeString((DateRange)obj);
		} else {
			result = obj.toString();
		}
		return result;
	}
}
