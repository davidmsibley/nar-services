package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelLeadingZeroIdFixTransform implements ColumnTransform {

    private static final Logger log = LoggerFactory.getLogger(ExcelLeadingZeroIdFixTransform.class);
    public static final char EXCEL_LEADING_CHAR = '\'';
    protected final Column idColumn;

    public ExcelLeadingZeroIdFixTransform(Column idColumn) {
	this.idColumn = idColumn;
    }
    /**
     * Preserves the leading zero on the ids that would otherwise be eliminated
     * during excel import. Prepending a single quote to the beginning of a 
     * cell causes excel to interpret the cell as a string instead of a number. 
     * @param row
     * @return single-quote prefixed String of the existing value
     */
    @Override
    public String transform(TableRow row) {
	String result = null;

	if (null != row) {
	    String id = row.getValue(idColumn);
	    result = EXCEL_LEADING_CHAR + id;
	}

	return result;
    }

}
