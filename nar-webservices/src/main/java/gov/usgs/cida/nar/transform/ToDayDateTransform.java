package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO --REALLY ONLY DOES CALENDAR YEAR
 * @author dmsibley
 */
public class ToDayDateTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(ToDayDateTransform.class);

	protected final Column inColumn;

	public ToDayDateTransform(Column inColumn) {
		this.inColumn = inColumn;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		
		if(null != row) {
			String in = row.getValue(inColumn);
			try {
				DateTime inDate = ISODateTimeFormat.dateTimeParser().parseDateTime(in);
				result = inDate.withZone(DateTimeZone.UTC).toString("YYYY-MM-dd");
			} catch (Exception e) {
				log.trace("Could not parse incoming value", e);
			}
			
			if (null == result) {
				result = in;
			}
		}
		
		return result;
	}

}
