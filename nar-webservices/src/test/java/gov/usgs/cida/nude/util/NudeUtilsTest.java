package gov.usgs.cida.nude.util;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.time.DateRange;
import gov.usgs.cida.sos.Observation;
import gov.usgs.cida.sos.ObservationMetadata;
import java.util.Map;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author jiwalker
 */
public class NudeUtilsTest {
	
	public NudeUtilsTest() {
	}

	/**
	 * Test of makeRowMap method, of class NudeUtils.
	 */
	@Test
	public void testMakeRowMap() {
		Observation bean = new Observation()
				.metadata(new ObservationMetadata()
						.defaultUnits("ft")
						.featureOfInterest("test")
						.observedProperty("test")
						.procedure("test")
						.timePeriod(new DateRange()))
				.time(new DateTime())
				.value("1.2");
				
		Map<Column, String> result = NudeUtils.makeRowMap(bean);
		assertThat("1.2", is(equalTo(result.get(new SimpleColumn("value")))));
	}
	
	/**
	 * Test of makeTableRow method, of class NudeUtils.
	 */
	@Test
	public void testMakeTableRow() {
		
	}
}
