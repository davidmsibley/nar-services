package gov.usgs.cida.nude.util;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.time.DateRange;
import gov.usgs.cida.sos.Observation;
import gov.usgs.cida.sos.ObservationMetadata;
import java.util.HashMap;
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
	 * Test of makeFlatRowMap method, of class NudeUtils.
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
				
		Map<Column, String> result = NudeUtils.makeFlatRowMap(bean);
		assertThat(result.get(new SimpleColumn("defaultUnits")), is(equalTo("ft")));
		assertThat(result.get(new SimpleColumn("featureOfInterest")), is(equalTo("test")));
		assertThat(result.get(new SimpleColumn("observedProperty")), is(equalTo("test")));
		assertThat(result.get(new SimpleColumn("procedure")), is(equalTo("test")));
		assertThat(result.get(new SimpleColumn("timePeriod")), is(notNullValue()));
		assertThat(result.get(new SimpleColumn("value")), is(equalTo("1.2")));
	}
	
	/**
	 * Test of makeTableRow method, of class NudeUtils.
	 */
	@Test
	public void testMakeTableRow() {
		
	}
	
	@Test
	public void testIsLiteral() {
		assertThat(NudeUtils.isValue("test"), is(true));
		assertThat(NudeUtils.isValue(true), is(true));
		assertThat(NudeUtils.isValue(1), is(true));
		assertThat(NudeUtils.isValue(1.4), is(true));
		assertThat(NudeUtils.isValue('c'), is(true));
		assertThat(NudeUtils.isValue(new HashMap<String, String>()), is(false));
	}
}
