package gov.usgs.cida.nar.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import gov.usgs.cida.nar.service.DataType;
import gov.usgs.cida.nar.service.DownloadServiceParameters;
import gov.usgs.cida.nar.service.DownloadType;

import org.junit.Test;

public class ServiceParameterUtilsTest {

	@Test
	public void testGetDelimiterFromFormat() {
		assertEquals("Defaults to csv", ",", ServiceParameterUtils.getDelimiterFromFormat(null));
		assertEquals("Produces tabs when tsv requested", "\t", ServiceParameterUtils.getDelimiterFromFormat(Arrays.asList(new String[] { "tsv" })));
	}
	
	@Test
	public void testIsSiteInformationRequested() {
		
		assertFalse("Site information not requested if param not provided", ServiceParameterUtils.isSiteInformationRequested(null));
		
		assertTrue("Site information requeted when parameter " + DownloadServiceParameters.dataType.name() +
				" contains value " + DataType.siteInformation.name(), ServiceParameterUtils.isSiteInformationRequested(Arrays.asList(new String[] { DataType.siteInformation.name() })));

		assertTrue("Site information requeted when parameter " + DownloadServiceParameters.dataType.name() +
				" contains value " + DataType.siteInformation.name(), ServiceParameterUtils.isSiteInformationRequested(Arrays.asList(new String[] { DataType.siteInformation.name(), "mixedinOtherValues" })));
	}
	
	@Test
	public void testIsDiscreteQwRequested() {
		assertFalse("Discrete QW not requested if params not provided", ServiceParameterUtils.isDiscreteQwRequested(null, null));
		
		assertTrue("Discrete QW requested if water quality data type requested without any QW data type filters", 
				ServiceParameterUtils.isDiscreteQwRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), null));

		assertFalse("Discrete QW NOT requested if water quality data type requested with a QW data type filters that do not include discrete QW", 
				ServiceParameterUtils.isDiscreteQwRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), Arrays.asList(new String[] { DownloadType.annualLoad.name() })));

		assertTrue("Discrete QW requested if water quality data type requested with a QW data type filters that include discrete QW", 
				ServiceParameterUtils.isDiscreteQwRequested(
						Arrays.asList(new String[] { DataType.waterQuality.name() }), 
						Arrays.asList(new String[] { DownloadType.annualLoad.name(), DownloadType.discreteQw.name() })
				)
			);
	}
	
	@Test
	public void testIsAnnualLoadsRequested() {
		
		assertFalse("Annual not requested if params not provided", ServiceParameterUtils.isAnnualLoadsRequested(null, null));
		
		assertTrue("Annual Load requested if water quality data type requested without any QW data type filters", 
				ServiceParameterUtils.isAnnualLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), null));

		assertFalse("Annual Load NOT requested if water quality data type requested with a QW data type filters that do not include Annual Load", 
				ServiceParameterUtils.isAnnualLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), Arrays.asList(new String[] { DownloadType.monthlyLoad.name() })));

		assertTrue("Annual Load requested if water quality data type requested with a QW data type filters that include Annual Load", 
				ServiceParameterUtils.isAnnualLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), Arrays.asList(new String[] { DownloadType.annualLoad.name(), DownloadType.discreteQw.name() })));
	}
	
	@Test
	public void testIsMonthlyLoadsRequested() {
		assertFalse("Monthly not requested if params not provided", ServiceParameterUtils.isMonthlyLoadsRequested(null, null));
		
		assertTrue("Monthly Load requested if water quality data type requested without any QW data type filters", 
				ServiceParameterUtils.isMonthlyLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), null));

		assertFalse("Monthly Load NOT requested if water quality data type requested with a QW data type filters that do not include Monthly Load", 
				ServiceParameterUtils.isMonthlyLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), Arrays.asList(new String[] { DownloadType.annualLoad.name() })));

		assertTrue("Monthly Load requested if water quality data type requested with a QW data type filters that include Monthly Load", 
				ServiceParameterUtils.isMonthlyLoadsRequested(Arrays.asList(new String[] { DataType.waterQuality.name() }), Arrays.asList(new String[] { DownloadType.monthlyLoad.name(), DownloadType.discreteQw.name() })));
	}

	@Test
	public void testIsAnnualFlowRequested() {
		assertFalse("Annual Flow not requested if params not provided", ServiceParameterUtils.isAnnualFlowRequested(null, null));
		
		assertTrue("Annual Flow requested if stream flow data type requested without any stream flow type filters", 
				ServiceParameterUtils.isAnnualFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), null));

		assertFalse("Annual Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Annual Flow", 
				ServiceParameterUtils.isAnnualFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.monthlyFlow.name() })));
		
		assertTrue("Annual Flow requested if stream flow data type requested with a stream flow filters that include Annual Flow", 
				ServiceParameterUtils.isAnnualFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.annualFlow.name(), DownloadType.monthlyFlow.name() })));
	}

	@Test
	public void testIsMonthlyFlowRequested() {
		assertFalse("Monthly Flow not requested if params not provided", ServiceParameterUtils.isMonthlyFlowRequested(null, null));
		
		assertTrue("Monthly Flow requested if stream flow data type requested without any stream flow type filters", 
				ServiceParameterUtils.isMonthlyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), null));

		assertFalse("Monthly Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Monthly Flow", 
				ServiceParameterUtils.isMonthlyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.annualFlow.name() })));

		assertTrue("Monthly Flow requested if stream flow data type requested with a stream flow filters that include Monthly Flow", 
				ServiceParameterUtils.isMonthlyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.annualFlow.name(), DownloadType.monthlyFlow.name() })));
	}

	@Test
	public void testIsDailyFlowRequested() {
		assertFalse("Daily Flow not requested if params not provided", ServiceParameterUtils.isDailyFlowRequested(null, null));
		
		assertTrue("Daily Flow requested if stream flow data type requested without any stream flow type filters", 
				ServiceParameterUtils.isDailyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), null));

		assertFalse("Daily Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Daily Flow", 
				ServiceParameterUtils.isDailyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.annualFlow.name() })));

		assertTrue("Daily Flow requested if stream flow data type requested with a stream flow filters that include Daily Flow", 
				ServiceParameterUtils.isDailyFlowRequested(Arrays.asList(new String[] { DataType.streamFlow.name() }), Arrays.asList(new String[] { DownloadType.annualFlow.name(), DownloadType.dailyFlow.name() })));
	}
}
