package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nar.transform.FourDigitYearTransform;
import gov.usgs.cida.nar.transform.PrefixStripTransform;
import gov.usgs.cida.nar.transform.QwIdToFlowIdTransform;
import gov.usgs.cida.nar.transform.RemarkedValueTransform;
import gov.usgs.cida.nar.transform.ToDayDateTransform;
import gov.usgs.cida.nar.transform.ToMonthNumberTransform;
import gov.usgs.cida.nar.transform.WaterYearTransform;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilterStep;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.filter.transform.ColumnAlias;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.plan.Plan;
import gov.usgs.cida.nude.plan.PlanStep;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.sos.DataAvailabilityMember;
import gov.usgs.cida.sos.OrderedFilter;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;


public class SosAggregationService {
	
	private static final Logger log = Logger.getLogger(SosAggregationService.class);
	
	//determines how long we wait to check if SOS connectors are ready
	//a higher wait time also helps with keeping output streams active by slowly streaming out bytes
	private static final int WAIT_TIME_BETWEEN_SOS_REQUESTS = 1000; 

	private static final String DATE_IN_COL = "DATE";
	
	private static final String SITE_QW_ID_IN_COL = "SITE_QW_ID";
	private static final String SITE_FLOW_ID_IN_COL = "SITE_FLOW_ID";

	private static final String QW_CONSTIT_IN_COL = "CONSTIT";
	private static final String QW_CONCENTRATION_IN_COL = "procedure"; 

	private static final String FLOW_IN_COL = "procedure"; 
	
	private static final String AN_MASS_UPPER_95_IN_COL = "annual_mass_upper_95";
	private static final String AN_MASS_LOWER_95_IN_COL = "annual_mass_lower_95";
	private static final String AN_MASS_IN_COL = "annual_mass";
	private static final String AN_YIELD_IN_COL = "annual_yield";
	private static final String AN_CONC_FLOW_WEIGHTED_IN_COL = "annual_concentration_flow_weighted";
	
	private static final String MON_MASS_UPPER_95_IN_COL = "monthly_mass_upper_95";
	private static final String MON_MASS_IN_COL = "monthly_mass";
	private static final String MON_FLOW_IN_COL = "procedure";
	private static final String MON_MASS_LOWER_95_IN_COL= "monthly_mass_lower_95";
	
	private static final String WY_OUT_COL = "WY";
	private static final String FLOW_OUT_COL = "FLOW";

	private static final String QW_CONCENTRATION_OUT_COL = "CONCENTRATION"; 
	private static final String MOD_TYPE_OUT_COL = "MODTYPE"; 
	private static final String REMARK_OUT_COL = "REMARK";

	private static final String AN_MASS_UPPER_95_OUT_COL = "TONS_U95";
	private static final String AN_MASS_LOWER_95_OUT_COL = "TONS_L95";
	private static final String AN_MASS_OUT_COL = "TONS_LOAD";
	private static final String AN_YIELD_OUT_COL = "YIELD";
	private static final String AN_CONC_FLOW_WEIGHTED_OUT_COL = "FWC";

	private static final String MON_MASS_UPPER_95_OUT_COL = "TONS_U95";
	private static final String MON_MASS_OUT_COL = "TONS_LOAD";
	private static final String MON_FLOW_OUT_COL = "FLOW";
	private static final String MON_MASS_LOWER_95_OUT_COL= "TONS_L95";
	
	private static final String MONTH_OUT_COL= "MONTH";
	
	private static final String PROPERTY_PREFIX = "http://cida.usgs.gov/def/NAR/property/";
	
	private DownloadType type;
	private String sosUrl;
	private String observedPropertyPrefix;
	private final SimpleFeatureCollection siteFeatures;
	
	public SosAggregationService(DownloadType type, String sosUrl, String observedPropertyPrefix, SimpleFeatureCollection siteFeatures) {
		this.type = type;
		this.sosUrl = sosUrl;
		this.observedPropertyPrefix = observedPropertyPrefix;
		this.siteFeatures = siteFeatures;
	}
	
	public void streamData(final OutputStream output,
			final MimeType mimeType,
			final List<String> constituent,
			final String startDateTime,
			final String endDateTime,
			final String header) throws IOException {
		
		
		final StringReader headerReader = new StringReader(header);

		final List<SOSConnector> sosConnectors = getSosConnectors(
				sosUrl,
				constituent,
				SiteInformationService.getStationIdsFromFeatureCollectoin(siteFeatures),
				startDateTime,
				endDateTime);
		
		List<PlanStep> steps = new LinkedList<>();
		PlanStep connectorStep;
		connectorStep = new PlanStep() {
			
			@Override
			public ResultSet runStep(ResultSet rs) {
				boolean areAllReady = false;
				List<ResultSet> rsets = new ArrayList<>();
				
				long start = System.currentTimeMillis();
				while (!areAllReady) {
					boolean readyCheck = true;
					int numberReady = 0;
					for (IConnector conn : sosConnectors) {
						boolean connReady = conn.isReady();
						readyCheck = (readyCheck && connReady);
						if (connReady) {
							numberReady++;
						}
					}
					// TODO make sure isReady() will eventually be true
					log.trace(String.format("Streams complete: {} of {}", numberReady, sosConnectors.size()));
					try {
						Thread.sleep(WAIT_TIME_BETWEEN_SOS_REQUESTS);
					}
					catch (InterruptedException ex) {
						log.debug(ex);
					}
					
					if (mimeType == MimeType.CSV || mimeType == MimeType.TAB) { //TODO use NUDE for this header writing
						//write a single byte to keep the stream active
						try {
							int nextByte = headerReader.read();
							if(nextByte > -1) {
								output.write(nextByte);
								output.flush();
							}
						} catch (IOException e) {
							log.debug("Exception writing header fragment", e);
						}
					}
					
					areAllReady = readyCheck;
				}
				
				//Write out what's left of the header now that we aren't waiting for SOS connectors
				if (mimeType == MimeType.CSV || mimeType == MimeType.TAB) { //TODO use NUDE for this header writing
					//write a single byte to keep the stream active
					try {
						int nextByte = headerReader.read();
						while(nextByte > -1) {
							output.write(nextByte);
							output.flush();
							nextByte = headerReader.read();
						}
					} catch (IOException e) {
						log.debug("Exception writing remaining header fragment", e);
					}
				}
				
				for (IConnector conn : sosConnectors) {
					ResultSet resultSet = conn.getResultSet();
					rsets.add(resultSet);
				}
				
				log.debug("***** Time elapsed: " + (System.currentTimeMillis() - start));
				return new MuxResultSet(rsets);
			}

			@Override
			public ColumnGrouping getExpectedColumns() {
				List<ColumnGrouping> cgs = new ArrayList<>();
				for (IConnector conn : sosConnectors) {
					cgs.add(conn.getExpectedColumns());
				}
				return ColumnGrouping.join(cgs);
			}
		};
		
		steps.add(connectorStep);

		//do necessary transformations by download type
		switch(this.type) {
			case annualLoad:
				steps.addAll(getAnnualLoadSteps(steps));
				break;
			case mayLoad:
				steps.addAll(getMayLoadSteps(steps));
				break;
			case annualFlow:
				steps.addAll(getAnnualFlowSteps(steps));
				break;
			case monthlyFlow:
				steps.addAll(getMonthlyFlowSteps(steps));
				break;
			case dailyFlow:
				steps.addAll(getDailyFlowSteps(steps));
				break;
			case sampleConcentrations:
				steps.addAll(getDiscreteQwSteps(steps));
				break;
			default: //nothing
		}
		
		Plan plan = new Plan(steps);
		
		ResultSet runStep = Plan.runPlan(plan);
		TableResponse tr = new TableResponse(runStep);
		StreamResponse sr = null;
		try {
			sr = Dispatcher.buildFormattedResponse(mimeType, tr);
		} catch (IOException| SQLException | XMLStreamException ex) {
			log.error("Unable to build formatted response", ex);
		}
		if (sr != null && output != null) {
			StreamResponse.dispatch(sr, new PrintWriter(output));
			output.flush();
			for (SOSConnector conn : sosConnectors) {
				IOUtils.closeQuietly(conn);
			}
		}
	}
	
	public List<SOSConnector> getSosConnectors(final String sosUrl,
			final List<String> constituent,
			final List<String> stationId,
			final String startDateTime,
			final String endDateTime) {
		List<SOSConnector> sosConnectors = new ArrayList<>();
		
		List<String> actualProperties = new ArrayList<>();
		for(String prop : constituent) {
			actualProperties.add(this.observedPropertyPrefix + prop);
		}
		
		DateTime start = null;
		try {
			start = DateTime.parse(startDateTime, DateTimeFormat.forPattern("MM/dd/yyyy"));
		} catch(Exception e) {
			log.debug(e);
		}
		DateTime end = null;
		try {
			end = DateTime.parse(endDateTime, DateTimeFormat.forPattern("MM/dd/yyyy"));
		} catch(Exception e) {
			log.debug(e);
		}
		
		Map<String, List<String>> columnMap = new HashMap<>();
		
		for(String procedure : this.type.getProcedures()) {
			String columnName = DownloadType.getColumnNameFromProcedure(procedure);
			if (columnMap.containsKey(columnName)) {
				List<String> procList = columnMap.get(columnName);
				procList.add(procedure);
			} else {
				List<String> procList = new LinkedList<>();
				procList.add(procedure);
				columnMap.put(columnName, procList);
			}
		}
		
		for (String columnName : columnMap.keySet()) {
			List<String> procList = columnMap.get(columnName);
			SortedSet<OrderedFilter> filters = new TreeSet<>();

			SOSClient gdaClient = new SOSClient(sosUrl, null, null, actualProperties, procList, null);
			List<DataAvailabilityMember> dataAvailityMembers = gdaClient.getDataAvailability();
			gdaClient.close();
			
			List<String> filteredStations = filterStationsByDataAvailibility(dataAvailityMembers, stationId);
			SOSClient sosClient = new SOSClient(sosUrl, start, end, actualProperties, procList, filteredStations);
			for (String procedure : procList) {
				for (String prop : actualProperties) {
					for (String featureOfInterest : filteredStations) {
						filters.add(new OrderedFilter(procedure, prop, featureOfInterest));
					}
				}
			}
			final SOSConnector sosConnector = new SOSConnector(sosClient, filters, columnName);
			sosConnectors.add(sosConnector);
		}
		return sosConnectors;
	}
	
	private List<String> filterStationsByDataAvailibility(List<DataAvailabilityMember> dataAvailityMembers, final List<String> stationIds) {
		List<String> filteredStationIds = new ArrayList<>();

		if(stationIds != null) {
			//filter features down to only those available
			for(String staid : stationIds) {
				for(DataAvailabilityMember da : dataAvailityMembers) {
					if(da.getFeatureOfInterest().equals(staid)) {
						if(!filteredStationIds.contains(staid)) {
							filteredStationIds.add(staid);
						}
					}
				}
			}
		}
		
		return filteredStationIds;
	}
	
	private List<PlanStep> getAnnualLoadSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(AN_CONC_FLOW_WEIGHTED_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), AN_CONC_FLOW_WEIGHTED_IN_COL) + 1)))
							.addTransform(new SimpleColumn(AN_MASS_LOWER_95_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), AN_MASS_LOWER_95_IN_COL) + 1)))
							.addTransform(new SimpleColumn(AN_MASS_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), AN_MASS_IN_COL) + 1)))
							.addTransform(new SimpleColumn(AN_MASS_UPPER_95_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), AN_MASS_UPPER_95_IN_COL) + 1)))
							.addTransform(new SimpleColumn(AN_YIELD_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), AN_YIELD_IN_COL) + 1)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);
		
		//drop columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, QW_CONSTIT_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MOD_TYPE_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, AN_MASS_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, AN_MASS_LOWER_95_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, AN_MASS_UPPER_95_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, AN_CONC_FLOW_WEIGHTED_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, AN_YIELD_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert date to WY
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));
		
		//Strip out the constituent prefix
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), 
						new PrefixStripTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), PROPERTY_PREFIX))
				.buildFilterStage())
		.buildFilter()));
		
		return steps;
	}
	
	private List<PlanStep> getMayLoadSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();

		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MONTH_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MON_MASS_LOWER_95_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), MON_MASS_LOWER_95_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MON_MASS_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), MON_MASS_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MON_MASS_UPPER_95_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), MON_MASS_UPPER_95_IN_COL) + 1)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);
		
		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, QW_CONSTIT_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MONTH_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MOD_TYPE_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MON_MASS_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MON_MASS_LOWER_95_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MON_MASS_UPPER_95_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert date to WY
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, MONTH_OUT_COL)), new ToMonthNumberTransform(finalColList.get(indexOfCol(finalColList, MONTH_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));
		
		//Strip out the constituent prefix
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), 
						new PrefixStripTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), PROPERTY_PREFIX))
				.buildFilterStage())
		.buildFilter()));
		
		return steps;
	}

	private List<PlanStep> getAnnualFlowSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(FLOW_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), FLOW_IN_COL) + 1)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);

		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, FLOW_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);

		//convert date to WY
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));

		return steps;
	}

	private List<PlanStep> getMonthlyFlowSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MONTH_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.addTransform(new SimpleColumn(MON_FLOW_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), MON_FLOW_IN_COL) + 1)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);

		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MONTH_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, MON_FLOW_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);

		//convert date to WY
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new FourDigitYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, MONTH_OUT_COL)), new ToMonthNumberTransform(finalColList.get(indexOfCol(finalColList, MONTH_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));

		return steps;
	}
	
	private List<PlanStep> getDailyFlowSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
							.addTransform(new SimpleColumn(FLOW_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), FLOW_IN_COL) + 1)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);
		
		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, DATE_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, FLOW_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert dates to WY and day
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL)), new ToDayDateTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));

		return steps;
	}

	private List<PlanStep> getDiscreteQwSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		//rename columns to specified headers
		//Not sure if any renaming is necessary until missing columns are available 
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
			.addFilterStage(new FilterStageBuilder(originals)
			.addTransform(new SimpleColumn(SITE_FLOW_ID_IN_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), SITE_QW_ID_IN_COL) + 1)))
			.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), DATE_IN_COL) + 1)))
			.addTransform(new SimpleColumn(QW_CONCENTRATION_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), QW_CONCENTRATION_IN_COL) + 1)))
			.addTransform(new SimpleColumn(REMARK_OUT_COL), new ColumnAlias(originals.get(indexOfCol(originals.getColumns(), QW_CONCENTRATION_IN_COL) + 1)))
			.buildFilterStage())
			.buildFilter());
		steps.add(renameColsStep);
		
		//missing cols commented out until available 
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_FLOW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, QW_CONSTIT_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, DATE_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, QW_CONCENTRATION_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, REMARK_OUT_COL)));
		//TODO NEED REMARK
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert dates to WY and day
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), new QwIdToFlowIdTransform(finalColList.get(indexOfCol(finalColList, SITE_FLOW_ID_IN_COL)), siteFeatures))
				.addTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL)), new ToDayDateTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));
		
		//Strip out the constituent prefix
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), 
						new PrefixStripTransform(finalColList.get(indexOfCol(finalColList, QW_CONSTIT_IN_COL)), PROPERTY_PREFIX))
				.buildFilterStage())
		.buildFilter()));
		
		//Strip out remark from value field and value from remark field
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, QW_CONCENTRATION_OUT_COL)), 
						new RemarkedValueTransform(finalColList.get(indexOfCol(finalColList, QW_CONCENTRATION_OUT_COL)), false))
				.addTransform(finalColList.get(indexOfCol(finalColList, REMARK_OUT_COL)), 
						new RemarkedValueTransform(finalColList.get(indexOfCol(finalColList, REMARK_OUT_COL)), true))
				.buildFilterStage())
		.buildFilter()));
		
		return steps;
	}
	
	/**
	 * Helper function to get the index of a column with the given name
	 */
	private int indexOfCol(List<Column> cols, String colName) {
		int index = -1;
		for(int i = 0; i < cols.size(); i++) {
			if(cols.get(i).getName().equals(colName)) {
				index = i;
				break;
			}
		}
		return index;
	}
}
