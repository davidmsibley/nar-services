package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.WFSConnector;
import gov.usgs.cida.nar.service.plan.FixLeadingZeroIdInExcelStepBuilder;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.JNDISingleton;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilterStep;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.filter.transform.ColumnAlias;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.plan.Plan;
import gov.usgs.cida.nude.plan.PlanStep;
import gov.usgs.cida.wfs.HttpComponentsWFSClient;
import gov.usgs.cida.wfs.WFSClientInterface;
import gov.usgs.webservices.framework.basic.MimeType;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.DefaultFeatureCollection;

public class SiteInformationService {
	
	private static final Logger log = Logger.getLogger(SiteInformationService.class);
	
	public static final String SITE_ATTRIBUTE_TITLE = DownloadType.siteAttribute.getTitle();
	public static final String SITE_ATTRIBUTE_OUT_FILENAME = SITE_ATTRIBUTE_TITLE.replaceAll(" ", "_");
	public static final String MRB_SITE_TYPE_VAL = "Mississippi River Basin";
	public static final String MS_SITE_VAL = "MS";
	
	private static final String SITE_INFO_URL_JNDI_NAME = "nar.endpoint.ows";
	private static final String SITE_LAYER_JNDI_NAME = "nar.ows.sitelayer";
	
	private static final String SITE_QW_ID_OUT_COL = "SITE_QW_ID";
	private static final String SITE_QW_NAME_OUT_COL = "SITE_QW_NAME";
	private static final String SITE_FLOW_ID_OUT_COL = "SITE_FLOW_ID";
	private static final String SITE_FLOW_NAME_OUT_COL = "SITE_FLOW_NAME";
	private static final String DA_OUT_COL = "DA";
	private static final String LATITUDE_OUT_COL = "LATITUDE";
	private static final String LONGITUDE_OUT_COL = "LONGITUDE";
	private static final String SITE_TYPE_OUT_COL = "SITE_TYPE";
	
	public void streamData(OutputStream output, 
			final MimeType mimeType,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state) throws IOException {
		
		String wfsUrl = JNDISingleton.getInstance().getProperty(SITE_INFO_URL_JNDI_NAME);
		String siteLayerName = JNDISingleton.getInstance().getProperty(SITE_LAYER_JNDI_NAME);
		
		final WFSConnector wfsConnector = new WFSConnector(wfsUrl, siteLayerName, getFilter(siteType, stationId, state));
		
		List<PlanStep> steps = new LinkedList<>();
		PlanStep connectorStep = new PlanStep() {

			@Override
			public ResultSet runStep(ResultSet rs) {
				return wfsConnector.getResultSet();
			}

			@Override
			public ColumnGrouping getExpectedColumns() {
				return wfsConnector.getExpectedColumns();
			}
		};
		steps.add(connectorStep);
		
		//rename columns to specified headers
		ColumnGrouping originals = connectorStep.getExpectedColumns();
		FilterStep renameFilterStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(SITE_QW_ID_OUT_COL), new ColumnAlias(originals.get(2)))
							.addTransform(new SimpleColumn(SITE_QW_NAME_OUT_COL), new ColumnAlias(originals.get(3)))
							.addTransform(new SimpleColumn(SITE_FLOW_ID_OUT_COL), new ColumnAlias(originals.get(4)))
							.addTransform(new SimpleColumn(SITE_FLOW_NAME_OUT_COL), new ColumnAlias(originals.get(5)))
							.addTransform(new SimpleColumn(DA_OUT_COL), new ColumnAlias(originals.get(6)))
							.addTransform(new SimpleColumn(LATITUDE_OUT_COL), new ColumnAlias(originals.get(7)))
							.addTransform(new SimpleColumn(LONGITUDE_OUT_COL), new ColumnAlias(originals.get(8)))
							.addTransform(new SimpleColumn(SITE_TYPE_OUT_COL), new ColumnAlias(originals.get(9)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameFilterStep);
		steps.add(getLeadingZeroIdExcelFixTransform(steps));
		//remove FID and all old column names
		List<Column> wrapped = renameFilterStep.getExpectedColumns().getColumns();
		ColumnGrouping removed = new ColumnGrouping(wrapped.subList(9, wrapped.size()));
		FilterStep removeFIDFilterStep = new FilterStep(new NudeFilterBuilder(removed)
				.addFilterStage(new FilterStageBuilder(removed)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeFIDFilterStep);
		
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
			if (mimeType == MimeType.CSV || mimeType == MimeType.TAB) { //TODO use NUDE for this header writing
				output.write(DescriptionLoaderSingleton.getDescription(SITE_ATTRIBUTE_TITLE).getBytes());
			}
			StreamResponse.dispatch(sr, new PrintWriter(output));
			output.flush();
		}
	}
	
	private static Filter getFilter(final List<String> siteType,
			final List<String> stationId,
			final List<String> state) {
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2( null );
		List<Filter> filters = new ArrayList<>();
		
		if(stationId != null && stationId.size() > 0) {
			List<Filter> stationIdFilters = new ArrayList<>();
			for(String sid : stationId) {
				stationIdFilters.add(ff.equals(ff.property(WFSConnector.WFS_SITE_ID_COL_NAME), ff.literal(sid)));
			}

			if(stationIdFilters.size() > 0) {
				filters.add(ff.or(stationIdFilters));
			}
		}
		
		if(state != null && state.size() > 0) {
			List<Filter> stateFilters = new ArrayList<>();
			for(String st : state) {
				stateFilters.add(ff.equals(ff.property(WFSConnector.WFS_STATE_COL_NAME), ff.literal(st)));
			}
			if(stateFilters.size() > 0) {
				filters.add(ff.or(stateFilters));
			}
		}
		

		boolean mrbSiteTypeRequested = false; //add a filter if this is true
		if(siteType != null && siteType.size() > 0) {
			List<Filter> siteTypeFilters = new ArrayList<>();
			for(String st : siteType) {
				if(st.equals(MRB_SITE_TYPE_VAL)) { //MRB isn't really a site type, it's a UI flag, create a different filter
					mrbSiteTypeRequested = true;
				} else {
					siteTypeFilters.add(ff.equals(ff.property(WFSConnector.WFS_SITE_TYPE_COL_NAME), ff.literal(st)));
				}
			}
			if(siteTypeFilters.size() > 0) {
				filters.add(ff.or(siteTypeFilters));
			}
		}
		
		if(mrbSiteTypeRequested) {
			filters.add(ff.equals(ff.property(WFSConnector.WFS_MS_SITE_COL_NAME), ff.literal(MS_SITE_VAL)));
		}
		
		if(filters.size() > 0) {
			return ff.and(filters);
		} else {
			return null;
		}
	}
	
	public static List<String> getMrbStationIds() throws IOException {
		return getStationIds(Arrays.asList(MRB_SITE_TYPE_VAL), null, null);
	}
	
	public static List<String> getStationIds(final List<String> siteType,
			final List<String> stationId,
			final List<String> state) throws IOException {
		List<String> stationIds = new ArrayList<>();
		WFSClientInterface client = new HttpComponentsWFSClient();
		try {
			client.setupDatastoreFromEndpoint(JNDISingleton.getInstance().getProperty(SITE_INFO_URL_JNDI_NAME));
		}
		catch (IOException ex) {
			log.error("Could not set up wfs connector", ex);
		}
		try {
			stationIds = getStationIdsFromFeatureCollectoin(getStationFeatures(siteType, stationId, state));
		} finally {
			try {
				client.close();
			} catch (Exception e) {
			}
		}
		
		return stationIds;
	}
	
	public static List<String> getStationIdsFromFeatureCollectoin(SimpleFeatureCollection features) throws IOException {
		List<String> stationIds = new ArrayList<>();
		
		SimpleFeatureIterator iter = features.features();
		while(iter.hasNext()) {
			String site = iter.next().getAttribute(WFSConnector.WFS_SITE_ID_COL_NAME).toString();
			stationIds.add(site);
		}
		
		return stationIds;
	}
	
	public static SimpleFeatureCollection getStationFeatures(final List<String> inSiteType,
			final List<String> stationId,
			final List<String> state) throws IOException {
		
		//copy site types to remove MRB site type 
		List<String> siteType = new ArrayList<>(inSiteType);
		if(siteType.contains(MRB_SITE_TYPE_VAL)) {
			siteType.remove(MRB_SITE_TYPE_VAL);
		}
		
		SimpleFeatureCollection stations = null;
		WFSClientInterface client = new HttpComponentsWFSClient();
		try {
			client.setupDatastoreFromEndpoint(JNDISingleton.getInstance().getProperty(SITE_INFO_URL_JNDI_NAME));
		}
		catch (IOException ex) {
			log.error("Could not set up wfs connector", ex);
		}
		try {
			SimpleFeatureCollection streaming = client.getFeatureCollection(
					JNDISingleton.getInstance().getProperty(SITE_LAYER_JNDI_NAME), 
					getFilter(siteType, stationId, state));
			
			//convert to NON streaming
			DefaultFeatureCollection nonStreamed = new DefaultFeatureCollection();
			
			SimpleFeatureIterator iter = streaming.features();
			while(iter.hasNext()) {
				nonStreamed.add(iter.next());
			}
			
			stations = nonStreamed;
		} finally {
			try {
				client.close();
			} catch (Exception e) {
			}
		}
		
		return stations;
	}
	
	public static String getFlowIdFromQwId(SimpleFeatureCollection features, String qwId) {
		String flowId = null;

		if(features != null) {
			SimpleFeatureIterator iter = features.features();
			while(iter.hasNext()) {
				SimpleFeature siteFeature = iter.next();
				String site = siteFeature.getAttribute(WFSConnector.WFS_SITE_ID_COL_NAME).toString();
				if(site.equals(qwId)) {
					flowId = siteFeature.getAttribute(WFSConnector.WFS_FLOW_ID_COL_NAME).toString();
					break;
				}
			}
		}
		
		return flowId;
	}
	
	public static boolean containsMrbSite(SimpleFeatureCollection features) {
		boolean result = false;
		
		if(features != null) {
			SimpleFeatureIterator iter = features.features();
			while(iter.hasNext()) {
				SimpleFeature siteFeature = iter.next();
				Object attribute = siteFeature.getAttribute(WFSConnector.WFS_MS_SITE_COL_NAME);
				String site = null == attribute ? "" : attribute.toString();
				if(MS_SITE_VAL.equals(site)) {
					result = true;
					break;
				}
			}
		}
		
		return result;
	}
	
	private PlanStep getLeadingZeroIdExcelFixTransform(final List<PlanStep> prevSteps) {
	    return FixLeadingZeroIdInExcelStepBuilder.build(prevSteps, SITE_FLOW_ID_OUT_COL,SITE_QW_ID_OUT_COL);
	}
 }
