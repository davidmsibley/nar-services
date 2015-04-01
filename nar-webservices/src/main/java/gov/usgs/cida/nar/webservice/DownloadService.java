package gov.usgs.cida.nar.webservice;

import gov.usgs.cida.nar.service.DownloadServiceParameters;
import static gov.usgs.cida.nar.service.DownloadServiceParameters.*;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nar.service.SiteInformationService;
import gov.usgs.cida.nar.service.SosAggregationService;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.JNDISingleton;
import gov.usgs.cida.nar.util.ServiceParameterUtils;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.naming.NamingException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("download")
public class DownloadService {
	private final static Logger LOG = LoggerFactory.getLogger(DownloadService.class);
	private static final String OBSERVED_PROPERTY_PREFIX = "http://cida.usgs.gov/def/NAR/property/";
	private static final String SOS_URL_JNDI_NAME = "nar.endpoint.sos";
	//This list is needed to mimic the "get all" when no constituents are chosen from the front end
	private static final List<String> CONSTITUENT_LIST = Arrays.asList(
			"NO23", "SSC", "TN", "TP"
			);
	private static final String FLOW_CONSTITUENT = "Q";
	private static final String NEWLINE_CHAR = "\r\n";
	
	@GET
	@Path("/bundle/zip")
	@Produces("application/zip")
	public void downloadZippedBundle(
			@QueryParam(MIME_TYPE_PARAM) final String mimeTypeParam,
			@QueryParam(DATA_TYPE_PARAM) final List<String> dataType,
			@QueryParam(QW_DATA_TYPE_PARAM) final List<String> qwDataType,
			@QueryParam(STREAM_FLOW_TYPE_PARAM) final List<String> streamFlowType,
			@QueryParam(CONSTITUENT_PARAM) final List<String> constituent,
			@QueryParam(SITE_TYPE_PARAM) final List<String> siteType,
			@QueryParam(STATION_ID_PARAM) final List<String> stationId,
			@QueryParam(STATE_PARAM) final List<String> state,
			@QueryParam(START_DATE_PARAM) final String startDateTime,
			@QueryParam(END_DATE_PARAM) final String endDateTime,
			@Context HttpServletResponse response) throws NamingException, IOException {
		
		//TODO validate parameters here (try to throw errors/exceptions before we start zip stream)
		
		LOG.debug("Stream full zipped bundle started");
		
		final MimeType mimeType = MimeType.lookup(mimeTypeParam);
		if (mimeType == null) {
			throw new RuntimeException("mimeType not supported");
		}

		OutputStream output = response.getOutputStream();
		response.addHeader("Content-Disposition", "attachment; filename=\"data.zip\"");
		response.addHeader("Pragma", "public");
		response.addHeader("Cache-Control", "max-age=0");
		
		ZipOutputStream zip = null;
		try {
			
			zip = new ZipOutputStream(output, StandardCharsets.UTF_8);
			addRequestSummaryEntry(zip, buildRequestDescriptionFromParams(mimeType,
					dataType,
					qwDataType,
					streamFlowType,
					constituent,
					siteType,
					stationId,
					state,
					startDateTime,
					endDateTime));
			
			if(ServiceParameterUtils.isSiteInformationRequested(dataType)) {
				addSiteInformationEntry(zip, mimeType,
						siteType,
						stationId,
						state);
			}
			
			if(ServiceParameterUtils.isDiscreteQwRequested(dataType, qwDataType)) {
				addAggregatedSosEntry(zip, 
						DownloadType.sampleConcentrations,
						mimeType,
						dataType,
						qwDataType,
						streamFlowType,
						constituent,
						siteType,
						stationId,
						state,
						startDateTime,
						endDateTime);
			}

			if(ServiceParameterUtils.isAnnualLoadsRequested(dataType, qwDataType)) {
				addAggregatedSosEntry(zip, 
						DownloadType.annualLoad,
						mimeType,
						dataType,
						qwDataType,
						streamFlowType,
						constituent,
						siteType,
						stationId,
						state,
						startDateTime,
						endDateTime);
			}

			if(ServiceParameterUtils.isMayLoadsRequested(dataType, qwDataType, siteType)) {
				//only do these if one of the sites is actually MRB
				if(SiteInformationService.containsMrbSite(
						SiteInformationService.getStationFeatures(siteType,	stationId,state)
						)
				) {
					//include both may load and may flow
					addAggregatedSosEntry(zip, 
							DownloadType.mayLoad,
							mimeType,
							dataType,
							qwDataType,
							streamFlowType,
							constituent,
							siteType,
							stationId,
							state,
							startDateTime,
							endDateTime);
	
					addAggregatedSosEntry(zip, 
							DownloadType.mayFlow,
							mimeType,
							dataType,
							qwDataType,
							streamFlowType,
							constituent,
							siteType,
							stationId,
							state,
							startDateTime,
							endDateTime);
				}
			}

			if(ServiceParameterUtils.isDailyFlowRequested(dataType, streamFlowType)) {
				addAggregatedSosEntry(zip, 
						DownloadType.dailyFlow,
						mimeType,
						dataType,
						qwDataType,
						streamFlowType,
						constituent,
						siteType,
						stationId,
						state,
						startDateTime,
						endDateTime);
			}

			if(ServiceParameterUtils.isAnnualFlowRequested(dataType, streamFlowType)) {
				addAggregatedSosEntry(zip, 
					DownloadType.annualFlow,
					mimeType,
					dataType,
					qwDataType,
					streamFlowType,
					constituent,
					siteType,
					stationId,
					state,
					startDateTime,
					endDateTime);
			}

		} finally {
				IOUtils.closeQuietly(zip);
		}
	}

	private void addSiteInformationEntry(ZipOutputStream zip, 
			final MimeType mimeType,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state
			) throws IOException {
		zip.putNextEntry(new ZipEntry(SiteInformationService.SITE_ATTRIBUTE_OUT_FILENAME + "." + mimeType.getFileSuffix()));
		new SiteInformationService().streamData(zip, 
				mimeType,
				siteType,
				stationId,
				state);
		zip.flush();
		zip.closeEntry();
	}
	
	private void addAggregatedSosEntry(ZipOutputStream zip,
			final DownloadType downloadType,
			final MimeType mimeType,
			final List<String> dataType,
			final List<String> qwDataType,
			final List<String> streamFlowType,
			final List<String> inConstituent,
			final List<String> inSiteType,
			final List<String> inStationId,
			final List<String> state,
			final String startDateTime,
			final String endDateTime) throws IOException {
		zip.putNextEntry(new ZipEntry(downloadType.name() + "." + mimeType.getFileSuffix()));
		
		//default to "All constituents"
		final List<String> constituent;
		if(inConstituent == null || inConstituent.size() <= 0) {
			constituent = CONSTITUENT_LIST;
		} else {
			constituent = inConstituent;
		}
		
		//if the download type is for flow, do not include requested constituents
		List<String> constituentsToUse = new ArrayList<>();
		if(downloadType.equals(DownloadType.annualFlow) 
				|| downloadType.equals(DownloadType.dailyFlow)
				|| downloadType.equals(DownloadType.mayFlow)) {
			constituentsToUse.add(FLOW_CONSTITUENT);
		} else {
			constituentsToUse.addAll(constituent);
		}
		
		//if mayLoad/monthlyFlow, enforce MRB_SITE_TYPE if not selected
		List<String> siteType = new ArrayList<>(inSiteType);
		if(downloadType.equals(DownloadType.mayLoad) || downloadType.equals(DownloadType.mayFlow)) {
			if(!siteType.contains(SiteInformationService.MRB_SITE_TYPE_VAL)) { //ensure MRB site type is requested
				siteType.add(SiteInformationService.MRB_SITE_TYPE_VAL);
			}
		}
		
		String headerText = null;
		if (mimeType == MimeType.CSV || mimeType == MimeType.TAB) {
			headerText = DescriptionLoaderSingleton.getDescription(downloadType.getTitle());
		}
		
		new SosAggregationService(
				downloadType, 
				JNDISingleton.getInstance().getProperty(SOS_URL_JNDI_NAME),
				OBSERVED_PROPERTY_PREFIX,
				SiteInformationService.getStationFeatures(siteType, inStationId, state)
				).streamData(zip, 
					mimeType,
					constituentsToUse,
					startDateTime,
					endDateTime,
					headerText);
		zip.flush();
		zip.closeEntry();
	}

	private void addRequestSummaryEntry(ZipOutputStream zip, String requestDescription) throws IOException {
		zip.putNextEntry(new ZipEntry("request.txt"));
		zip.write(requestDescription.getBytes("UTF-8"));
		zip.closeEntry();
		zip.flush();
	}
	
	private String buildRequestDescriptionFromParams(
			final MimeType mimeType,
			final List<String> dataType,
			final List<String> qwDataType,
			final List<String> streamFlowType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final String startDateTime,
			final String endDateTime) throws IOException {
		StringBuffer sb = new StringBuffer();
		
		//List service criteria
		sb.append("Request criteria provided");
		sb.append(NEWLINE_CHAR);
		prettyPrintParamList(sb, Arrays.asList(mimeType.name()), DownloadServiceParameters.MIME_TYPE_PARAM);
		prettyPrintParamList(sb, dataType, DownloadServiceParameters.DATA_TYPE_PARAM);
		prettyPrintParamList(sb, qwDataType, DownloadServiceParameters.QW_DATA_TYPE_PARAM);
		prettyPrintParamList(sb, streamFlowType, DownloadServiceParameters.STREAM_FLOW_TYPE_PARAM);
		prettyPrintParamList(sb, constituent, DownloadServiceParameters.CONSTITUENT_PARAM);
		prettyPrintParamList(sb, siteType, DownloadServiceParameters.SITE_TYPE_PARAM);
		prettyPrintParamList(sb, stationId, DownloadServiceParameters.STATION_ID_PARAM);
		prettyPrintParamList(sb, state, DownloadServiceParameters.STATE_PARAM);
		prettyPrintParamList(sb, Arrays.asList(startDateTime), DownloadServiceParameters.START_DATE_PARAM);
		prettyPrintParamList(sb, Arrays.asList(endDateTime), DownloadServiceParameters.END_DATE_PARAM);
		sb.append(NEWLINE_CHAR);
		sb.append(NEWLINE_CHAR);
		
		//List data headers which match request
		if(ServiceParameterUtils.isSiteInformationRequested(dataType)) {
			appendDataTypeDescription(sb, DownloadType.siteAttribute);
		}

		if(ServiceParameterUtils.isDiscreteQwRequested(dataType, qwDataType)) {
			appendDataTypeDescription(sb, DownloadType.sampleConcentrations);
		}

		if(ServiceParameterUtils.isAnnualLoadsRequested(dataType, qwDataType)) {
			appendDataTypeDescription(sb, DownloadType.annualLoad);
		}

		if(ServiceParameterUtils.isMayLoadsRequested(dataType, qwDataType, siteType)) {
			//only do these if one of the sites is actually MRB
			if(SiteInformationService.containsMrbSite(
					SiteInformationService.getStationFeatures(siteType,	stationId, state)
					)
			) {
				//include both may
				appendDataTypeDescription(sb, DownloadType.mayLoad);
				appendDataTypeDescription(sb, DownloadType.mayFlow);
			}
		}

		if(ServiceParameterUtils.isDailyFlowRequested(dataType, streamFlowType)) {
			appendDataTypeDescription(sb, DownloadType.dailyFlow);
		}

		if(ServiceParameterUtils.isAnnualFlowRequested(dataType, streamFlowType)) {
			appendDataTypeDescription(sb, DownloadType.annualFlow);
		}
		
		return sb.toString();
	}
	
	private void appendDataTypeDescription(StringBuffer sb, DownloadType t) {
		sb.append(t.getTitle());
		sb.append(NEWLINE_CHAR);
		sb.append(DescriptionLoaderSingleton.getDescription(t.getTitle()));
		sb.append(NEWLINE_CHAR);
		sb.append(NEWLINE_CHAR);
	}
	
	private void prettyPrintParamList(StringBuffer sb, List<String> values, String name) {
		if(values != null && values.size() > 0) {
			sb.append(" - ");
			sb.append(DownloadServiceParameters.valueOf(name).getTitle());
			sb.append(": ");
			for(String v : values) {
				sb.append(v);
				sb.append(", ");
			}
			sb.delete(sb.length() - 2, sb.length());
			sb.append(NEWLINE_CHAR);
		}
	}
}
