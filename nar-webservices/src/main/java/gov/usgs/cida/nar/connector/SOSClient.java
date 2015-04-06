package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.CachedResultSet;
import gov.usgs.cida.nar.util.Profiler;
import gov.usgs.cida.sos.DataAvailabilityMember;
import gov.usgs.cida.sos.EndOfXmlStreamException;
import gov.usgs.cida.sos.WaterML2Parser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSClient extends Thread implements AutoCloseable {
	
	private static final Logger log = LoggerFactory.getLogger(SOSClient.class);
	private static final int MAX_CONNECTIONS = 8;
	private transient static final AtomicInteger numConnections;
	
	static {
		numConnections = new AtomicInteger();
	}
	
	private final File file;
	private final String sosEndpoint;
	private final DateTime startTime;
	private final DateTime endTime;
	private final List<String> observedProperties;
	private final List<String> procedures;
	private final List<String> featuresOfInterest;
	private boolean fetched = false;

	public SOSClient(String sosEndpoint, DateTime startTime, DateTime endTime, List<String> observedProperties,
			List<String> procedures, List<String> featuresOfInterest) {
		UUID randomUUID = UUID.randomUUID();
		this.file = FileUtils.getFile(FileUtils.getTempDirectory(), randomUUID.toString() + ".xml");
		log.debug("SOSClient on {}", randomUUID.toString());
		this.sosEndpoint = sosEndpoint;
		this.startTime = startTime;
		this.endTime = endTime;
		this.observedProperties = observedProperties;
		this.procedures = procedures;
		this.featuresOfInterest = featuresOfInterest;
	}

	@Override
	public void run() {
		this.fetchData();
	}
	
	@Override
	public void close() {
//		FileUtils.deleteQuietly(file);
	}
	
	public List<DataAvailabilityMember> getDataAvailability() {
		log.debug("Getting DataAvailability {}", this.file.getName());
		List<DataAvailabilityMember> dataAvailabilityMembers = null;
		
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 10000);
		clientConfig.property(ClientProperties.READ_TIMEOUT, 60000);
		Client client = ClientBuilder.newClient(clientConfig);

		InputStream returnStream = null;
		try {
			Response response = client.target(this.sosEndpoint)
				.path("")
				.request(new MediaType[]{MediaType.APPLICATION_XML_TYPE})
				.post(buildGetDataAvailability(observedProperties, procedures));
			returnStream = response.readEntity(InputStream.class);
			dataAvailabilityMembers = DataAvailabilityMember.buildListFromXmlInputStream(returnStream);
		} catch (EndOfXmlStreamException | XMLStreamException | FactoryConfigurationError e) {
			log.debug("error parsing GetDataAvailability response", e);
		} finally {
			IOUtils.closeQuietly(returnStream);
		}
		return dataAvailabilityMembers;
	}
	
	public ResultSet readFile() {
		CachedResultSet result = null;
		try {
			result = new CachedResultSet(this.file);
		} catch (IOException ex) {
			log.debug("Error retrieving cached dataset", ex);
		}
		return result;
	}

	private synchronized void fetchData() {
		log.debug("Fetching {}", this.file.getName());
		if (fetched) {
			return;
		}
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 10000);
		clientConfig.property(ClientProperties.READ_TIMEOUT, 180000);
		Client client = ClientBuilder.newClient(clientConfig);
		
		InputStream returnStream = null;
		try {
			while (numConnections.get() >= MAX_CONNECTIONS) {
				try {
					sleep(250);
				}
				catch (InterruptedException ex) {
					log.debug("interrupted", ex);
				}
			}
			int numCon = numConnections.incrementAndGet();
			log.debug("incremented active connections: {}", numCon);
			UUID timerId = Profiler.startTimer();
			Response response = client.target(this.sosEndpoint)
				.path("")
				.request(new MediaType[]{MediaType.APPLICATION_XML_TYPE})
				.post(buildGetObservationRequest(startTime, endTime, observedProperties, procedures, featuresOfInterest));
			returnStream = response.readEntity(InputStream.class);
			WaterML2Parser parser = new WaterML2Parser(returnStream);
			long sosTime = Profiler.stopTimer(timerId);
			Profiler.log.debug("SOS GetObservations took {} milliseconds", sosTime);
			
			timerId = Profiler.startTimer();
			ResultSet parse = parser.parse();
			CachedResultSet.serialize(parse, this.file);
			long parseTime = Profiler.stopTimer(timerId);
			Profiler.log.debug("Parsing SOS took {} milliseconds", parseTime);
		} catch (IOException | XMLStreamException | SQLException ex) {
			log.error("Unable to get data from service", ex);
		} finally {
			int numCon = numConnections.decrementAndGet();
			log.debug("decremented active connections: {}", numCon);
			IOUtils.closeQuietly(returnStream);
			fetched = true;
		}
	}
	
	private static Entity buildGetObservationRequest(DateTime startTime, DateTime endTime, List<String> observedProperties,
			List<String> procedures, List<String> featuresOfInterest) {
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
				.append("<sos:GetObservation service=\"SOS\" version=\"2.0.0\" ")
				.append("xmlns:sos=\"http://www.opengis.net/sos/2.0\" ")
				.append("xmlns:fes=\"http://www.opengis.net/fes/2.0\" ")
				.append("xmlns:gml=\"http://www.opengis.net/gml/3.2\" ")
				.append("xmlns:swe=\"http://www.opengis.net/swe/2.0\" ")
				.append("xmlns:xlink=\"http://www.w3.org/1999/xlink\" ")
				.append("xmlns:swes=\"http://www.opengis.net/swes/2.0\" ")
				.append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/sos/2.0 http://schemas.opengis.net/sos/2.0/sos.xsd\">")
				;
		for (String proc : procedures) {
			builder.append("<sos:procedure>" + proc + "</sos:procedure>");
		}
		for (String obsProp : observedProperties) {
			builder.append("<sos:observedProperty>" + obsProp + "</sos:observedProperty>");
		}
		if (startTime != null || endTime != null) {
			builder.append("<sos:temporalFilter>")
					.append("<fes:During>")
					.append("<fes:ValueReference>phenomenonTime</fes:ValueReference>")
					.append("<gml:TimePeriod gml:id=\"tp_1\">");


			if(startTime != null) {
				builder.append("<gml:beginPosition>" + startTime.toString() + "</gml:beginPosition>");
			}
			if(endTime != null){
				builder.append("<gml:endPosition>" + endTime.toString() + "</gml:endPosition>");
			}

			builder.append("</gml:TimePeriod>")
					.append("</fes:During>")
					.append("</sos:temporalFilter>");
		}
		for (String feature : featuresOfInterest) {
			builder.append("<sos:featureOfInterest>" + feature + "</sos:featureOfInterest>");
		}
		builder.append("<sos:responseFormat>http://www.opengis.net/waterml/2.0</sos:responseFormat>")
				.append("</sos:GetObservation>");
		return Entity.xml(builder.toString());
	}
	
	private static Entity buildGetDataAvailability(List<String> observedProperties,
			List<String> procedures) {
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
			.append("<gda:GetDataAvailability service=\"SOS\" version=\"2.0.0\" ")
		    .append("xmlns:gda=\"http://www.opengis.net/sosgda/1.0\" ")
		    .append("xmlns:swes=\"http://www.opengis.net/swes/2.0\" ")
		    .append("xmlns:fes=\"http://www.opengis.net/fes/2.0\" ")
		    .append("xmlns:gml=\"http://www.opengis.net/gml/3.2\" ")
		    .append("xmlns:swe=\"http://www.opengis.net/swe/2.0\">");
		
		for (String proc : procedures) {
			builder.append("<gda:procedure>" + proc + "</gda:procedure>");
		}
		for (String obsProp : observedProperties) {
			builder.append("<gda:observedProperty>" + obsProp + "</gda:observedProperty>");
		}
		
		builder.append("</gda:GetDataAvailability>");
		return Entity.xml(builder.toString());
	}
	
}
