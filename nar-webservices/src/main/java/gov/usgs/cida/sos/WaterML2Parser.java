package gov.usgs.cida.sos;

import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WaterML2Parser {
	
	private InputStream observationResults;
	
	public WaterML2Parser(InputStream observationResults) {
		this.observationResults = observationResults;
	}
	
	public ObservationCollection getObservations() throws XMLStreamException {
		InputStreamReader reader = new InputStreamReader(observationResults);
		XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(reader);
		ObservationCollection observationCollection = new ObservationCollection(xmlReader);
		return observationCollection;
	}
	
	public FilteredObservationCollection getFilteredObservations(OrderedFilter filter) throws XMLStreamException {
		InputStreamReader reader = new InputStreamReader(observationResults);
		XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(reader);
		FilteredObservationCollection observationCollection = new FilteredObservationCollection(xmlReader, filter);
		return observationCollection;
	}
	
	public ResultSet parse() throws XMLStreamException {
		ResultSet rs = new IteratorWrappingResultSet(getObservations().tableRowIterator());
		return rs;
	}
	
}
