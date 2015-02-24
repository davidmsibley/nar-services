package gov.usgs.cida.sos;

import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.nude.time.DateRange;
import gov.usgs.cida.nude.util.NudeUtils;
import java.io.Closeable;
import java.util.Iterator;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.commons.lang.StringEscapeUtils;
import org.geotools.xlink.XLINK;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a WaterML 2.0 timeseries into an observation Collection
 * 
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class ObservationCollection implements Iterable, Iterator, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(ObservationCollection.class);

	private boolean inUse;
	private XMLStreamReader reader;
	protected Observation currentObservation;
	protected ObservationMetadata sharedMetadata;
	
	public ObservationCollection(XMLStreamReader reader) {
		this.reader = reader;
		this.inUse = false;
	}
	
	@Override
	public Iterator<Observation> iterator() {
		if (inUse) {
			throw new RuntimeException("This is a single use iterator");
		}
		inUse = true;
		return this;
	}
	
	public Iterator<TableRow> tableRowIterator() {
		final ObservationCollection parent = this;
		return new Iterator<TableRow>() {

			@Override
			public boolean hasNext() {
				return parent.hasNext();
			}

			@Override
			public TableRow next() {
				return NudeUtils.makeTableRow(parent.next());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Read-only");
			}
		};
	}

	@Override
	public boolean hasNext() {
		boolean hasNext = false;
		if (currentObservation != null && currentObservation.isReady()) {
			hasNext = true;
		} else {
			try {
				currentObservation = gatherObservation();
				hasNext = (currentObservation != null) ? true : false;
			} catch (XMLStreamException ex) {
				log.error("Problem with xml stream", ex);
			} catch (EndOfXmlStreamException ex) {
				hasNext = false;
			}
		}
		
		return hasNext;
	}

	@Override
	public Observation next() {
		if (hasNext()) {
			// About to return this, don't use again
			currentObservation.setReady(false);
			return currentObservation;
		} else {
			throw new IllegalStateException("There is no next value, you should have called hasNext() first");
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("This is a read-only iterator");
	}
	
	protected Observation gatherObservation() throws XMLStreamException, EndOfXmlStreamException {
		Observation ob = null;
		
		while (this.reader.hasNext() && (ob == null || !ob.isReady())) {
			switch (reader.next()) {
				case XMLStreamConstants.START_ELEMENT:
					// Start metadata collection
					if (isElement(ObservationMetadata.CLEAR_METADATA_ELEMENT)) {
						this.sharedMetadata = new ObservationMetadata();
					}
					if (isElement(ObservationMetadata.TIME_PERIOD_ELEMENT)) {
						if (this.sharedMetadata != null) {
							this.sharedMetadata.timePeriod(gatherDateRange());
						}
					}
					
					if (isElement(ObservationMetadata.PROCEDURE_ELEMENT)) {
						if (this.sharedMetadata != null) {
							this.sharedMetadata.procedure(getHrefAttribute());
						}
					}
					if (isElement(ObservationMetadata.OBSERVED_PROPERTY_ELEMENT)) {
						if (this.sharedMetadata != null) {
							this.sharedMetadata.observedProperty(getHrefAttribute());
						}
					}
					if (isElement(ObservationMetadata.FEATURE_OF_INTEREST_ELEMENT)) {
						if (this.sharedMetadata != null) {
							this.sharedMetadata.featureOfInterest(getHrefAttribute());
						}
					}
					if (isElement(ObservationMetadata.UOM_ELEMENT)) {
						if (this.sharedMetadata != null) {
							this.sharedMetadata.defaultUnits(
									reader.getAttributeValue(null, ObservationMetadata.CODE_ATTRIBUTE));
						}
					}
					// End metadata collection
					if (isElement(Observation.POINT_ELEMENT)) {
						ob = new Observation().metadata(this.sharedMetadata);
					}
					if (isElement(Observation.TIME_ELEMENT)) {
						if (ob != null) {
							ob.time(new DateTime(reader.getElementText()));
						}
					}
					if (isElement(Observation.VALUE_ELEMENT)) {
						if (ob != null) {
							String elementText = reader.getElementText();
							ob.value(StringEscapeUtils.unescapeXml(elementText));
						}
					}
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (isElement(ObservationMetadata.CLEAR_METADATA_ELEMENT)) {
						this.sharedMetadata = null;
					}
					
					if (isElement(Observation.POINT_ELEMENT)) {
						// IMPORTANT: this is what triggers the return
						if (ob != null) {
							ob.setReady(true);
						}
					}
					break;
				case XMLStreamConstants.END_DOCUMENT:
					throw new EndOfXmlStreamException();
			}
		}
		return ob;
	}
	
	/**
	 * Returns true if current XML element is equal to that passed in
	 * @param elementName name to check for equality with element
	 * @return true if equal
	 */
	private boolean isElement(String elementName) {
		boolean isElement = false;
		if (elementName != null) {
			isElement = elementName.equals(reader.getLocalName());
		} 
		return isElement;
	}
	
	private String getHrefAttribute() {
		return reader.getAttributeValue(XLINK.HREF.getNamespaceURI(), XLINK.HREF.getLocalPart());
	}
	
	private DateRange gatherDateRange() throws XMLStreamException {
		DateRange range = null;
		DateTime startTime = null;
		DateTime endTime = null;
		
		while (this.reader.hasNext() && range == null) {
			switch (reader.next()) {
				case XMLStreamConstants.START_ELEMENT:
					if (isElement(ObservationMetadata.BEGIN_POSITION_ELEMENT)) {
						startTime = new DateTime(reader.getElementText());
					}
					if (isElement(ObservationMetadata.END_POSITION_ELEMENT)) {
						endTime = new DateTime(reader.getElementText());
					}
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (isElement(ObservationMetadata.TIME_PERIOD_ELEMENT)) {
						range = new DateRange(startTime, endTime);
					}
					break;
			}
		}
		return range;
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (NullPointerException | XMLStreamException ex) {
			log.debug("Unable to close xml stream", ex);
		}
	}
	
}
