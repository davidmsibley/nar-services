package gov.usgs.cida.sos;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.BeanMap;
import org.joda.time.DateTime;
import org.opengis.feature.type.PropertyDescriptor;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class Observation {
	
	public static final String POINT_ELEMENT = "point";
	public static final String TIME_ELEMENT = "time";
	public static final String VALUE_ELEMENT = "value";

	private boolean ready;
	private ObservationMetadata metadata;
	private DateTime time;
	private String value;
	
	public Observation() {
		this.ready = false;
	}
	
	protected boolean isReady() {
		return ready;
	}
	
	protected void setReady(boolean ready) {
		this.ready = ready;
	}
	
	public ObservationMetadata metadata() {
		return this.metadata;
	}
	
	public Observation metadata(ObservationMetadata metadata) {
		this.metadata = metadata;
		return this;
	}
	
	public DateTime time() {
		return this.time;
	}
	
	public Observation time(DateTime time) {
		this.time = time;
		return this;
	}
	
	public String value() {
		return this.value;
	}
	
	public Observation value(String value) {
		this.value = value;
		return this;
	}
	
}
