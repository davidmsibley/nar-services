package gov.usgs.cida.sos;

/**
 * This is a simple wrapper around a getObservation request.
 * It is called ordered, because when used in a list it is important to order it
 * based on the grouping of the parameters desired.  This is probably not very
 * re-usable, so use ObservationCollection most of the time.
 * 
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class OrderedFilter implements Comparable<OrderedFilter>{
	
	public final String procedure;
	public final String observedProperty;
	public final String featureOfInterest;
	
	public OrderedFilter(String procedure, String observedProperty, String featureOfInterest) {
		this.procedure = procedure;
		this.observedProperty = observedProperty;
		this.featureOfInterest = featureOfInterest;
	}

	@Override
	public int compareTo(OrderedFilter o) {
		if (featureOfInterest.equals(o.featureOfInterest)) {
			if (observedProperty.equals(o.observedProperty)) {
				return procedure.compareTo(o.procedure);
			} else {
				return observedProperty.compareTo(o.observedProperty);
			}
		} else {
			return featureOfInterest.compareTo(o.featureOfInterest);
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof OrderedFilter) {
			return (this.compareTo((OrderedFilter)o) == 0);
		}
		return false;
	}

}
