package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.DataAvailabilityMember;
import gov.usgs.cida.sos.DataAvailabilityMetadata;
import gov.usgs.cida.sos.OrderedFilter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.SortedSet;
import java.util.logging.Level;
import javax.xml.stream.XMLStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSResultSet extends OGCResultSet {
	
	private static final Logger log = LoggerFactory.getLogger(SOSResultSet.class);
	
	private SortedSet<OrderedFilter> filters;
	private SOSClient client;
	private ResultSet currentFilteredResultSet;
	private OrderedFilter currentFilter;

	public SOSResultSet(SortedSet<OrderedFilter> filters, SOSClient client, ColumnGrouping colGroups) {
		this.filters = filters;
		this.client = client;
		this.columns = colGroups;
	}

	@Override
	public void close() throws SQLException {
		currentFilteredResultSet.close();
		super.close();
	}
	
	private void nextFilter() {
		try {
			currentFilteredResultSet.close();
		} catch (SQLException ex) {
			log.debug("Exception closing result set", ex);
		}
		this.currentFilteredResultSet = this.client.readFile();
		
		if (filters.size() > 0) {
			OrderedFilter first = filters.first();
			filters.remove(first);
			currentFilter = first;
		}
	}
	
	// TODO add row filtering to NUDE
	private boolean filter(TableRow row) {
		boolean allEqual = true;
		if (row != null) {
			if (this.currentFilter.procedure != null &&
					!this.currentFilter.procedure.equals(row.getValue(new SimpleColumn(DataAvailabilityMetadata.PROCEDURE_ELEMENT)))) {
				allEqual = false;
			}
			if (this.currentFilter.observedProperty != null &&
					!this.currentFilter.observedProperty.equals(row.getValue(new SimpleColumn(DataAvailabilityMetadata.OBSERVED_PROPERTY_ELEMENT)))) {
				allEqual = false;
			}
			if (this.currentFilter.featureOfInterest != null &&
					!this.currentFilter.featureOfInterest.equals(row.getValue(new SimpleColumn(DataAvailabilityMetadata.FEATURE_OF_INTEREST_ELEMENT)))) {
				allEqual = false;
			}
		}
		return allEqual;
	}
	
	@Override
	protected TableRow makeNextRow() {
		TableRow row = null;
		try {
			if (currentFilteredResultSet != null && currentFilteredResultSet.next()) {
				TableRow inRow = TableRow.buildTableRow(currentFilteredResultSet);
				if (filter(inRow)) {
					row = inRow;
				}
			} else {
				nextFilter();
			}
		} catch (SQLException ex) {
			log.debug("Problem with resultset", ex);
		}
		return row;
	}

}
