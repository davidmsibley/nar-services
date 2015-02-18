package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.OrderedFilter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.SortedSet;
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
	
	private void nextFilter() throws XMLStreamException {
		try {
			currentFilteredResultSet.close();
		} catch (SQLException ex) {
			
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
//		if (row != null) {
//			if (this.filter.procedure != null && !this.filter.procedure.equals(currentObservation.metadata().procedure())) {
//				allEqual = false;
//			}
//			if (this.filter.observedProperty != null && !this.filter.observedProperty.equals(currentObservation.metadata().observedProperty())) {
//				allEqual = false;
//			}
//			if (this.filter.featureOfInterest != null && !this.filter.featureOfInterest.equals(currentObservation.metadata().featureOfInterest())) {
//				allEqual = false;
//			}
//		}
		return true;
	}
	
	@Override
	protected TableRow makeNextRow() {
		TableRow row = null;
		try {
			if (currentFilteredResultSet != null && currentFilteredResultSet.next()) {
			
				TableRow inRow = TableRow.buildTableRow(currentFilteredResultSet);
//			
//			Map<Column, String> ob = new HashMap<>();
//			for (Column col : columns) {
//				String attribute = null;
//				if (col.equals(SOSConnector.SOS_DATE_COL)) {
//					attribute = next.time().toString();
//				}
//				else if (col.equals(SOSConnector.SOS_MOD_TYPE_COL)) {
//					attribute = DownloadType.getModTypeFromProcedure(next.metadata().procedure());
//				}
//				else if (col.equals(SOSConnector.SOS_CONSTITUENT_COL)) {
//					attribute = next.metadata().observedProperty();
//				}
//				else if (col.equals(SOSConnector.SOS_SITE_COL)) {
//					attribute = next.metadata().featureOfInterest();
//				}
//				else {
//					attribute = next.value();
//				}
//				ob.put(col, attribute);
//			}
//			
//			row = new TableRow(columns, ob);

			} else {
//			IOUtils.closeQuietly(currentFilteredResultSet);
//			try {
//				currentFilteredResultSet = nextCollection();
//			}
//			catch (XMLStreamException ex) {
//				log.error("Error reading xml stream", ex);
//			}
//			if (currentFilteredResultSet != null) {
//				row = makeNextRow();
//			}
			}
		} catch (SQLException ex) {
			log.debug("Problem with resultset", ex);
		}
		return row;
	}

}
