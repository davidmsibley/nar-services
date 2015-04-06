package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.Observation;
import gov.usgs.cida.sos.ObservationMetadata;
import gov.usgs.cida.sos.OrderedFilter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSResultSet extends OGCResultSet {
	
	private static final Logger log = LoggerFactory.getLogger(SOSResultSet.class);
	
	private static final Column PROCEDURE_IN_COL = new SimpleColumn(ObservationMetadata.PROCEDURE_ELEMENT);
	private static final Column OBSERVED_PROPERTY_IN_COL = new SimpleColumn(ObservationMetadata.OBSERVED_PROPERTY_ELEMENT);
	private static final Column FEATURE_OF_INTEREST_IN_COL = new SimpleColumn(ObservationMetadata.FEATURE_OF_INTEREST_ELEMENT);
	
	private final String uuid = UUID.randomUUID().toString().substring(0, 5);
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
	
	private boolean nextFilter() {
		boolean hasNext = false;
		try {
			if (currentFilteredResultSet != null) {
				currentFilteredResultSet.close();
			}
		} catch (SQLException ex) {
			log.debug("Exception closing result set", ex);
		}
		this.currentFilteredResultSet = this.client.readFile();
		
		if (filters.size() > 0) {
			OrderedFilter first = filters.first();
			filters.remove(first);
			currentFilter = first;
			hasNext = true;
		}
		return hasNext;
	}
	
	// TODO add row filtering to NUDE
	private boolean filter(TableRow row) {
		boolean allEqual = true;
		if (row != null) {
			if (this.currentFilter.procedure != null &&
					!this.currentFilter.procedure.equals(row.getValue(PROCEDURE_IN_COL))) {
				allEqual = false;
			}
			if (this.currentFilter.observedProperty != null &&
					!this.currentFilter.observedProperty.equals(row.getValue(OBSERVED_PROPERTY_IN_COL))) {
				allEqual = false;
			}
			if (this.currentFilter.featureOfInterest != null &&
					!this.currentFilter.featureOfInterest.equals(row.getValue(FEATURE_OF_INTEREST_IN_COL))) {
				allEqual = false;
			}
		}
		return allEqual;
	}
	
	@Override
	protected TableRow makeNextRow() {
		TableRow row = null;
		try {
			TableRow inRow = null;
			boolean hasFilter = true;
			while (row == null && hasFilter) {
				if (currentFilteredResultSet == null || currentFilteredResultSet.isAfterLast()) {
					hasFilter = nextFilter();
				}

				while (row == null && hasFilter && currentFilteredResultSet.next()) {
					inRow = TableRow.buildTableRow(currentFilteredResultSet);
					if (filter(inRow)) {
						log.debug("  filtered {} {} {} {} {}", inRow.getValue(new SimpleColumn(Observation.TIME_ELEMENT)), this.uuid, this.filters.size(), inRow.getValue(new SimpleColumn(ObservationMetadata.PROCEDURE_ELEMENT)), inRow.getValue(new SimpleColumn(ObservationMetadata.OBSERVED_PROPERTY_ELEMENT)));
						Map<Column, String> resultMap = new HashMap<>();
						for (Column col : columns) {
							String attribute = null;
							if (col.equals(SOSConnector.SOS_DATE_COL)) {
								attribute = inRow.getValue(new SimpleColumn(Observation.TIME_ELEMENT));
							}
							else if (col.equals(SOSConnector.SOS_MOD_TYPE_COL)) {
								String procedure = inRow.getValue(new SimpleColumn(ObservationMetadata.PROCEDURE_ELEMENT));
								attribute = DownloadType.getModTypeFromProcedure(procedure);
							}
							else if (col.equals(SOSConnector.SOS_CONSTITUENT_COL)) {
								attribute = inRow.getValue(new SimpleColumn(ObservationMetadata.OBSERVED_PROPERTY_ELEMENT));
							}
							else if (col.equals(SOSConnector.SOS_SITE_COL)) {
								attribute = inRow.getValue(new SimpleColumn(ObservationMetadata.FEATURE_OF_INTEREST_ELEMENT));
							}
							else {
								attribute = inRow.getValue(new SimpleColumn(Observation.VALUE_ELEMENT));
							}
							resultMap.put(col, attribute);
						}
						row = new TableRow(columns, resultMap);
					} else {
						log.debug("unfiltered {} {} {} {} {}", inRow.getValue(new SimpleColumn(Observation.TIME_ELEMENT)), this.uuid, this.filters.size(), inRow.getValue(new SimpleColumn(ObservationMetadata.PROCEDURE_ELEMENT)), inRow.getValue(new SimpleColumn(ObservationMetadata.OBSERVED_PROPERTY_ELEMENT)));
					}
				}
				
			}
		} catch (SQLException ex) {
			log.debug("Problem with resultset", ex);
		}
		return row;
	}

}
