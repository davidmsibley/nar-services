package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.SOSResultSet;
import gov.usgs.cida.nar.transform.PlainStringNumberTransform;
import gov.usgs.cida.nar.util.Profiler;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.filter.FilterStage;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilteredResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.OrderedFilter;
import java.io.Closeable;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSConnector implements IConnector, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(SOSConnector.class);

	public static final Column COMPOSITE_KEY_COL = new SimpleColumn("COMPOSITE", false);
	public static final Column SOS_SITE_COL = new SimpleColumn("SITE_QW_ID");
	public static final Column SOS_CONSTITUENT_COL = new SimpleColumn("CONSTIT");
	public static final Column SOS_MOD_TYPE_COL = new SimpleColumn("MODTYPE");
	public static final Column SOS_DATE_COL = new SimpleColumn("DATE");
	
	private static final String NUMERIC_SUFFIX = "_NUMERIC";
	
	private final SOSClient client;
	private final ColumnGrouping cg;
	private final SortedSet<OrderedFilter> filters;
	private final Column valueColumn;
	
	private boolean isReady;

	private UUID timer;

	public SOSConnector(SOSClient client, SortedSet<OrderedFilter> filters, String valueColumnName) {
		this.client = client;
		this.filters = filters;
		this.isReady = false;
		this.valueColumn = new SimpleColumn(valueColumnName);
		this.cg = makeColumnGrouping(valueColumn);
	}

	@Override
	public void addInput(ResultSet in) {
		return;
	}

	@Override
	public String getStatement() {
		return null;
	}

	@Override
	public ResultSet getResultSet() {
		ResultSet resultSet = null;
		
		if (!isReady()) {
			throw new RuntimeException("Not ready yet");
		}
		resultSet = new SOSResultSet(this.filters, this.client, this.cg);
		
		FilterStage makeNumericColumnsStage = new FilterStageBuilder(getExpectedColumns())
				.addTransform(new SimpleColumn(SOS_SITE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_SITE_COL);
						long orderedVal = value.length() * Long.parseLong(value);
						String stringVal = String.valueOf(orderedVal);
						return stringVal;
					}
				})
				.addTransform(new SimpleColumn(SOS_CONSTITUENT_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_CONSTITUENT_COL);
						String orderedVal = "" + Math.abs(value.hashCode());
						return StringUtils.leftPad(orderedVal.substring(0, 4), 4, "0");
					}
				})
				.addTransform(new SimpleColumn(SOS_MOD_TYPE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_MOD_TYPE_COL);
						String orderedVal = "" + Math.abs(value.hashCode());
						return StringUtils.leftPad(orderedVal.substring(0, 4), 4, "0");
					}
				})
				.addTransform(new SimpleColumn(SOS_DATE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_DATE_COL);
						DateTime parsed = DateTime.parse(value);
						int padTo = String.valueOf(DateTime.now().getMillis()/1000).length();
						return StringUtils.leftPad(String.valueOf(parsed.getMillis()/1000), padTo, "0");
					}
				})
				.buildFilterStage();
		ResultSet numericColumnResultSet = new FilteredResultSet(resultSet, makeNumericColumnsStage);
		
		FilterStage makePrimaryKeyStage = new FilterStageBuilder(makeNumericColumnsStage.getOutputColumns())
				.addTransform(COMPOSITE_KEY_COL, new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String siteVal = row.getValue(new SimpleColumn(SOS_SITE_COL.getName() + NUMERIC_SUFFIX));
						String constituentVal = row.getValue(new SimpleColumn(SOS_CONSTITUENT_COL.getName() + NUMERIC_SUFFIX));
						String modtypeVal = row.getValue(new SimpleColumn(SOS_MOD_TYPE_COL.getName() + NUMERIC_SUFFIX));
						String dateVal = row.getValue(new SimpleColumn(SOS_DATE_COL.getName() + NUMERIC_SUFFIX));
						String compositeVal = siteVal + constituentVal + modtypeVal + dateVal;
						return compositeVal;
					}
				})
				.buildFilterStage();
		ResultSet numericPrimaryKeyResultSet = new FilteredResultSet(numericColumnResultSet, makePrimaryKeyStage);
		
		FilterStage fixScientificNotationStage = new FilterStageBuilder(makePrimaryKeyStage.getOutputColumns())
				.addTransform(valueColumn, new PlainStringNumberTransform(valueColumn))
				.buildFilterStage();
		ResultSet fixedScientificNotationResultSet = new FilteredResultSet(numericPrimaryKeyResultSet, fixScientificNotationStage);
		
		return fixedScientificNotationResultSet;
	}

	@Override
	public IParser getParser() {
		return null;
	}

	@Override
	public boolean isValidInput() {
		return true;
	}

	@Override
	public boolean isReady() {
		if (client.getState() == Thread.State.NEW) {
			client.start();
		} else if (client.getState() == Thread.State.TERMINATED) {
			this.isReady = true;
			timer = Profiler.startTimer();
		}
		
		return this.isReady;
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.cg;
	}
	
	private static ColumnGrouping makeColumnGrouping(Column valueColumn) {
		Column primaryKey = COMPOSITE_KEY_COL;
		List<Column> allColumns = new LinkedList<>();
		allColumns.add(primaryKey);
		allColumns.add(SOS_SITE_COL);
		allColumns.add(SOS_CONSTITUENT_COL);
		allColumns.add(SOS_MOD_TYPE_COL);
		allColumns.add(SOS_DATE_COL);
		allColumns.add(valueColumn);
		return new ColumnGrouping(primaryKey, allColumns);
	}
	
	@Override
	public void close() {
		client.close();
		long mergeTime = Profiler.stopTimer(timer);
		Profiler.log.debug("Time taken to merge sos stream {} milliseconds", mergeTime);
	}

}
