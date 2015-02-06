package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.PeekingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class CachedResultSet extends PeekingResultSet {
	
	private static final long serialVersionUID = -1057296193256896787L;
	
	private static final Logger log = LoggerFactory.getLogger(CachedResultSet.class);
	
	private final ObjectInputStream objInputStream;
	
	public CachedResultSet(File file) throws IOException {
		this.objInputStream = new ObjectInputStream(new FileInputStream(file));
		this.metadata = deserializeMetadata(this.objInputStream);
	}
	
	/**
	 * Write to disk a serialized resultset
	 * @param rset {@link java.sql.Resultset} to cache to disk
	 * @param file {@link java.io.File} to write to
	 * @throws java.io.IOException
	 * @throws java.sql.SQLException
	 */
	public static void serialize(ResultSet rset, File file) throws IOException, SQLException {
		FileOutputStream f = new FileOutputStream(file);
		
		try (ObjectOutput s = new ObjectOutputStream(f)) {
			ColumnGrouping columnGrouping = ColumnGrouping.getColumnGrouping(rset);
			ResultSetMetaData metaData = new CGResultSetMetaData(columnGrouping);
			s.writeObject(metaData);

			while (rset.next()) {
				TableRow tr = TableRow.buildTableRow(rset);
				s.writeObject(tr);
			}
		}
	}

	@Override
	protected void addNextRow() throws SQLException {
		TableRow row = null;
		try {
			Object obj = objInputStream.readObject();
			if (obj instanceof TableRow) {
				row = (TableRow)obj;
			}
		} catch (ClassNotFoundException | IOException e) {
			log.debug(e.getMessage(), e);
		}
		if (row != null) {
			this.nextRows.add(row);
		}
	}

	@Override
	public String getCursorName() throws SQLException {
		return "cacheCursor";
	}
	
	private static ResultSetMetaData deserializeMetadata(ObjectInputStream input) {
		ResultSetMetaData result = null;
		try {
			Object obj = input.readObject();
			if (obj instanceof ResultSetMetaData) {
				result = (ResultSetMetaData)obj;
			} else {
				throw new ClassCastException("Object read is not of correct type");
			}
		}
		catch (IOException | ClassNotFoundException ex) {
			log.error(ex.getMessage(), ex);
		}
		return result;
	}

	@Override
	public void close() throws SQLException {
		IOUtils.closeQuietly(this.objInputStream);
		super.close();
	}

}
