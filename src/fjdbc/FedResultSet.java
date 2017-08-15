package fjdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/*
 * CAUTION: Haven't implemented this class yet, might/might not need modificaions. So can't comment on it. Can be changed based on requirements
 */
public class FedResultSet implements FedResultSetInterface {

	private List<ResultSet> resultSets;
	private ResultSet currentResultSet;

	private boolean isClose = true;

	public FedResultSet(List<ResultSet> resultSets) {
		this.resultSets = resultSets;
		currentResultSet = resultSets.remove(0);
		isClose = false;
	}

	
	public boolean next() throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}

		boolean hasNext = false;
		try {
			hasNext = currentResultSet.next();
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		if (hasNext) {
			return true;
		}
		if (resultSets.isEmpty()) {
			return false;
		}
		currentResultSet = resultSets.remove(0);
		return true;
	}

	
	public String getString(int columnIndex) throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		String value = null;
		try {
			value = currentResultSet.getString(columnIndex);
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return value;
	}

	
	public int getInt(int columnIndex) throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		Integer value = null;
		try {
			value = currentResultSet.getInt(columnIndex);
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return value.intValue();
	}

	
	public int getColumnCount() throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		int value = 0;
		try {
			value = currentResultSet.getMetaData().getColumnCount();
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return value;
	}

	
	public String getColumnName(int index) throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		String value = "";
		try {
			value = currentResultSet.getMetaData().getColumnName(index);
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return value;
	}

	
	public int getColumnType(int index) throws FedException {
		if (isClose) {
			throw new FedException(new Throwable("FedConnection resource is closed."));
		}
		Integer value = null;
		try {
			value = currentResultSet.getMetaData().getColumnType(index);
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
		return value.intValue();
	}

	
	public void close() throws FedException {
		try {
			currentResultSet.close();
			isClose = true;
		} catch (SQLException e) {
			throw new FedException(new Throwable(e.getMessage()));
		}
	}

}
