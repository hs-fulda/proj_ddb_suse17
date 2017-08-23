package fdbs;

public class QueryTypeConstant {
	public static final int NONE = -1;
	public static final int CREATE_NON_PARTITIONED = 1;
	public static final int CREATE_PARTITIONED = 2;
	public static final int DROP = 3;
	public static final int DELETE = 4;
	public static final int INSERT = 5;
	public static final int UPDATE = 6;
	public static final int SELECT_WITHOUT_JOIN = 7;
	public static final int SELECT_WITH_JOIN_ONLY = 8;
	public static final int SELECT_WITH_JOIN_AND_NONJOIN = 9;
	public static final int SELECT_WITH_GROUP = 10;
	public static final int SELECT_COUNT_ALL_TABLE = 11;
	public static final int SELECT_NO_GROUP = 12;

	
}
