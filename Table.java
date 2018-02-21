import java.util.*;

/**
 * Table class that holds values in columns and rows.
 * Design: Map<Integer, Map<String, String>>, where
 *     the Integer is the row number (row 0 == column names),
 *     the first String is the name of a column, and
 *     the second String is the value in that particular column (and row).
 *
 * @author Moo Jin Kim
 */
public class Table {
    /* Instance variables */
    private int numRows;    // excluding row 0 (column names)
    private final int numColumns;
    private String[] columnNames;
    private LinkedHashMap<Integer, LinkedHashMap<String, String>> table;

    /** Creates a Table with a fixed number of columns and column names. */
    public Table(int numColumns, String[] columnNames) {
        this.numRows = 0;
        this.numColumns = numColumns;
        this.columnNames = new String[this.numColumns];
        System.arraycopy(columnNames, 0, this.columnNames, 0,
                         this.numColumns);
        this.table = new LinkedHashMap<>();
    }

    /** Adds a row to the table. */
    public void addRow(String[] values) {
        if (values == null) {
            return;
        }

        numRows++;
        table.put(numRows, new LinkedHashMap<>());

        for (int i = 0; i < values.length; i++) {
            table.get(numRows).put(columnNames[i], values[i]);
        }
    }

    /** Returns the number of rows in a table. */
    public int getNumRows() {
        return numRows;
    }

    /** Returns the number of columns in a table. */
    public int getNumColumns() {
        return numColumns;
    }

    /**
     * Returns the Nth row from the table, where row 0 is the row
     * containing column names, and row 1 is the first row of elements.
     * This function makes join() so much easier to implement.
     */
    public String[] getRow(int N) {
        String[] rowElements;
        if (N < 0) {
            rowElements = null;
        } else if (N == 0) {
            rowElements = new String[numColumns];
            System.arraycopy(columnNames, 0, rowElements, 0, numColumns);
        } else {
            rowElements = new String[numColumns];
            int index = 0;
            for (Map.Entry<String, String> column : table.get(N).entrySet()) {
                rowElements[index] = column.getValue();
                index++;
            }
        }
        return rowElements;
    }

    /** Prints the column names in the Table. */
    public void printColumns() {
        for (String name : columnNames) {
            System.out.print(name + "\t");
        }
        System.out.print("\n");
    }

    /** Prints the rows in the Table. */
    public void printRows() {
        for (Map.Entry<Integer, LinkedHashMap<String, String>>
                outerEntry : table.entrySet())
        {
            for (Map.Entry<String, String> innerEntry :
                 outerEntry.getValue().entrySet())
            {
                System.out.print(innerEntry.getValue() + "\t");
            }
            System.out.print("\n");
        }
        System.out.print("\n");
    }

    /** Prints the Table. */
    public void printTable() {
        printColumns();
        printRows();
    }

    /** Finds and returns a list of shared columns. */
    public static ArrayList<String> sharedColumns(Table a, Table b) {
        ArrayList<String> list = new ArrayList<>();
        for (String leftColumn : a.columnNames) {
            for (String rightColumn : b.columnNames) {
                if (leftColumn == rightColumn) {
                    list.add(rightColumn);
                    break;
                }
            }
        }
        return list;
    }

    /**
     * Finds and returns a list of columns unique to the Table passed in
     * as the first argument.
     */
    public static ArrayList<String> uniqueColumns(Table a, Table b) {
        ArrayList<String> list = new ArrayList<>();
        for (String leftColumn : a.columnNames) {
            boolean isUnique = true;
            for (String rightColumn : b.columnNames) {
                if (leftColumn == rightColumn) {
                    isUnique = false;
                    break;
                }
            }
            if (isUnique) {
                list.add(leftColumn);
            }
        }
        return list;
    }

    /** Combines two tables and returns the result. */
    public static Table join(Table a, Table b) {
        /* Find columns shared by both tables. */
        ArrayList<String> sharedColumns = sharedColumns(a, b);

        /* Find columns unique to Table a (the "left" table). */
        ArrayList<String> leftUniqueColumns = uniqueColumns(a, b);

        /* Find columns unique to Table b (the "right" table). */
        ArrayList<String> rightUniqueColumns = uniqueColumns(b, a);

        /* Initialize the combined table. */
        ArrayList<String> temp = new ArrayList<String>();
        temp.addAll(sharedColumns);
        temp.addAll(leftUniqueColumns);
        temp.addAll(rightUniqueColumns);
        int len = sharedColumns.size() + leftUniqueColumns.size() +
                  rightUniqueColumns.size();
        String[] columnNames = temp.toArray(new String[0]); // from StckOvflw
        Table ret = new Table(len, columnNames);

        /* If there are no shared columns, return the Cartesian Product of the
           two tables. */
        if (sharedColumns.size() == 0) {
            int nRow_a = a.getNumRows();
            int nCol_a = a.getNumColumns();
            int nRow_b = a.getNumRows();
            int nCol_b = a.getNumColumns();
            for (int i = 1; i <= nRow_a; i++) {
                for (int j = 1; j <= nRow_b; j++) {
                    String[] rowElements = new String[len];
                    /* Insert all elements from A's row. */
                    System.arraycopy(a.getRow(i), 0, rowElements, 0, nCol_a);
                    /* Insert all elements from B's row. */
                    System.arraycopy(b.getRow(j), 0, rowElements, nCol_a, nCol_b);
                    ret.addRow(rowElements);
                }
            }
            return ret;
        }

        /* Iterate through the left table first. */
        /* Merge rows i.f.f. values in every shared column match. */
        for (Map.Entry<Integer, LinkedHashMap<String, String>> row :
             a.table.entrySet()) // each row
        {

            for (Map.Entry<String, String> column :
                    row.getValue().entrySet())
            {

            }
        }
        /* Merge rows i.f.f. values in every shared column match. */
        return null;
    }

    public static void main(String[] args) {
        String[] columns = new String[]{"X int", "Y int"};
        Table T1 = new Table(2, columns);
        String[] row1 = new String[]{"2", "5"};
        String[] row2 = new String[]{"8", "3"};
        String[] row3 = new String[]{"13", "7"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);
        T1.printTable();
        System.out.println("Done!");
    }
}
