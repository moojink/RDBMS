package db;

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
    private int numRows;    // excluding row 0 (column names)
    private final int numColumns;
    private String[] columnNames;
    private LinkedHashMap<Integer, LinkedHashMap<String, String>> table;

    /** Creates a Table with a fixed number of columns and column names. */
    public Table(String[] columnNames) {
        this.numRows = 0;
        this.numColumns = columnNames.length;
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
     * 0 <= N <= numRows
     */
    public String[] getRow(int N) {
        String[] rowElements;
        if (N < 0 || N > numRows) {
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

    /**
     * Gets the value in the table at the specified row and column.
     * Row 1: first row of values
     * Column 1: first column of values
     * So, to get the top left value, row == 1 and col == 1.
     *
     * @return  value   if in bounds
     *          null    if not in bounds
     */
    public String getVal(int row, int col) {
        if (row < 1 || col < 1 || row > numRows || col > numColumns)
            return null;

        String[] rowElements = getRow(row);
        return rowElements[col - 1];
    }

    /**
     * Gets the value in the table at the Nth row and corresponding to
     * the column name.
     */
    public String getVal(int N, String name) {
        if (N < 1 || N > numRows)
            return null;

        String[] column = getCol(name);
        return column[N - 1];
    }

    /**
     * Returns the Nth column of values from the table, where column 1 is the
     * first column. Does not include the column name.
     * 1 <= N <= numColumns
     */
    public String[] getCol(int N) {
        String[] colElements;
        if (N < 1 || N > numColumns) {
            colElements = null;
        } else {
            colElements = new String[numRows];
            int index = 0;
            for (int i = 1; i <= numRows; i++) {
                colElements[index] = getVal(i, N);
                index++;
            }
        }
        return colElements;
    }

    /**
     * Returns the column of values from the table corresponding to the
     * column name passed in as the argument. Does not include the column name.
     */
    public String[] getCol(String columnName) {
        for (int i = 0; i < numColumns; i++) {
            if (columnNames[i] == columnName) {
                return getCol(i + 1);
            }
        }
        return null;
    }

    /**
     * Returns the Nth row, excluding values from columns whose names match
     * the names in the array passed in as an argument. Although getRow()
     * can get the 0th row (column names), this function cannot.
     */
    public String[] getRestOfRow(int N, String[] arr) {
        if (N < 1 || N > numRows || arr == null)
            return null;

        String[] entireRow = getRow(N);
        int amountRemoved = 0;
        for (int i = 0; i < numColumns; i++) {
            for (int j = 0; j < arr.length; j++) {
                /* If the column name matches one of the names in arr, remove
                   the value from the row. */
                if (columnNames[i] == arr[j]) {
                    entireRow[i] = null;
                    amountRemoved++;
                    break;
                }
            }
        }

        /* Now return the non-null values. */
        int len = numColumns - amountRemoved;
        int index = 0;
        String[] restOfRow = new String[len];
        for (int i = 0; i < numColumns; i++) {
            if (entireRow[i] != null) {
                restOfRow[index] = entireRow[i];
                index++;
            }
        }
        return restOfRow;
    }

    /** Prints the column names in the Table. */
    public void printColumns() {
        int count = 1;
        for (String name : columnNames) {
            System.out.print(name);
            /* Don't end the line with a comma. */
            if (count != numColumns) {
                System.out.print(",");
            }
            count++;
        }
        System.out.print("\n");
    }

    /** Prints the rows in the Table. */
    public void printRows() {
        for (Map.Entry<Integer, LinkedHashMap<String, String>>
                outerEntry : table.entrySet())
        {
            int count = 1;
            for (Map.Entry<String, String> innerEntry :
                 outerEntry.getValue().entrySet())
            {
                System.out.print(innerEntry.getValue());
                /* Don't end the line with a comma. */
                if (count != numColumns) {
                    System.out.print(",");
                }
                count++;
            }
            System.out.print("\n");
        }
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
        int nRow_a = a.getNumRows();
        int nCol_a = a.getNumColumns();
        int nRow_b = b.getNumRows();
        int nCol_b = b.getNumColumns();
        ArrayList<String> temp = new ArrayList<String>();
        temp.addAll(sharedColumns);
        temp.addAll(leftUniqueColumns);
        temp.addAll(rightUniqueColumns);
        int len = sharedColumns.size() + leftUniqueColumns.size() +
                  rightUniqueColumns.size();
        String[] columnNames = temp.toArray(new String[0]); // from StckOvflw
        Table ret = new Table(columnNames);

        /* If there are no shared columns, return the Cartesian Product of the
           two tables. */
        if (sharedColumns.size() == 0) {
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


        /* If there are shared columns, return merged rows i.f.f. values
           in every shared column match. */

        /* For each row in the left table... */
        for (int i = 1; i <= nRow_a; i++) {
            /* Create an array containing only the values in the shared
               columns, in the same order as the shared columns in the final
               table. */
            String[] arr_a = new String[sharedColumns.size()];
            int index_a = 0;
            for (String name : sharedColumns) {
                arr_a[index_a] = a.getVal(i, name);
                index_a++;
            }

            /* Do the same for each row in the right table while comparing the
               arrays. If the left table array matches the right table array,
               then merge the rest of those rows together. */
            for (int j = 1; j <= nRow_b; j++) {
                String[] arr_b = new String[sharedColumns.size()];
                int index_b = 0;
                for (String name: sharedColumns) {
                    arr_b[index_b] = b.getVal(j, name);
                    index_b++;
                }
                if (Arrays.equals(arr_a, arr_b)) {
                    int index_r = 0;
                    String[] rowToAdd = new String[len];
                    String[] sharedColumnNames = sharedColumns.toArray(new String[0]);
                    String[] ror_a = a.getRestOfRow(i, sharedColumnNames);
                    String[] ror_b = b.getRestOfRow(j, sharedColumnNames);

                    /* Add in the shared column values. */
                    System.arraycopy(arr_a, 0, rowToAdd, 0, arr_a.length);
                    index_r += arr_a.length;

                    /* Add in the left table row's values. */
                    for (String val : ror_a) {
                        rowToAdd[index_r] = val;
                        index_r++;
                    }
                    /* Add in the right table row's values.*/
                    for (String val : ror_b) {
                        rowToAdd[index_r] = val;
                        index_r++;
                    }
                    ret.addRow(rowToAdd);
                }
            }
        }
        return ret;
    }

    /** Combines multiple tables and returns the result. */
    public static Table join(Table[] arr) {
        Table ret = null;
        int size = arr.length;
        for (int i = 1; i < size; i++) {
            arr[i] = join(arr[i - 1], arr[i]);
        }
        return arr[size - 1];
    }

    public static void main(String[] args) {

    }
}
