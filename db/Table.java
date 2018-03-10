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
    private String name;    // name of the table: <name>.tbl
    private final int numColumns;
    private boolean isValid = true;     // set to false if format is incorrect
    private String[] columnNames;
    private String[] rawColumnNames;    // column names without types
    private final String[] validTypes = new String[]{"string", "int", "float"};
    private String[] columnTypes;
    private LinkedHashMap<Integer, LinkedHashMap<String, String>> table;

    /** Creates a Table with column names. */
    public Table(String[] columnNames) {
        /* Check correct formatting. */
        if (!areValidNames(columnNames) || !setColumnTypes(columnNames)) {
            isValid = false;
            numColumns = 0;
            return;
        }

        /* Initialize Table. */
        this.numRows = 0;
        this.numColumns = columnNames.length;
        this.columnNames = new String[this.numColumns];
        System.arraycopy(columnNames, 0, this.columnNames, 0,
                this.numColumns);
        this.rawColumnNames = toRaw(this.columnNames);
        this.table = new LinkedHashMap<>();
    }

    /** Creates a Table with column names and a name. */
    public Table(String[] columnNames, String name) {
        this(columnNames);  // one-argument constructor
        this.name = name;
    }

    /**
     * Adds a row to the table if its values match their corresponding column
     * types.
     *
     * @return  true    if the values match their column types
     *          false   if not
     */
    public boolean addRow(String[] values) {
        if (values == null) {
            return false;
        }

        /* Verify that the number of values matches the number of columns. */
        if (values.length != numColumns) {
            return false;
        }

        /* For each value... */
        for (int i = 0; i < values.length; i++) {
            String value = values[i];

            /* Accept NOVALUE inputs. */
            if (value.equals("NOVALUE")) {
                continue;
            }

            /* Get the corresponding column type. */
            String type = columnTypes[i];

            if (type.equals("string")) {
                /* Verify that the value begins and ends with a single quote. */
                char firstChar = value.charAt(0);
                char lastChar = value.charAt(value.length() - 1);
                if (firstChar != '\'' || lastChar != '\'') {
                    return false;
                }

                /* Verify that the value does not contain newlines, tabs,
                   commas, or quotes between the single quotes. */
                for (int j = 1; j < value.length() - 1; j++) {
                    char c = value.charAt(j);
                    if (c == '\n' || c == '\t' || c == ',' ||
                        c == '\'' || c == '\"')
                    {
                        return false;
                    }
                }
            } else if (type.equals("int")) {
                /* Verify that the value only contains digits, except for the
                   first character, which may be a negative sign. */
                char c = value.charAt(0);
                if (!Character.isDigit(c) && c != '-') {
                    return false;
                }
                for (int j = 1; j < value.length(); j++) {
                    c = value.charAt(j);
                    if (!Character.isDigit(c)) {
                        return false;
                    }
                }
            } else if (type.equals("float")) {
                /* Verify that the value only contains digits and exactly
                   one decimal point. The first character may be a decimal
                   point, a negative sign, or a digit. */
                char c = value.charAt(0);
                if (!Character.isDigit(c) && c != '-' && c != '.') {
                    return false;
                }
                int decimalPointCount = 0;
                for (int j = 1; j < value.length(); j++) {
                    c = value.charAt(j);
                    if (!Character.isDigit(c) && c != '.') {
                        return false;
                    }
                    if (c == '.') {
                        decimalPointCount++;
                    }
                }
                if (decimalPointCount != 1) {
                    return false;
                }
            } else {
                return false;
            }
        }

        numRows++;
        table.put(numRows, new LinkedHashMap<>());

        for (int i = 0; i < values.length; i++) {
            table.get(numRows).put(columnNames[i], values[i]);
        }
        return true;
    }

    /** Gets the name of the table. */
    public String getName() {
        return name;
    }

    /** Sets the name of the table. */
    public void setName(String name) {
        this.name = name;
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
            if (columnNames[i].equals(columnName)) {
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
                if (columnNames[i].equals(arr[j])) {
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

    /** Get column names. */
    public String[] getColumnNames() {
        return columnNames;
    }

    /** Get raw column names. */
    public String[] getRawColumnNames() {
        return rawColumnNames;
    }

    /** Prints the column names in the Table. */
    public void printColumnNames() {
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
        System.out.print(toString());
    }

    /** Finds and returns a list of shared columns. */
    public static ArrayList<String> sharedColumns(Table a, Table b) {
        ArrayList<String> list = new ArrayList<>();
        for (String leftColumn : a.columnNames) {
            for (String rightColumn : b.columnNames) {
                if (leftColumn.equals(rightColumn)) {
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
                if (leftColumn.equals(rightColumn)) {
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
        if (a == null || b == null) {
            return null;
        }

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

    /** Gets the string representation of the table. */
    public String toString() {
        String ret = "";

        /* Column names. */
        int count = 1;
        for (String name : columnNames) {
            ret += name;
            /* Don't end the line with a comma. */
            if (count != numColumns) {
                ret += ",";
            }
            count++;
        }
        ret += "\n";

        /* Rows. */
        for (Map.Entry<Integer, LinkedHashMap<String, String>>
                outerEntry : table.entrySet())
        {
            count = 1;
            for (Map.Entry<String, String> innerEntry :
                    outerEntry.getValue().entrySet())
            {
                ret += innerEntry.getValue();
                /* Don't end the line with a comma. */
                if (count != numColumns) {
                    ret += ",";
                }
                count++;
            }
            ret += "\n";
        }
        return ret;
    }

    /**
     * Sets column types.
     *
     * @return  true    if column names are formatted correctly
     *          false   if not
     */
    public boolean setColumnTypes(String[] columnNames) {
        columnTypes = new String[columnNames.length];

        /* For every column name... */
        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            String substring;

            /* Get the string after the space character, aka the type. */
            if (name.contains(" ")) {
                substring = name.substring(name.indexOf(' ') + 1);
                if (substring.equals("")) {
                    return false;
                }
            } else {
                return false;
            }

            /* Check if the type is valid. */
            boolean isValid = false;
            for (int j = 0; j < validTypes.length; j++) {
                if (validTypes[j].equals(substring)) {
                    this.columnTypes[i] = validTypes[j];
                    isValid = true;
                }
            }
            if (!isValid) {
                return false;
            }
        }
        return true;
    }

    /** Get column types. */
    public String[] getColumnTypes() {
        return columnTypes;
    }

    /** Returns whether or not the table is valid. */
    public boolean isValid() {
        return isValid;
    }

    /** Check if the table names are valid. */
    public boolean areValidNames(String[] columnNames) {
        /*
         * Invariants:
         * Column names must:
         *      start with a letter,
         *      have more than 1 character,
         *      contain only letters, numbers, and/or underscores
         */

        /* For every column name... */
        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            /* Get the substring before the space character. */
            String substring = name.substring(0, name.indexOf(" "));

            /* Verify that the substring's length is at least 1. */
            if (substring.length() < 1) {
                return false;
            }

            /* Verify that the first character is a letter. */
            if (!Character.isLetter(substring.charAt(0))) {
                return false;
            }

            /* Verify that the rest of the characters are only letters,
               numbers, or underscores. */
            for (int j = 1; j < substring.length(); j++) {
                char c = substring.charAt(j);
                if (!Character.isLetter(c) && !Character.isDigit(c) &&
                    c != '_')
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the "raw" column names, aka column names without the type.
     * Example: LastName string --> LastName
     * This will be useful for database transactions that use 'select'.
     */
    private String[] toRaw(String[] columnNames) {
        String[] rawColumnNames = new String[numColumns];

        /* For each column name... */
        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            /* Store the substring before the space character. */
            String substring = name.substring(0, name.indexOf(" "));
            rawColumnNames[i] = substring;
        }
        return rawColumnNames;
    }

    /** Checks if a raw column name exists, and if so, returns the non-raw
     * version (aka the column name + ' ' + type):
     * Example: LastName --> LastName string
     *
     * @return  name    if column name exists in table
     *          null    otherwise
     */
    public String addType(String rawColumnName) {
        for (int i = 0; i < rawColumnNames.length; i++) {
            if (rawColumnNames[i].equals(rawColumnName)) {
                return columnNames[i];
            }
        }
        return null;
    }

    /**
     * Get the type of the column given column name.
     *
     * @return  "string"    if string
     *          "int"       if int
     *          "float"     if float
     *          null        if column doesn't exist in table
     */
    public String getColType(String columnName) {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnName)) {
                return columnTypes[i];
            }
        }
        return null;
    }
}
