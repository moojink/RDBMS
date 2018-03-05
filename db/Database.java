package db;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.StringJoiner;
import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;

public class Database {
    static ArrayList<Table> tables;
    int numTables;

    /* Various common constructs, simplifies parsing. */
    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    /* Stage 1 syntax, contains the command name. */
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD   = Pattern.compile("load " + REST),
            STORE_CMD  = Pattern.compile("store " + REST),
            DROP_CMD   = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD  = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    /* Stage 2 syntax, contains the clauses of commands. */
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\(\\s*(\\S+\\s+\\S+\\s*" +
            "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                    "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                    "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+" +
                    "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+" +
                    SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                    "\\s*(?:,\\s*.+?\\s*)*)");

    /** Constructor. */
    public Database() {
        tables = new ArrayList<Table>();
        numTables = 0;
    }

    /** Processes database transaction. */
    public String transact(String query) {
        return eval(query);
    }
    /** Evaluates the query: calls appropriate function and returns result. */
    public static String eval(String query) {
        String result = "";
        Matcher m;

        if ((m = CREATE_CMD.matcher(query)).matches()) {
            result = createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            result = loadTable(m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            result = storeTable(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            result = dropTable(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            result = insertRow(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            result = printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            result = select(m.group(1));
        } else {
            result = "ERROR: Malformed query.\n";
        }
        return result;
    }

    /** Finds and returns a table with the corresponding name, or null. */
    private static Table findTable(String name) {
        for (Table table : tables) {
            String temp = table.getName();
            if (temp != null && temp.equals(name)) {
                return table;
            }
        }
        return null;
    }

    /** Calls the correct table creating function. */
    private static String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3),
                    m.group(4));
        } else {
            return "ERROR: Malformed create command.\n";
        }
    }

    /**
     * Creates a new table from scratch.
     *
     * @param name  name of the table
     * @param cols  array containing the column names
     */
    private static String createNewTable(String name, String[] cols) {
        /* Create the table if the name is not already used. */
        if (findTable(name) == null) {
            Table table = new Table(cols, name);
            if (table.isValid()) {
                tables.add(table);
                return "";
            } else {
                return "ERROR: incorrect format!\n";
            }
        }
        return "ERROR: table " + name + " already exists!\n";
    }

    /** Creates a table using 'select'. */
    private static String createSelectedTable(String name, String exprs, String
            tables, String conds) {
        return "You are trying to create a table named " + name +
                " by selecting these " + "expressions: '" + exprs +
                "' from the join of these tables: '" + tables +
                "', filtered by these conditions: '" + conds + "'\n";
    }

    /** Loads table from a .tbl file. */
    private static String loadTable(String name) {
        Table table;
        String filename = name + ".tbl";
        File file = new File(filename);
        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader(file));
            String line;

            /* Process the first line in the file. */
            line = reader.readLine();
            ArrayList<String> list = new ArrayList<String>();
            String columnName = "";
            int lineLength = line.length();
            for (int i = 0; i < lineLength; i++) {
                char c = line.charAt(i);
                /* Upon finding comma, add the column name to list. */
                if (c == ',') {
                    list.add(columnName);
                    columnName = "";
                    continue;
                }
                columnName += c;
            }
            list.add(columnName);    // add last column

            /* Initialize the table. */
            String[] columnNames = list.toArray(new String[0]);
            table = new Table(columnNames, name);

            /* Check if the table is valid. */
            if (!table.isValid()) {
                return "ERROR: Attempting to load incorrectly formatted " +
                        "table!\n";
            }

            /* Process all other lines. */
            String row[];
            while ((line = reader.readLine()) != null) {
                row = lineToArr(line);
                table.addRow(row);
            }

            /* If a table exists with the same name, overwrite it. */
            Table temp = findTable(name);
            if (temp != null) {
                tables.remove(temp);
            }
            tables.add(table);
            reader.close();
        } catch (FileNotFoundException e) {
            return "ERROR: " + filename + " not found.\n";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    /** Writes table to a .tbl file. */
    private static String storeTable(String name) {
        Table table = findTable(name);
        if (table == null) {
            return "ERROR: No such table: " + name + "\n";
        }

        /* Create and write to .tbl file. */
        try {
            String filename = name + ".tbl";
            BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
            bw.write(table.toString());
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: IOException caught.\n";
        }

        return "";
    }

    /** Deallocates a table. */
    private static String dropTable(String name) {
        Table table = findTable(name);
        if (table != null) {
            tables.remove(table);
            return "";
        } else {
            return "ERROR: No such table: " + name + "\n";
        }
    }

    /** Inserts a row into a table. */
    private static String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "Malformed insert.";
        }

        String name = m.group(1);
        String line = m.group(2);

        Table table = findTable(name);
        if (table != null) {
            boolean ret = table.addRow(lineToArr(line));
            if (ret == false) {
                return "ERROR: Row format does not match the table's!\n";
            }
        } else {
            return "ERROR: No such table: " + name + "\n";
        }
        return "";
    }

    /** Prints the table. */
    private static String printTable(String name) {
        Table table = findTable(name);
        if (table != null) {
            table.printTable();
            return "";
        } else {
            return "ERROR: No such table: " + name + "\n";
        }
    }

    /** Processes a comma-separated line and converts it to an array. */
    private static String[] lineToArr(String line) {
        ArrayList<String> list = new ArrayList<>();
        int lineLength = line.length();
        String value = "";

        for (int i = 0; i < lineLength; i++) {
            char c = line.charAt(i);
            /* Upon finding comma, add the value to the list. */
            if (c == ',') {
                list.add(value);
                value = "";
                continue;
            }
            value += c;
        }
        list.add(value);    // add the last value
        String[] arr = list.toArray(new String[0]);
        return arr;
    }

    /**
     * Checks whether the operation between two columns are valid by checking
     * their types in the table.
     */
    private static boolean isValidOperation(Table table, String name1,
                                            String operator, String name2)
    {
        /* Get the column types. */
        String type1 = table.getType(name1);
        String type2 = table.getType(name2);

        /* If one operand is a string, the other one MUST also be a string. */
        if (type1.equals("string") && !type2.equals("string")) {
            return false;
        }
        if (!type1.equals("string") && type2.equals("string")) {
            return false;
        }

        /* If both types are string, the only valid operator is "+". */
        if (type1.equals("string") && type2.equals("string")) {
            if (!operator.equals("+")) {
                return false;
            }
        }

        return true;
    }

    /** Helper function for applyOperation(). */
    private static float doMath(float operand1, String operator,
                                float operand2)
    {
        if (operator.equals("+")) {
            return operand1 + operand2;
        } else if (operator.equals("-")) {
            return operand1 - operand2;
        } else if (operator.equals("*")) {
            return operand1 * operand2;
        } else if (operator.equals("/")) {
            return operand1 / operand2;
        } else {
            return 0;
        }
    }

    /** Applies the operation to two arrays (columns) and returns the result. */
    private static String[] applyOperation(Table table, String name1,
                                    String operator, String name2)
    {
        /* Get the column types. */
        String type1 = table.getType(name1);
        String type2 = table.getType(name2);

        /* Get the column values. */
        String[] values1 = table.getCol(name1);
        String[] values2 = table.getCol(name2);
        int numValues = values1.length;

        String[] ret = new String[numValues];

        /* If both types are string, the operation is "+". */
        if (type1.equals("string") && type2.equals("string")) {
            /* Concatenate all values. */
            for (int i = 0; i < numValues; i++) {
                String result = "";
                result += '\'';
                String value1 = values1[i];
                for (int j = 0; j < value1.length(); j++) {
                    char c = value1.charAt(j);
                    if (c != '\'') {    // don't add extra single quotes
                        result += c;
                    }
                }

                String value2 = values2[i];
                for (int j = 0; j < value2.length(); j++) {
                    char c = value2.charAt(j);
                    if (c != '\'') {
                        result += c;    // don't add extra single quotes
                    }
                }
                result += '\'';
                ret[i] = result;
            }
            return ret;
        }


        /* If we reached this point, the types are ints/floats in any
           combination. Apply the arithmetic. Possible ops: { +,-, *, / } */
        float operand1, operand2, result;
        int result_int;
        for (int i = 0; i < numValues; i++) {
            /* Get the values in the correct types. */
            operand1 = Float.parseFloat(values1[i]);
            operand2 = Float.parseFloat(values2[i]);

            /* Do the operation. */
            result = doMath(operand1, operator, operand2);

            /* Cast the result to the right type. */
            if (type1.equals("int") && type2.equals("int")) {
                result_int = (int) result;
                ret[i] = Integer.toString(result_int);
            } else {
                ret[i] = String.format("%.3f", result);
            }
        }
        return ret;
    }

    /** Gets the resulting type after an operation. */
    private static String getResultingType(String type1, String type2) {
        if (type1.equals("string") || type2.equals("string")) {
            return "string";
        } else if (type1.equals("float") || type2.equals("float")) {
            return "float";
        } else {
            return "int";
        }
    }

    /**
     * Evaluates and applies column expressions to a table to choose which
     * columns to return.
     *
     * @param table the table to apply the expressions to
     * @param exprs the column expressions
     */
    private static Table evalExprs(Table table, String[] exprs) {
        int numColumns = exprs.length;
        int numRows = table.getNumRows();

        /* If the expression is "*", return all columns. */
        if (exprs.length == 1 && exprs[0].equals("*")) {
            return table;
        }

        /* New table's column names */
        String[] columnNames = new String[numColumns];
        /* New table's column values */
        String[][] columnValues = new String[numColumns][numRows];

        /* For each expression... */
        for (int i = 0; i < exprs.length; i++) {
            String expr = exprs[i];

            /* Check how many operands there are: 1 or 2.
               If there is 1 operand, it's just the column name; 0 spaces.

               If there are 2 operands, there are 4 parts: operand1, operator,
               operand2, and newColumnName. Each is separated by a space;
               4 spaces. Example: "a + b as sum" (the "as" is used to name
               the new column as "sum").

               Any other number of spaces is an error. */

            String operand1 = null, operator = null, operand2 = null;
            String newColumnName = null;
            String substring = "";
            int numSpaces = 0;
            for (int j = 0; j < expr.length(); j++) {
                char c = expr.charAt(j);
                if (c == ' ') {
                    numSpaces++;
                    if (numSpaces == 1) {
                        operand1 = substring;
                    } else if (numSpaces == 2) {
                        operator = substring;
                    } else if (numSpaces == 3) {
                        operand2 = substring;
                    }
                    substring = "";
                    continue;
                }
                substring += c;
            }
            newColumnName = substring;

            if (numSpaces == 0) {                               // 1 operand
                /* Verify that the column name exists in table. */
                String columnName = table.addType(expr);
                if (columnName == null) {
                    return null;
                }

                /* Just copy the entire column. */
                columnNames[i] = columnName;
                String[] values = table.getCol(columnName);
                columnValues[i] = values;
            } else if (numSpaces == 4) {                        // 2 operands
                /* Verify that an "as" was included. */
                if (!expr.contains("as")) {
                    return null;
                }
                /* Verify that the column names exist in table. */
                String name1 = table.addType(operand1);
                String name2 = table.addType(operand2);
                if (name1 == null || name2 == null) {
                    return null;
                }

                /* Verify that this is a valid operation. */
                if (!isValidOperation(table, name1, operator, name2)) {
                    return null;
                }

                /* Apply the operation and form the correct column values. */
                String[] values = applyOperation(table, name1, operator, name2);
                columnValues[i] = values;

                /* Attach the correct type to the newly formed column. */
                String type1 = table.getType(name1);
                String type2 = table.getType(name2);
                String resultingType = getResultingType(type1, type2);
                newColumnName += " " + resultingType;
                columnNames[i] = newColumnName;

            } else {
                return null;
            }
        }

        /* Put the column names and column values into a new table. */
        Table ret = new Table(columnNames);
        for (int i = 0; i < numRows; i++) {
            String[] row = new String[numColumns];
            for (int j = 0; j < numColumns; j++) {
                row[j] = columnValues[j][i];    // j first, i second on purpose
            }
            ret.addRow(row);
        }
        return ret;
    }

    /**
     * Evaluates and applies conditions to a table to choose which rows to
     * return.
     *
     * @param table the table to apply the expressions to
     * @param conds the conditions
     */
    private static Table evalConds(Table table, String[] conds) {
        int numColumns = table.getNumColumns();
        int numRowsOriginal = table.getNumRows();

        Table ret = new Table(table.getColumnNames());

        /* For each row in the original table... */
        for (int i = 1; i <= numRowsOriginal; i++) {
            boolean shouldAdd = true;
            String[] row = table.getRow(i);

            /* For each condition... */
            for (int k = 0; k < conds.length; k++) {
                String cond = conds[k];
                String operand1 = null, operator = null, operand2 = null;
                String substring = "";

                /* Split up the condition into 3 parts:
                   operand1, operator, operand2.
                   Example: "Lastname <= 'Lee'" */
                int numSpaces = 0;
                for (int j = 0; j < cond.length(); j++) {
                    char c = cond.charAt(j);
                    if (c == ' ') {
                        numSpaces++;
                        if (numSpaces == 1) {
                            operand1 = substring;
                        } else if (numSpaces == 2) {
                            operator = substring;
                        }
                        substring = "";
                        continue;
                    }
                    substring += c;
                }
                operand2 = substring;

                /* Verify that the condition has correct format (2 spaces). */
                if (numSpaces != 2) {
                    return null;
                }

                /* Find the column(s) that this condition applies to. */

            }
            if (shouldAdd) {
                ret.addRow(row);
            }
        }


        return ret;
    }

    private static String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return "Malformed select: " + expr + "\n";
        }

        return select(m.group(1), m.group(2), m.group(3));
    }

    private static String select(String exprs, String tables, String conds) {
        /* Split up the lines into processable arrays. */
        String[] columnExprs = null, names = null, conditions = null;
        if (exprs != null) {
            columnExprs = lineToArr(exprs);
        } else {
            return "ERROR: No column expressions given!\n";
        }
        if (tables != null) {
            names = lineToArr(tables);
        } else {
            return "ERROR: No table names given!\n";
        }
        if (conds != null) {
            conditions = conds.split(AND);
        }

        /* Verify that each table exists in the database. */
        Table[] arr = new Table[names.length];
        for (int i = 0; i < names.length; i++) {
            Table table = findTable(names[i]);
            if (table != null) {
                arr[i] = table;
            } else {
                return "ERROR: No such table: " + names[i] + "\n";
            }
        }

        /* Join the tables. */
        Table joinedTable = Table.join(arr);

        Table expressedTable = null, conditionedTable = null;
        if (columnExprs != null) {
            expressedTable = evalExprs(joinedTable, columnExprs);
        }
        if (expressedTable == null) {
            return "ERROR: Malformed column expressions.\n";
        }
        if (conditions != null) {
            conditionedTable = evalConds(expressedTable, conditions);
            if (conditionedTable == null) {
                return "ERROR: Malformed conditions.\n";
            }
            return conditionedTable.toString();
        } else {
            return expressedTable.toString();
        }
    }
}
