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
    public static String eval(String unformattedQuery) {
        String result = "";
        Matcher m;

        String query = formatQuery(unformattedQuery);

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

    /** Formats a query to remove excess whitespaces and returns the result. */
    private static String formatQuery(String unformattedQuery) {
        /*
         * Each command may have arbitrary amounts of whitespace in between
         * keywords. To make life easier for the rest of these functions (which
         * assume that the format strictly matches the formats given in the
         * spec), format the query before passing it to all the database
         * functions.
         *
         * Formatting algorithm:
         *      PART 1:
         *          Convert all series of spaces into 1 space, while removing
         *          leading and trailing spaces.
         *      PART 2:
         *          There should be NO spaces between a keyword and a comma, on
         *          both sides of the comma. So, remove spaces that immediately
         *          precede or follow a comma.
         */

        String ret = "";

        /* PART 1 */
        ret = unformattedQuery.trim().replaceAll(" +", " "); // from StckOvflw

        /* Part 2 */
        ret = ret.trim().replaceAll(", ", ",");
        ret = ret.trim().replaceAll(" ,", ",");

        return ret;
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
            tableNames, String conds) {
        /* Get string representation of table. */
        String stringRep = select(exprs, tableNames, conds);

        /* Separate the string representation of table line-by-line. */
        ArrayList<String> lines = new ArrayList<>();
        String line = "";
        for (int i = 0; i < stringRep.length(); i++) {
            char c = stringRep.charAt(i);
            if (c == '\n') {
                lines.add(line);
                line = "";
                continue;
            }
            line += c;
        }

        /* Process the first line in the string representation of table. */
        line = lines.get(0);
        ArrayList<String> list = new ArrayList<>();
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
        Table table = new Table(columnNames, name);

        /* Check if the table is valid. */
        if (!table.isValid()) {
            return "ERROR: Attempting to create incorrectly formatted " +
                    "table!\n";
        }

        /* Process all other lines. */
        String row[];
        for (int i = 1; i < lines.size(); i++) {
            line = lines.get(i);
            row = lineToArr(line);
            table.addRow(row);
        }

        /* If a table exists with the same name, overwrite it. */
        Table temp = findTable(name);
        if (temp != null) {
            tables.remove(temp);
        }
        tables.add(table);
        return "";
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
            return "Malformed insert.\n";
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
     * Gets the type of a literal: string, int, or float.
     *
     * @return  "string"    if matches string format
     *          "int"       if matches int format
     *          "float"     if matches float format
     *          null        if matches no format
     */
    private static String getLitType(String literal) {
        /* Three possible types:
           string: begins and ends with single quote, no other quotes between.
           int: all characters are digits, no decimal point.
           float: all characters are digits, exactly 1 decimal point. */
        boolean literalIsString = true, literalIsInt = true, literalIsFloat = true;

        /* The literal must be at least one character long. */
        if (literal.length() < 1) {
            return null;
        }

        /* Check if the literal is a string. */
        /* If the literal is a string, it must begin and end with a single
           quote. */
        char firstChar = literal.charAt(0);
        char lastChar = literal.charAt(literal.length() - 1);
        if (firstChar != '\'' || lastChar != '\'') {
            literalIsString = false;
        }
        /* If the literal is a string, the value cannot contain newlines, tabs,
           commas, or quotes between the single quotes. */
        for (int j = 1; j < literal.length() - 1; j++) {
            char c = literal.charAt(j);
            if (c == '\n' || c == '\t' || c == ',' ||
                    c == '\'' || c == '\"')
            {
                literalIsString = false;
            }
        }

        /* If the literal is not a string, verify that it's an int or float. */
        if (!literalIsString) {
            /* If all the characters in the literal are digits (except the
               first one, which could be a negative sign), then it's an int. */
            if (!Character.isDigit(firstChar) && firstChar != '-') {
                literalIsInt = false;
            }
            for (int i = 1; i < literal.length(); i++) {
                if (!Character.isDigit(literal.charAt(i))) {
                    literalIsInt = false;
                }
            }

            /* If the literal is a float, then its first character can either be
               a decimal point, a negative sign, or a digit. Verify that the
               literal only contains digits and exactly one decimal point. */
            if (!Character.isDigit(firstChar) && firstChar != '-' &&
                    firstChar != '.') {
                literalIsFloat = false;
            }
            int decimalPointCount = 0;
            for (int j = 1; j < literal.length(); j++) {
                char c = literal.charAt(j);
                if (!Character.isDigit(c) && c != '.') {
                    literalIsFloat = false;
                }
                if (c == '.') {
                    decimalPointCount++;
                }
            }
            if (decimalPointCount != 1) {
                literalIsFloat = false;
            }
        }

        if (literalIsString) {
            return "string";
        }
        if (literalIsInt) {
            return "int";
        }
        if (literalIsFloat)
            return "float";

        return null;
    }

    /**
     * "Is valid column operation": Checks whether the operation between two
     * columns is valid by checking their types in the table.
     */
    private static boolean isValidColOperation(Table table, String name1,
                                            String operator, String name2)
    {
        /* Get the column types. */
        String type1 = table.getColType(name1);
        String type2 = table.getColType(name2);

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

    /**
     * "Is valid literal operation": Checks whether the operation between one
     * column and a literal is valid by checking their types. Also checks
     * if the literal is valid by seeing if it is a string, int, or float.
     */
    private static boolean isValidLitOperation(Table table, String name,
                                               String operator, String literal)
    {

        /* Get the column type of the column (argument 'name'). */
        String type1 = table.getColType(name);

        /* Get the type of the literal. */
        String type2 = getLitType(literal);

        /* If the literal is not a string, int, or float, then return false. */
        if (type2 == null) {
            return false;
        }

        /* If the column type is string, the literal MUST be a string. */
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

        /* The only valid operators are +, -, *, and /. */
        if (!operator.equals("+") && !operator.equals("-") &&
            !operator.equals("*") && !operator.equals("/"))
        {
            return false;
        }

        return true;
    }

    /**
     * "Is valid literal comparison": Checks whether the comparison between one
     * column and a literal is valid by checking their types. Also checks
     * if the literal is valid by seeing if it is a string, int, or float.
     */
    private static boolean isValidLitComparison(Table table, String name,
                                               String operator, String literal)
    {

        /* Get the column type of the column (argument 'name'). */
        String type1 = table.getColType(name);

        /* Get the type of the literal. */
        String type2 = getLitType(literal);

        /* If the literal is not a string, int, or float, then return false. */
        if (type2 == null) {
            return false;
        }

        /* If the column type is string, the literal MUST be a string. */
        if (type1.equals("string") && !type2.equals("string")) {
            return false;
        }
        if (!type1.equals("string") && type2.equals("string")) {
            return false;
        }

        /* The only valid operators are ==, !=, <, >, <=, and >=. */
        if (!operator.equals("==") && !operator.equals("!=") &&
            !operator.equals("<") && !operator.equals(">") &&
            !operator.equals("<=") && !operator.equals(">="))
        {
            return false;
        }

        return true;
    }

    /** Helper function for applyColOperation() and applyLitOperation(). */
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

    /** Helper function for passesColCondition() and passesLitCondition(). */
    private static boolean doStringComparison(String operand1, String operator,
                                        String operand2)
    {
        if (operator.equals("==")) {
            if (operand1.equals(operand2)) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("!=")) {
            if (!operand1.equals(operand2)) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("<")) {
            /* compareTo() should return negative int */
            if (operand1.compareTo(operand2) < 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals(">")) {
            /* compareTo() should return positive int */
            if (operand1.compareTo(operand2) > 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("<=")) {
            /* compareTo() should return negative int or 0 */
            int ret = operand1.compareTo(operand2);
            if (ret < 0 || ret == 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals(">=")) {
            /* compareTo() should return positive int or 0 */
            int ret = operand1.compareTo(operand2);
            if (ret > 0 || ret == 0) {
                return true;
            } else {
                return false;
            }
        }

        System.out.println("This line should never print (doStringComparison)");
        return false;
    }

    /** Helper function for passesColCondition() and passesLitCondition(). */
    private static boolean doNumComparison(String value1, String operator,
                                              String value2)
    {
        Float operand1 = Float.parseFloat(value1);
        Float operand2 = Float.parseFloat(value2);
        if (operator.equals("==")) {
            if (operand1.equals(operand2)) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("!=")) {
            if (!operand1.equals(operand2)) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("<")) {
            /* compareTo() should return negative int */
            if (operand1.compareTo(operand2) < 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals(">")) {
            /* compareTo() should return positive int */
            if (operand1.compareTo(operand2) > 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals("<=")) {
            /* compareTo() should return negative int or 0 */
            int ret = operand1.compareTo(operand2);
            if (ret < 0 || ret == 0) {
                return true;
            } else {
                return false;
            }
        } else if (operator.equals(">=")) {
            /* compareTo() should return positive int or 0 */
            int ret = operand1.compareTo(operand2);
            if (ret > 0 || ret == 0) {
                return true;
            } else {
                return false;
            }
        }

        System.out.println("This line should never print (doNumComparison)");
        return false;
    }

    /** Applies the operation to two arrays (columns) and returns the result. */
    private static String[] applyColOperation(Table table, String name1,
                                    String operator, String name2)
    {
        /* Get the column types. */
        String type1 = table.getColType(name1);
        String type2 = table.getColType(name2);

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

    /** Applies the operation to column w/ a literal and returns the result. */
    private static String[] applyLitOperation(Table table, String name1,
                                              String operator, String literal)
    {
        /* Get the column type and the literal type. */
        String type1 = table.getColType(name1);
        String type2 = getLitType(literal);

        /* Get the column values. */
        String[] values1 = table.getCol(name1);
        int numValues = values1.length;

        String[] ret = new String[numValues];

        /* If both types are string, the operation is "+". */
        if (type1.equals("string") && type2.equals("string")) {
            /* Concatenate all values with the literal. */
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

                for (int j = 0; j < literal.length(); j++) {
                    char c = literal.charAt(j);
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
            operand2 = Float.parseFloat(literal);

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

    /**
     * Tests whether a particular row in a table passes a condition when given
     * a column name and a literal.
     *
     * @return  true    if the row passes the condition
     *          false   otherwise
     */
    private static boolean passesLitCondition(Table table, String name1,
                                              String operator, String literal,
                                              int rowNum)
    {
        /* Get the column type and the literal type. */
        String type1 = table.getColType(name1);
        String type2 = getLitType(literal);

        /* Get the value in the column and row specified by name1 and rowNum. */
        String value = table.getVal(rowNum, name1);

        /* Return the result of condition test. */
        if (type1.equals("string") || type2.equals("string")) {
            return doStringComparison(value, operator, literal);
        } else {
            return doNumComparison(value, operator, literal);
        }
    }

    /**
     * Tests whether a particular row in a table passes a condition when given
     * two column names.
     *
     * @return  true    if the row passes the condition
     *          false   otherwise
     */
    private static boolean passesColCondition(Table table, String name1,
                                              String operator, String name2,
                                              int rowNum)
    {
        /* Get the column type and the literal type. */
        String type1 = table.getColType(name1);
        String type2 = table.getColType(name2);

        /* Get values in the columns and rows specified by name1 and rowNum. */
        String value1 = table.getVal(rowNum, name1);
        String value2 = table.getVal(rowNum, name2);

        /* Return the result of condition test. */
        if (type1.equals("string") || type2.equals("string")) {
            return doStringComparison(value1, operator, value2);
        } else {
            return doNumComparison(value1, operator, value2);
        }
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
            boolean operand2IsLiteral = false;
            String newColumnName = null;
            String substring = "";
            int numSpaces = 0;
            boolean countSpaces = true;
            for (int j = 0; j < expr.length(); j++) {
                char c = expr.charAt(j);
                /* It's possible that we encounter a literal like this ' Pie'.
                   In this case, the space character before the "Pie" should not
                   be counted. We do this by ignoring spaces until the closing
                   single quote. */
                if (c == '\'') {
                    countSpaces = !countSpaces;
                }
                if (c == ' ' && countSpaces) {
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
                /* Verify that the first (left) column name exists in table. */
                String name1 = table.addType(operand1);
                String name2 = table.addType(operand2);
                if (name1 == null) {
                    return null;
                }
                /* If the second column name does not exist in table, it is a
                   literal. */
                if (name2 == null) {
                    operand2IsLiteral = true;
                }

                /* Apply the operation. */
                String[] values = null;
                if (operand2IsLiteral) {
                    /* Verify that the operation with a literal is valid. */
                    if (!isValidLitOperation(table, name1, operator, operand2)) {
                        return null;
                    }
                    values = applyLitOperation(table, name1, operator,
                                               operand2);
                } else {
                    /* Verify that the operation with two columns is valid. */
                    if (!isValidColOperation(table, name1, operator, name2)) {
                        return null;
                    }
                    values = applyColOperation(table, name1, operator, name2);
                }

                /* Form the correct column values. */
                columnValues[i] = values;

                /* Attach the correct type to the newly formed column. */
                String type1 = table.getColType(name1), type2 = null;
                if (!operand2IsLiteral) {
                    type2 = table.getColType(name2);
                } else {
                    type2 = getLitType(operand2);
                }
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
                boolean countSpaces = true;
                for (int j = 0; j < cond.length(); j++) {
                    char c = cond.charAt(j);
                    /* It's possible that we encounter a literal like this ' Pie'.
                   In this case, the space character before the "Pie" should not
                   be counted. We do this by ignoring spaces until the closing
                   single quote. */
                    if (c == '\'') {
                        countSpaces = !countSpaces;
                    }
                    if (c == ' ' && countSpaces) {
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

                /* Verify that operand1 is an existing column name in table. */
                String name1 = table.addType(operand1);
                String name2 = table.addType(operand2);
                boolean operand2IsLiteral = false;
                if (name1 == null) {
                    return null;
                }

                /* If name2 is null, then operand2 is a literal. Else it is a
                   column name. */
                if (name2 == null) {
                    operand2IsLiteral = true;
                }

                /* Apply the condition. If the row fails any condition, do not
                   add it to the new table. */
                int rowNum = i;
                if (operand2IsLiteral) {
                    if (!isValidLitComparison(table, name1, operator,
                                              operand2))
                    {
                        return null;
                    }
                    if (!passesLitCondition(table, name1, operator, operand2,
                                           rowNum))
                    {
                        shouldAdd = false;
                        break;
                    }
                } else {
                    if (!isValidLitComparison(table, name1, operator, name2)) {
                        return null;
                    }
                    if (!passesColCondition(table, name1, operator, name2,
                                            rowNum))
                    {
                        shouldAdd = false;
                        break;
                    }
                }
            }
            /* If the row passes all the conditions, add it to the new table. */
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
