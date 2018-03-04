package db;

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

    public Database() {
        tables = new ArrayList<Table>();
        numTables = 0;
    }

    public String transact(String query) {
        return eval(query);
    }

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

    private static String createSelectedTable(String name, String exprs, String
            tables, String conds) {
        return "You are trying to create a table named " + name +
                " by selecting these " + "expressions: '" + exprs +
                "' from the join of these tables: '" + tables +
                "', filtered by these conditions: '" + conds + "'\n";
    }

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

    private static String dropTable(String name) {
        Table table = findTable(name);
        if (table != null) {
            tables.remove(table);
            return "";
        } else {
            return "ERROR: No such table: " + name + "\n";
        }
    }

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
                return "ERROR: Values do not match corresponding column " +
                        "types!\n";
            }
        } else {
            return "ERROR: No such table: " + name + "\n";
        }
        return "";
    }

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
     * Evaluates and applies column expressions to a table to choose which
     * columns to return.
     *
     * @param table the table to apply the expressions to
     * @param exprs the column expressions
     */
    private static Table evalExprs(Table table, String[] exprs) {
        /* If the expression is "*", return all columns. */
        if (exprs.length == 1 && exprs[0].equals("*")) {
            return table;
        }


        /* For each expression... */
        for (int i = 0; i < exprs.length; i++) {
            String expr = exprs[i];

            /* Check how many operands there are: 1 or 2. There are 2 operands
               i.f.f. there are 4 spaces in the expression (ex: "a + b as sum").
               Any other number of spaces is an error. */
            int numSpaces = 0;
            for (int j = 0; j < expr.length(); j++) {
                if (expr.charAt(j) == ' ') {
                    numSpaces++;
                }
            }
            if (numSpaces == 0) {           // one operand

            } else if (numSpaces == 4) {    // two operands

            } else {                        // error
                return null;
            }

        }
        return null;
    }

    /**
     * Evaluates and applies conditions to a table to choose which rows to
     * return.
     */
    private static Table evalConds(Table table, String[] conds) {
        return null;
    }

    private static String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return "Malformed select: " + expr;
        }

        return select(m.group(1), m.group(2), m.group(3));
    }

    private static String select(String exprs, String tables, String conds) {
        /* Split up the lines into processable arrays. */
        String[] columnExprs = null, names = null, conditions = null;
        if (exprs != null) {
            columnExprs = lineToArr(exprs);
        }
        if (tables != null) {
            names = lineToArr(tables);
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
        if (conditions != null) {
            conditionedTable = evalConds(expressedTable, conditions);
        }


        return joinedTable.toString();
//        return "You are trying to select these expressions: \'" + exprs +
//                "\' from the join of these tables: \'" + tables + "\', " +
//                "filtered by these conditions: '" + conds + "\'\n";
    }
}
