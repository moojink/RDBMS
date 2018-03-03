package db;

import java.util.List;
import java.util.ArrayList;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.StringJoiner;

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
        if ((m = LOAD_CMD.matcher(query)).matches()) {
            result = loadTable(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            result = printTable(m.group(1));
        }

//        if ((m = CREATE_CMD.matcher(query)).matches()) {
//            result = createTable(m.group(1));
//        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
//            result = loadTable(m.group(1));
//        } else if ((m = STORE_CMD.matcher(query)).matches()) {
//            result = storeTable(m.group(1));
//        } else if ((m = DROP_CMD.matcher(query)).matches()) {
//            result = dropTable(m.group(1));
//        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
//            result = insertRow(m.group(1));
//        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
//            result = printTable(m.group(1));
//        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
//            result = select(m.group(1));
//        } else {
//            result = "ERROR: Malformed query.\n";
//        }
        return result;
    }

//    private static void createTable(String expr) {
//        Matcher m;
//        if ((m = CREATE_NEW.matcher(expr)).matches()) {
//            createNewTable(m.group(1), m.group(2).split(COMMA));
//        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
//            createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
//        } else {
//            System.err.printf("Malformed create: %s\n", expr);
//        }
//    }
//
//    private static void createNewTable(String name, String[] cols) {
//        StringJoiner joiner = new StringJoiner(", ");
//        for (int i = 0; i < cols.length-1; i++) {
//            joiner.add(cols[i]);
//        }
//
//        String colSentence = joiner.toString() + " and " + cols[cols.length-1];
//        System.out.printf("You are trying to create a table named %s with the columns %s\n", name, colSentence);
//    }
//
//    private static void createSelectedTable(String name, String exprs, String tables, String conds) {
//        System.out.printf("You are trying to create a table named %s by selecting these expressions:" +
//                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", name, exprs, tables, conds);
//    }

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
            table = new Table(columnNames);

            /* Process all other lines. */
            String row[] = new String[table.getNumColumns()];
            String value = "";
            int index = 0;
            while ((line = reader.readLine()) != null) {
                lineLength = line.length();
                for (int i = 0; i < lineLength; i++) {
                    char c = line.charAt(i);
                    /* Upon finding comma, add the value to row. */
                    if (c == ',') {
                        row[index] = value;
                        index++;
                        value = "";
                        continue;
                    }
                    value += c;
                }
                row[index] = value;    // add last value
                table.addRow(row);
                index = 0;
                value = "";
            }
            table.setName(name);
            tables.add(table);
            reader.close();
        } catch (FileNotFoundException e) {
            return "ERROR: " + filename + " not found.\n";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

//    private static void storeTable(String name) {
//        System.out.printf("You are trying to store the table named %s\n", name);
//    }
//
//    private static void dropTable(String name) {
//        System.out.printf("You are trying to drop the table named %s\n", name);
//    }
//
//    private static void insertRow(String expr) {
//        Matcher m = INSERT_CLS.matcher(expr);
//        if (!m.matches()) {
//            System.err.printf("Malformed insert: %s\n", expr);
//            return;
//        }
//
//        System.out.printf("You are trying to insert the row \"%s\" into the table %s\n", m.group(2), m.group(1));
//    }

    private static String printTable(String name) {
        for (Table table : tables) {
            String temp = table.getName();
            if (temp.equals(name)) {
                table.printTable();
                return "";
            }
        }
        return "ERROR: No such table: " + name;
    }

//    private static void select(String expr) {
//        Matcher m = SELECT_CLS.matcher(expr);
//        if (!m.matches()) {
//            System.err.printf("Malformed select: %s\n", expr);
//            return;
//        }
//
//        select(m.group(1), m.group(2), m.group(3));
//    }
//
//    private static void select(String exprs, String tables, String conds) {
//        System.out.printf("You are trying to select these expressions:" +
//                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", exprs, tables, conds);
//    }
}
