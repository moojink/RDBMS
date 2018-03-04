package db;

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests the Table class.
 * @author Moo Jin Kim
 */
public class TableTest {

    /** Tests getRow(). */
    @Test
    public void testGetRow() {
        String[] columns = new String[]{"X int", "Y int"};
        Table T1 = new Table(columns);
        String[] row1 = new String[]{"2", "5"};
        String[] row2 = new String[]{"8", "3"};
        String[] row3 = new String[]{"14", "7"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);

        assertEquals(columns, T1.getRow(0));
        assertEquals(row1, T1.getRow(1));
        assertEquals(row2, T1.getRow(2));
        assertEquals(row3, T1.getRow(3));
    }

    /** Tests getCol(int N) and getCol(String columnName). */
    @Test
    public void testGetCol() {
        String[] columns = new String[]{"X int", "Y int"};
        Table T1 = new Table(columns);
        String[] row1 = new String[]{"2", "5"};
        String[] row2 = new String[]{"8", "3"};
        String[] row3 = new String[]{"14", "7"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);

        String[] col1 = new String[]{"2", "8", "14"};
        String[] col2 = new String[]{"5", "3", "7"};

        assertEquals(col1, T1.getCol(1));
        assertEquals(col2, T1.getCol(2));
        assertEquals(col1, T1.getCol("X int"));
        assertEquals(col2, T1.getCol("Y int"));
    }

    /** Tests getVal(int row, int col) and getVal(int N, String name). */
    @Test
    public void testGetVal() {
        String[] columns = new String[]{"X int", "Y int"};
        Table T1 = new Table(columns);
        String[] row1 = new String[]{"2", "5"};
        String[] row2 = new String[]{"8", "3"};
        String[] row3 = new String[]{"14", "7"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);

        /* getVal(int row, int col) */
        assertEquals(null, T1.getVal(0, 1));
        assertEquals(null, T1.getVal(1, 0));
        assertEquals(null, T1.getVal(4, 1));
        assertEquals(null, T1.getVal(1, 3));
        assertEquals("2", T1.getVal(1, 1));
        assertEquals("5", T1.getVal(1, 2));
        assertEquals("8", T1.getVal(2, 1));
        assertEquals("3", T1.getVal(2, 2));
        assertEquals("14", T1.getVal(3, 1));
        assertEquals("7", T1.getVal(3, 2));

        /* getVal(int N, String name) */
        assertEquals(null, T1.getVal(0, 1));
        assertEquals(null, T1.getVal(1, 0));
        assertEquals(null, T1.getVal(4, 1));
        assertEquals(null, T1.getVal(1, 3));
        assertEquals("2", T1.getVal(1, "X int"));
        assertEquals("5", T1.getVal(1, "Y int"));
        assertEquals("8", T1.getVal(2, "X int"));
        assertEquals("3", T1.getVal(2, "Y int"));
        assertEquals("14", T1.getVal(3, "X int"));
        assertEquals("7", T1.getVal(3, "Y int"));
    }

    /** Tests getRestOfRow(). */
    @Test
    public void testGetRestOfRow() {
        String[] columns = new String[]{"X int", "Y int", "Z int"};
        Table T1 = new Table(columns);
        String[] row1 = new String[]{"2", "5", "7"};
        String[] row2 = new String[]{"8", "3", "8"};
        String[] row3 = new String[]{"14", "7", "9"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);

        String[] unwantedCol = new String[]{"Y int"};
        /* Rest of rows. */
        String[] ror1 = new String[] {"2", "7"};
        String[] ror2 = new String[] {"8", "8"};
        String[] ror3 = new String[] {"14", "9"};

        assertEquals(ror1, T1.getRestOfRow(1, unwantedCol));
        assertEquals(ror2, T1.getRestOfRow(2, unwantedCol));
        assertEquals(ror3, T1.getRestOfRow(3, unwantedCol));
    }

    /** Tests sharedColumns(). */
    @Test
    public void testSharedColumns() {
        String[] columns1 = new String[]{"W int", "X int", "Y int", "Z int"};
        Table t1 = new Table(columns1);

        String[] columns2 = new String[]{"A int", "Y int", "Z int", "B int"};
        Table t2 = new Table(columns2);

        ArrayList<String> columns = Table.sharedColumns(t1, t2);
        assertEquals(true, columns.contains("Y int"));
        assertEquals(true, columns.contains("Z int"));
        assertEquals(2, columns.size());
    }

    /** Tests uniqueColumns(). */
    @Test
    public void testUniqueColumns() {
        String[] columns1 = new String[]{"W int", "X int", "Y int", "Z int"};
        Table t1 = new Table(columns1);

        String[] columns2 = new String[]{"A int", "Y int", "Z int", "B int"};
        Table t2 = new Table(columns2);

        ArrayList<String> unique_t1 = Table.uniqueColumns(t1, t2);
        ArrayList<String> unique_t2 = Table.uniqueColumns(t2, t1);

        assertEquals(true, unique_t1.contains("W int"));
        assertEquals(true, unique_t1.contains("X int"));
        assertEquals(true, unique_t2.contains("A int"));
        assertEquals(true, unique_t2.contains("B int"));
        assertEquals(2, unique_t1.size());
        assertEquals(2, unique_t2.size());
    }

    /** Tests join(). */
    @Test
    public void testJoin() {
        /* Part 1: Cartesian Product of two tables. */
        String[] columns1 = new String[]{"X int", "Y int"};
        Table t1 = new Table(columns1);
        String[] columns2 = new String[]{"A int", "B int"};
        Table t2 = new Table(columns2);

        String[] t1_row1 = new String[]{"1", "2"};
        String[] t1_row2 = new String[]{"3", "4"};
        String[] t2_row1 = new String[]{"5", "6"};
        String[] t2_row2 = new String[]{"7", "8"};
        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);

        Table joinedTable = Table.join(t1, t2);
        String[] jt_row0 = new String[]{"X int", "Y int", "A int", "B int"};
        String[] jt_row1 = new String[]{"1", "2", "5", "6"};
        String[] jt_row2 = new String[]{"1", "2", "7", "8"};
        String[] jt_row3 = new String[]{"3", "4", "5", "6"};
        String[] jt_row4 = new String[]{"3", "4", "7", "8"};
        assertEquals(4, joinedTable.getNumColumns());
        assertEquals(4, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));
        assertEquals(jt_row3, joinedTable.getRow(3));
        assertEquals(jt_row4, joinedTable.getRow(4));

        System.out.println("Test 1 (Cartesian Product):");
        joinedTable.printTable();
        /*
            Should print:
            X int,Y int,A int,B int
            1,2,a,b
            1,2,c,d
            3,4,a,b
            3,4,c,d
         */

        /* Part 2: Merging of rows. */

        /* Part 2.0 */
        //  Table 1     Table 2     Table 3 (Joined)
        //  [x ][y ]    [x ][z ]    [x ][y ][z ]
        //  [1 ][7 ]    [3 ][8 ]    (empty)
        //  [7 ][7 ]    [4 ][9 ]
        //  [1 ][9 ]    [5 ][10]

        columns1 = new String[]{"x int", "y int"};
        columns2 = new String[]{"x int", "z int"};
        t1 = new Table(columns1);
        t2 = new Table(columns2);

        t1_row1 = new String[]{"1", "7"};
        t1_row2 = new String[]{"7", "7"};
        String[] t1_row3 = new String[]{"1", "9"};
        t2_row1 = new String[]{"3", "8"};
        t2_row2 = new String[]{"4", "9"};
        String[] t2_row3 = new String[]{"5", "10"};

        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t1.addRow(t1_row3);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);
        t2.addRow(t2_row3);

        joinedTable = Table.join(t1, t2);

        jt_row0 = new String[]{"x int", "y int", "z int"};
        assertEquals(3, joinedTable.getNumColumns());
        assertEquals(0, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));

        System.out.println("Test 2.0 (Merge)");
        joinedTable.printTable();
        /*
            Should print:
            Test 2.0 (Merge)
            x int,y int,z int
         */


        /* Part 2.1 */
        //  Table 1     Table 2     Table 3 (Joined)
        //  [x ][y ]    [x ][z ]    [x ][y ][z ]
        //  [2 ][5 ]    [2 ][4 ]    [2 ][5 ][4 ]
        //  [8 ][3 ]    [8 ][9 ]    [8 ][3 ][9 ]
        //  [13][7 ]    [10][1 ]
        //              [11][1 ]

        columns1 = new String[]{"x int", "y int"};
        columns2 = new String[]{"x int", "z int"};
        t1 = new Table(columns1);
        t2 = new Table(columns2);

        String[] t2_row4;
        t1_row1 = new String[]{"2", "5"};
        t1_row2 = new String[]{"8", "3"};
        t1_row3 = new String[]{"13", "7"};
        t2_row1 = new String[]{"2", "4"};
        t2_row2 = new String[]{"8", "9"};
        t2_row3 = new String[]{"10", "1"};
        t2_row4 = new String[]{"11", "1"};

        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t1.addRow(t1_row3);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);
        t2.addRow(t2_row3);
        t2.addRow(t2_row4);

        joinedTable = Table.join(t1, t2);

        jt_row0 = new String[]{"x int", "y int", "z int"};
        jt_row1 = new String[]{"2", "5", "4"};
        jt_row2 = new String[]{"8", "3", "9"};
        assertEquals(3, joinedTable.getNumColumns());
        assertEquals(2, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));

        System.out.println("Test 2.1 (Merge)");
        joinedTable.printTable();
        /*
            Should print:
            Test 2.1 (Merge)
            x int,y int,z int
            2,5,4
            8,3,9
         */


        /* Part 2.2 */
        //  Table 1     Table 2     Table 3 (Joined)
        //  [x ][y ]    [x ][z ]    [x ][y ][z ]
        //  [1 ][4 ]    [1 ][7 ]    [1 ][4 ][7 ]
        //  [2 ][5 ]    [7 ][7 ]    [1 ][4 ][9 ]
        //  [3 ][6 ]    [1 ][9 ]    [1 ][4 ][11]
        //              [1 ][11]

        columns1 = new String[]{"x int", "y int"};
        columns2 = new String[]{"x int", "z int"};
        t1 = new Table(columns1);
        t2 = new Table(columns2);

        t1_row1 = new String[]{"1", "4"};
        t1_row2 = new String[]{"2", "5"};
        t1_row3 = new String[]{"3", "6"};
        t2_row1 = new String[]{"1", "7"};
        t2_row2 = new String[]{"7", "7"};
        t2_row3 = new String[]{"1", "9"};
        t2_row4 = new String[]{"1", "11"};

        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t1.addRow(t1_row3);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);
        t2.addRow(t2_row3);
        t2.addRow(t2_row4);

        joinedTable = Table.join(t1, t2);

        jt_row0 = new String[]{"x int", "y int", "z int"};
        jt_row1 = new String[]{"1", "4", "7"};
        jt_row2 = new String[]{"1", "4", "9"};
        jt_row3 = new String[]{"1", "4", "11"};
        assertEquals(3, joinedTable.getNumColumns());
        assertEquals(3, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));
        assertEquals(jt_row3, joinedTable.getRow(3));

        System.out.println("Test 2.2 (Merge)");
        joinedTable.printTable();
        /*
            Should print:
            Test 2.2 (Merge)
            x int,y int,z int
            1,4,7
            1,4,9
            1,4,11
         */


        /* Part 2.3 */
        //  Table 1             Table 2         Table 3 (Joined)
        //  [x ][y ][z ][w ]    [w ][b ][z ]    [z ][w ][x ][y ][b ]
        //  [1 ][7 ][2 ][10]    [1 ][7 ][4 ]    [4 ][1 ][7 ][7 ][7 ]
        //  [7 ][7 ][4 ][1 ]    [7 ][7 ][3 ]    [9 ][1 ][1 ][9 ][11]
        //  [1 ][9 ][9 ][1 ]    [1 ][9 ][6 ]
        //                      [1 ][11][9 ]

        columns1 = new String[]{"x int", "y int", "z int", "w int"};
        columns2 = new String[]{"w int", "b int", "z int"};
        t1 = new Table(columns1);
        t2 = new Table(columns2);

        t1_row1 = new String[]{"1", "7", "2", "10"};
        t1_row2 = new String[]{"7", "7", "4", "1"};
        t1_row3 = new String[]{"1", "9", "9", "1"};
        t2_row1 = new String[]{"1", "7", "4"};
        t2_row2 = new String[]{"7", "7", "3"};
        t2_row3 = new String[]{"1", "9", "6"};
        t2_row4 = new String[]{"1", "11", "9"};

        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t1.addRow(t1_row3);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);
        t2.addRow(t2_row3);
        t2.addRow(t2_row4);

        joinedTable = Table.join(t1, t2);

        jt_row0 = new String[]{"z int", "w int", "x int", "y int", "b int"};
        jt_row1 = new String[]{"4", "1", "7", "7", "7"};
        jt_row2 = new String[]{"9", "1", "1", "9", "11"};
        assertEquals(5, joinedTable.getNumColumns());
        assertEquals(2, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));

        System.out.println("Test 2.3 (Merge)");
        joinedTable.printTable();
        /*
            Should print:
            Test 2.3 (Merge)
            z int,w int,x int,y int,b int
            4,1,7,7,7
            9,1,1,9,11
         */



        /* Part 3: Joining 3+ tables. Using tables t1 and t2 from Test 2.3.*/

        /* Test 3.0: Merge 3 tables */
        //  Table 1             Table 2         Table 3
        //  [x ][y ][z ][w ]    [w ][b ][z ]    [z ][w ][c ]
        //  [1 ][7 ][2 ][10]    [1 ][7 ][4 ]    [4 ][1 ][7 ]
        //  [7 ][7 ][4 ][1 ]    [7 ][7 ][3 ]    [9 ][1 ][2 ]
        //  [1 ][9 ][9 ][1 ]    [1 ][9 ][6 ]    [9 ][4 ][5 ]
        //                      [1 ][11][9 ]
        //
        //  Joined Table
        //  [z ][w ][x ][y ][b ][c ]
        //  [4 ][1 ][7 ][7 ][7 ][7 ]
        //  [9 ][1 ][1 ][9 ][11][2 ]


        String[] columns3 = new String[]{"z int", "w int", "c int"};
        String[] t3_row1 = new String[]{"4", "1", "7"};
        String[] t3_row2 = new String[]{"9", "1", "2"};
        String[] t3_row3 = new String[]{"9", "4", "5"};
        Table t3 = new Table(columns3);
        t3.addRow(t3_row1);
        t3.addRow(t3_row2);
        t3.addRow(t3_row3);
        Table[] arr = new Table[]{t1, t2, t3};

        joinedTable = Table.join(arr);
        jt_row0 = new String[]{"z int", "w int", "x int", "y int", "b int", "c int"};
        jt_row1 = new String[]{"4", "1", "7", "7", "7", "7"};
        jt_row2 = new String[]{"9", "1", "1", "9", "11", "2"};
        assertEquals(6, joinedTable.getNumColumns());
        assertEquals(2, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));

        System.out.println("Test 3.0 (Merge 3 tables)");
        joinedTable.printTable();
        /*
            Should print:
            Test 3.0 (Merge 3 tables)
            z int,w int,x int,y int,b int,c int
            4,1,7,7,7,7
            9,1,1,9,11,2
         */
    }

    @Test
    public void testSetColumnTypes() {
        /* Test 1: Returns true */
        String[] columns1 = new String[]{"X int", "Y string", "Z float"};
        Table t1 = new Table(columns1);
        assertEquals(t1.setColumnTypes(columns1), true);
        String[] columnTypes1 = t1.getColumnTypes();
        assertEquals(columnTypes1[0], "int");
        assertEquals(columnTypes1[1], "string");
        assertEquals(columnTypes1[2], "float");

        /* Test 2: Returns false */
        columns1 = new String[]{"X int ", "Y string", "Z float"};
        t1 = new Table(columns1);
        assertEquals(t1.setColumnTypes(columns1), false);

        /* Test 3: Returns false */
        columns1 = new String[]{"X in", "Y string", "Z float"};
        t1 = new Table(columns1);
        assertEquals(t1.setColumnTypes(columns1), false);

        /* Test 2: Returns false */
        columns1 = new String[]{"X int", "Y String", "Z float"};
        t1 = new Table(columns1);
        assertEquals(t1.setColumnTypes(columns1), false);
    }

    public static void main(String[] args) {
        jh61b.junit.TestRunner.runTests("all", TableTest.class);
    }
}
