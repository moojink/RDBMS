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
        Table T1 = new Table(2, columns);
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
        Table T1 = new Table(2, columns);
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

    /** Tests getVal(). */
    @Test
    public void testGetVal() {
        String[] columns = new String[]{"X int", "Y int"};
        Table T1 = new Table(2, columns);
        String[] row1 = new String[]{"2", "5"};
        String[] row2 = new String[]{"8", "3"};
        String[] row3 = new String[]{"14", "7"};
        T1.addRow(row1);
        T1.addRow(row2);
        T1.addRow(row3);

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
    }

    /** Tests sharedColumns(). */
    @Test
    public void testSharedColumns() {
        String[] columns1 = new String[]{"W", "X", "Y", "Z"};
        Table t1 = new Table(4, columns1);

        String[] columns2 = new String[]{"A", "Y", "Z", "B"};
        Table t2 = new Table(4, columns2);

        ArrayList<String> columns = Table.sharedColumns(t1, t2);
        assertEquals(true, columns.contains("Y"));
        assertEquals(true, columns.contains("Z"));
        assertEquals(2, columns.size());
    }

    /** Tests uniqueColumns(). */
    @Test
    public void testUniqueColumns() {
        String[] columns1 = new String[]{"W", "X", "Y", "Z"};
        Table t1 = new Table(4, columns1);

        String[] columns2 = new String[]{"A", "Y", "Z", "B"};
        Table t2 = new Table(4, columns2);

        ArrayList<String> unique_t1 = Table.uniqueColumns(t1, t2);
        ArrayList<String> unique_t2 = Table.uniqueColumns(t2, t1);

        assertEquals(true, unique_t1.contains("W"));
        assertEquals(true, unique_t1.contains("X"));
        assertEquals(true, unique_t2.contains("A"));
        assertEquals(true, unique_t2.contains("B"));
        assertEquals(2, unique_t1.size());
        assertEquals(2, unique_t2.size());
    }

    /** Tests join(). */
    @Test
    public void testJoin() {
        /* Part 1: Cartesian Product of two tables. */
        String[] columns1 = new String[]{"X", "Y"};
        Table t1 = new Table(2, columns1);
        String[] columns2 = new String[]{"A", "B"};
        Table t2 = new Table(2, columns2);

        String[] t1_row1 = new String[]{"1", "2"};
        String[] t1_row2 = new String[]{"3", "4"};
        String[] t2_row1 = new String[]{"a", "b"};
        String[] t2_row2 = new String[]{"c", "d"};
        t1.addRow(t1_row1);
        t1.addRow(t1_row2);
        t2.addRow(t2_row1);
        t2.addRow(t2_row2);

        Table joinedTable = Table.join(t1, t2);
        String[] jt_row0 = new String[]{"X", "Y", "A", "B"};
        String[] jt_row1 = new String[]{"1", "2", "a", "b"};
        String[] jt_row2 = new String[]{"1", "2", "c", "d"};
        String[] jt_row3 = new String[]{"3", "4", "a", "b"};
        String[] jt_row4 = new String[]{"3", "4", "c", "d"};
        assertEquals(4, joinedTable.getNumColumns());
        assertEquals(4, joinedTable.getNumRows()); // excludes row 0 from count
        assertEquals(jt_row0, joinedTable.getRow(0));
        assertEquals(jt_row1, joinedTable.getRow(1));
        assertEquals(jt_row2, joinedTable.getRow(2));
        assertEquals(jt_row3, joinedTable.getRow(3));
        assertEquals(jt_row4, joinedTable.getRow(4));

        joinedTable.printTable();
        /*
            Should print:
            X	Y	A	B
            1	2	a	b
            1	2	c	d
            3	4	a	b
            3	4	c	d
         */

        /* Part 2: Merging of rows. */
    }

    public static void main(String[] args) {
        jh61b.junit.TestRunner.runTests("all", TableTest.class);
    }
}
