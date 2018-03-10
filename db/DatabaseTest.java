package db;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Scanner;

/** Unit tests the Database class. */
public class DatabaseTest {
    @Test
    public void exampleRun() {
        String data = "load fanss";
        InputStream stdin = System.in;
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes()));
            Scanner scanner = new Scanner(System.in);
            System.out.println(scanner.nextLine());
        } finally {
            System.setIn(stdin);
        }
    }

    /** Runs all the unit tests in the DatabaseTest file. */
    public static void main(String[] args) {
        jh61b.junit.TestRunner.runTests("all", DatabaseTest.class);
    }
}
