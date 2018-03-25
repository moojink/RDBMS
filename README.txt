Project Overview:
    This is Project 2 from UC Berkeley's CS 61B (Spring 2017 by Professor Josh
    Hug), a massive capstone project in which you use Java to implement a
    relational database management system that a user can interact with via a
    language similar to SQL.

SCORE: 115.394 / 100.0
    The average score among actual Berkeley students enrolled in the course
    was somewhere in the 40s or 50s. Extra credit was awarded for passing
    extra test cases. Graded using UC Berkeley's public autograder on
    Gradescope.

Full Project Specifications:
    http://datastructur.es/sp17/materials/proj/proj2/proj2.html
    (HTML file included in the main directory: proj2.htm)

===============================================================================

I learned how to:
    1.) Design a Table data structure after experimenting with standard data
        structures like maps, lists, sets, and arrays.

        Final Table Design (See Table.java for more details):
            LinkedHashMap<Integer, LinkedHashMap<String, String>>

        Some designs I thought about but abandoned after realizing flaws:
            a.) String[][]
            b.) HashMap<String, ArrayList>
            c.) LinkedHashMap<String, LinkedHashMap<String, String>>
                This one has a subtle difference from the final design, but
                its use is too complicated to explain and eventually becomes a
                nasty problem in the join() function.

    2.) Implement a database and relational management features, such as the
        merging of two tables (via the "natural inner join" algorithm) and the
        evaluation of column expressions and conditions used to extract desired
        columns and rows from tables.

    3.) Set a standard table format (.tbl), extract data from .tbl files
        to load tables into the database, and store data from the database
        into new .tbl files.

    4.) Parse through and put into effect commands that the user inputs to
        interact with the database, accounting for various combinations of
        whitespace (a lot harder than it sounds, and honestly quite painful).

    5.) Juggle this huge project with the work piled on by my Operating Systems
        and Computer Architecture classes at UCLA in Winter 2018.

===============================================================================

Notes to self:
    1.) Stop using == to compare Strings. Use String.equals().
    2.) Float comparisons using operators like ==, !=, etc. are incorrect.
       Use Float.compareTo().
