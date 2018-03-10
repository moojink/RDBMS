[IN PROGRESS: Working on special value NaN]

-------------------------------------------------------------------------------

Project Overview:
    This is Project 2 from UC Berkeley's CS 61B (Spring 2017 by Professor Josh
    Hug), a massive capstone project in which you use Java to implement a
    relational database management system that a user can interact with via a
    language similar to SQL.

Full Project Specifications:
    http://datastructur.es/sp17/materials/proj/proj2/proj2.html

Score: [IN PROGRESS]
    Graded using UC Berkeley's public autograder on Gradescope.


-------------------------------------------------------------------------------

I learned how to:
    1.) Design and implement a Table structure by experimenting with different
        data structures (maps, lists, sets, arrays).
        Final design:
            LinkedHashMap<Integer, LinkedHashMap<String, String>>
                See Table.java for more details.
        Flawed design attempts:
            a.) String[][]
            b.) HashMap<String, ArrayList>
            c.) LinkedHashMap<String, LinkedHashMap<String, String>>
                This one has a subtle difference from the final design, but
                its use is too complicated to explain and eventually becomes a
                nasty problem in the join() function.
    2.) Set a standard table format (.tbl) and parse through it to add data to
        a database.
    3.) Implement a database and the corresponding management features.
    4.) Parse through and put into effect commands that the user inputs to
        interact with the database.
    5.) ...

-------------------------------------------------------------------------------

Notes to self:
    1. Stop using == to compare Strings. You must use String.equals().

