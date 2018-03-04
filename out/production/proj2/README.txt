(IN PROGRESS: Implementing Database language parser)

This is the capstone project in UC Berkeley's CS 61B, where you implement a
relational database management system that a user can interact with through
a language similar to SQL.

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
    2.) ...

Gradescope Autograder Score: (IN PROGRESS)

-------------------------------------------------------------------------------

This is Project 2 from UC Berkeley's CS 61B Spring 2017 by Professor Josh Hug:
http://datastructur.es/sp17/

Graded using UC Berkeley's public autograder on Gradescope.

-------------------------------------------------------------------------------
Notes for self:
    1. Stop using == to compare Strings. You must use String.equals().

