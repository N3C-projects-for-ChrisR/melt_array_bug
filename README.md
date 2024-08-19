# melt_array_bug

Some code that's doing R-like reshape operations may be contributing 
to a bug in the system.  It's a challenge if you haven't used the 
PySpark SQL functions much, especially some of the tricks for 
creating structs and arrays. Once I read past that, I saw the join 
problem.

Easier to see here with  a small set of data.

- melt_df.py is the smaller inner function. Comments added here 
about how the sql functions work.
- melt_array.py adds a column number and this code has a little more
test data.

