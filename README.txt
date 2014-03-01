Name: Paul Rodriguez
sid:303675125

project 2

- i did not use pointers to access the records in the nodes. i used the function memcpy since it was more efficient for me and easier to understand than keeping track of pointers.

-i had to edit most of the implementation of the insertAndSplit function for the BTNonLeafNode since it was not inserting the keys correctly
and i also had to add a new function locateKey  for the BTNonLeafNode class so that it is easier to locate keys when inserting and splitting.

-i added constructors to BTLeafNode and BTNonLeafNode to make their initializations easier and not having to call other functions.

-the leaf nodes can hold 84 entries and the last 12 bytes hold the number of entries, the pointer to next leaf and the type of node.
the nonleaf nodes hold 100 records and the last 8 bytes contain the type of nodes and the number of entries.

- i had to add a function startPid to the BTNonLeafNode object so that i would be able to insert the the first pid in a nonleaf.

- i needed to change the locateKey function that i added to the BTreeIndex object since it was not inserting entries correctly when the height of the tree became 2 or larger, and i changed the function's name to something more meaningful. i also changed some stuff in the insert function of BTreeIndex since it would not split leafs correctly.

- i changed the bound in an if loop for the readForward function since it would read for the maximum number of entries, even if those entries were empty, so i changed it so that it would read the entries depending on how many there were in the node.

- the functions readdata and writedata are used for reading the first page in the BTreeIndex, which holds the rootPid and height of tree, and write it back when insertion occurs

-for the SqlEngine select function i make sure that it used the index if a table has an index, and make sure to skip reads to record file if the query only involves selecting count(*), selecting key or only checking on key conditions in the where clause

- it will only use the tree if there exists an index file and there where conditions on the key in the where clause, or if we select count(*) and there where no conditions on value 

-it first iterates through all the conditions to find a lowerbound and upperbound for keys so that it only checks on that range and exits if we go past it

- i set the select function in a way so that it does not do the search if there is two equality conditions in the where clause, unless the values to check are the same
example: it stops search if query is something like "select * from xlarge where key=400 and key=401", but it searches if its something like "select * from xlarge where key=400 and key=400"

- the test queries in 'test.sql' should print the correct results
