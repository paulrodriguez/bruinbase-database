/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
	treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc;
	if((rc = pf.open(indexname, mode))<0) {
		return RC_FILE_OPEN_FAILED;
	}
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC rc = pf.close();
	if(rc < 0) {
		return RC_FILE_CLOSE_FAILED;
	}
    return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	//check this when a file has just been created
	RC rc;
	//if the tree is empty. then insert the record
	if(treeHeight == 0) {
		BTLeafNode rootleaf;
		rc = rootleaf.insert(key, rid);
		if(rc<0) return rc;
		//get the end id of the 
		rootPid = pf.endPid();
		//file was initially empty
		if(rootPid==0) {
		
			rootPid++;
		}
		rc = rootleaf.write(rootPid, pf);
		//write to the page with new pid
		if(rc<0) {
			
			return rc;
		}
		//increase tree height
		treeHeight++;
		return 0;

	}
	else if (treeHeight == 1) {
		//the root is actually a leaf node. i.e. there is only one node the tree
		PageId siblingpid;
		PageId rpid = rootPid;
		BTLeafNode currentroot;
		rc = currentroot.read(rpid, pf);
		//if insert fails, that means the node was full
		if((rc = currentroot.insert(key, rid)) < 0) {
			//create a new sibling node
			BTLeafNode sibling;
			int siblingKey;
			//insert and split from current node and sibling node
			rc = currentroot.insertAndSplit(key, rid, sibling, siblingKey);
			if(rc<0) {return rc;}
			
			//write the sibling to the end of the file. this should return 1
			siblingpid =  pf.endPid();
			rc = sibling.write(siblingpid, pf);
			//set current node's pointer to next node
			currentroot.setNextNodePtr(siblingpid);
			//now we need to create a new root
			BTNonLeafNode newroot;
			rc = newroot.initializeRoot(rpid, siblingKey, siblingpid);
			//this should return 2
			rootPid = pf.endPid();
			newroot.write(rootPid, pf);
			treeHeight++;	
		}
		currentroot.write(rpid, pf);
		return 0;
	}
	//if the tree has height >= 2
	else {
			int level = 1;
			int returnkey;
			PageId rtpid;
			rc = locateAndInsertToLeaf(key, rid, rootPid, level, rtpid, returnkey);	
		}
    return 0;
}

/*
this will insert 
@ int key: the key to insert
@ RecordId rid: the record to insert
@ PageId pid: the page id of the page where we need to inspect
@ int level: the current level of the tree
@ PageId&spid: the page id to return if a leaf node was full
@int& pkey: the key returned to insert into parent
*/
RC BTreeIndex::locateAndInsertToLeaf(int key, const RecordId& rid, PageId currentPid, int level, PageId& siblingPid, int& sib_mid_key) {

		RC rc;
		
		//we are at a leaf
		if(level == treeHeight) {
			PageId sibpid;
			BTLeafNode leaf;
			leaf.read(currentPid, pf);
			//try to insert key to the leaf
			rc = leaf.insert(key, rid);
			//if write was successful then return 
			if(rc < 0) {
				BTLeafNode sibling;
				int siblingfirstkey;
				leaf.insertAndSplit(key, rid, sibling, siblingfirstkey);
				
				sibpid = pf.endPid();
				
				sibling.write(sibpid, pf);
				leaf.setNextNodePtr(sibpid);
				//this is what is going to be returned if the node was full
				siblingPid = sibpid;
				sib_mid_key = siblingfirstkey;
			}
			leaf.write(currentPid, pf);
		}
		
		//while we are at a nonleaf, do this
		else {
		//get the node with the given pid
			BTNonLeafNode nl;
			PageId newpid;
			PageId splitpid;
			nl.read(currentPid, pf);
			//locate the child with key larger than or equal to the key we want to insert and get its pid
			rc = nl.locateChildPtr(key, newpid);
			int newlevel = level+1;
			int spkey;
			//search one level below. this creates a recursion
			rc = locateAndInsertToLeaf(key, rid, newpid, newlevel, splitpid, spkey);
			//if the node on the  was full then we need to insert of sibling to the parent node
			if(rc == RC_NODE_FULL) {
			//try to insert the key return from previous recursion
				rc = nl.insert(spkey, splitpid);
				
				//insertion failed so the node must be full
				if(rc<0) {
				
					int midKey;
					BTNonLeafNode sib;
					nl.insertAndSplit(sib_mid_key, splitpid, sib, midKey);
					PageId sibpid = pf.endPid();
					sib.write(sibpid, pf);
					//if this was a root node and was full, then we need to create a new root
					if(level==1) {
						BTNonLeafNode newroot;
						newroot.initializeRoot(currentPid, midKey, sibpid);
						rootPid = pf.endPid();
						newroot.write(rootPid, pf);
						treeHeight++;
					}
					sib_mid_key = midKey;
					siblingPid = sibpid;
				}
				nl.write(currentPid, pf);
			}
		}
	return rc;
	//return 0;
}
/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{	
	RC read_node;
	BTNonLeafNode nonleaf;
	PageId pid = rootPid;
	int level = 1;
	//while we are still at a nonleaf
	while(level < treeHeight) {
		read_node = nonleaf.read(pid,pf);
		if(read_node<0) {return read_node;}
		//get the pid of next node that has the pointer
		read_node = nonleaf.locateChildPtr(searchKey, pid);
		if(read_node<0) {return read_node;}
		level++;
	}
	//we have reached the bottom-most level of the tree i.e. the leaf(s)
	BTLeafNode leaf;
	read_node = leaf.read(pid, pf);
	if(read_node<0) {return read_node;}
	int entryid;
	read_node = leaf.locate(searchKey, entryid);
	if(read_node<0) {return read_node;}
	cursor.pid = pid;
	cursor.eid = entryid;
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;
	BTLeafNode leaf;
	//read page of leaf
	rc = leaf.read(cursor.pid,pf);
	if(rc<0) return rc;
	//read entry
	rc = leaf.readEntry(cursor.eid, key, rid);
	if(rc<0) return rc;
	
	//after reading move to the next entry in the same leaf
	cursor.eid++;
	//if we had just recovered the last entry in the node, get the pointer to next leaf
	if(cursor.eid > leaf.getKeyCount()) {
		cursor.pid = leaf.getNextNodePtr();
		cursor.eid=1;
	}
    return 0;
}

RC BTreeIndex::readdata() {
	//buffer to read the first page, which contains the height of the tree and the pid of the root
	char buffer[PageFile::PAGE_SIZE];
	RC rc;
	if((rc=pf.read(0,buffer))<0) {
		return rc; 
	}
	//copy root pid and height of tree to the tree index object
	memcpy(&rootPid, buffer, sizeof(PageId));
	memcpy(&treeHeight, buffer+4, sizeof(int));
	return 0;
}

//this is to write the data of root id and treeheight to first page
RC BTreeIndex::writedata() {
	 char buffer[PageFile::PAGE_SIZE];
	 memcpy(buffer, &rootPid, sizeof(PageId));
	 memcpy(buffer+4, &treeHeight, sizeof(int));
	 pf.write(0, buffer);
	 return 0;
}
