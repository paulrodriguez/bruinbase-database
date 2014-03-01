#include "BTreeNode.h"

using namespace std;



/**
initialize a leaf node by setting everything to -1 and set count of entries to zero
**/
BTLeafNode::BTLeafNode() {

	int* buff = (int*) buffer;
	int i;
	for(i=0; i<256;i++) {
	*(buff+i) = -1;
	}
	//set the count of total entries
	*(ibuff+255) = 0;
	strcpy(buffer, (char*)buff);
	
	/*for (int i=0; i < PageFile::PAGE_SIZE; i+=4) {
		memcpy(buffer+i, &reset, sizeof(int));
		}
	int count = 0;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &count, sizeof(int));
	*/	
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	RC rc;
	if((rc=pf.read(pid, buffer))<0) {
		
		return rc;
	}
	return 0;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;
	if((rc = pf.write(pid, buffer))<0){
		return rc;
	}

	return 0;
 }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	int* buff = (int*) buffer;
	/*int count;
        memcpy(&count, buffer+PageFile::PAGE_SIZE-8, sizeof(int));
	return count;*/
	return *(buff+255);
 }

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	 int count = getKeyCount();
	if(count < 0) count = 0;
	// node is already full
	if(count >=MAX_LEAF) {
		return RC_NODE_FULL;
	}

	//this keeps track of where its position is
	int eid = -1;
	RC rc;
	rc = locate(key, eid);
	//if the record was not found, then we can append the new entry at the end
	if(rc < 0 || eid < 0) {
		
		int end = count*LEAF_ENTRY_SIZE;
		memcpy(buffer+end, &rid.pid, sizeof(int));
		memcpy(buffer+end+4, &rid.sid, sizeof(int));
		memcpy(buffer+end+8, &key, sizeof(int));
	}
	else {
		//shift everything to the right to insert element
		
	for (int i = count*LEAF_ENTRY_SIZE-1;i>=(eid-1)*LEAF_ENTRY_SIZE;i--) {
			buffer[i+LEAF_ENTRY_SIZE] = buffer[i];
	}

		//begin inserting the entry
		int pos_insert = (eid-1)*LEAF_ENTRY_SIZE;
		memcpy(buffer+pos_insert, &rid.pid, sizeof(int));
		
		memcpy(buffer+pos_insert+sizeof(int), &rid.sid, sizeof(int));
		
		memcpy(buffer+pos_insert+sizeof(int)*2, &key, sizeof(int));
	}
	count++;
	
	
	memcpy(buffer+PageFile::PAGE_SIZE-4, &count, sizeof(int));
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	RC rc;
	int count = getKeyCount();
	
	//the first record where the split is supposed to occur
	int startsplit = LEAF_ENTRY_SIZE*count/2;
	//to read the records and then insert them to the sibling
	int skey;
	RecordId siblingRid;
	
	int reset = -1;
	for (int i = startsplit; i < LEAF_ENTRY_SIZE*count; i += 12) {
		memcpy(&siblingRid.pid, buffer+i, sizeof(PageId));
		memcpy(&siblingRid.sid, buffer+i+4, sizeof(int));
		memcpy(&skey, buffer+i+8, sizeof(int));
		sibling.insert(skey, siblingRid);
		//get the first entry in the sibling
		if(i == startsplit) {
			siblingKey = skey;
		}	
		//this is to clear up the space
		memcpy(buffer+i, &reset, sizeof(int));
		memcpy(buffer+i+4, &reset, sizeof(int));
		memcpy(buffer+i+8, &reset, sizeof(int));
	}
	count = count/2;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &count, sizeof(int));
	//if the key is less than the first key entry in the sibling node then insert it to this current node
	if(key < siblingKey) {
		rc=this->insert(key, rid);
	}
	else {
		
		rc=sibling.insert(key, rid);
		if(rc<0) {return rc;}
	}
	sibling.setNextNodePtr(this->getNextNodePtr());
	
	return 0; 
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int count = getKeyCount();
	int key;
	int counter = 0;
	for (int i = 8; i < count*LEAF_ENTRY_SIZE; i += 12) {
		counter++;
		memcpy(&key, buffer+i, sizeof(int));
		if(key>=searchKey) {
			eid = counter;
			return 0;
		}
	}	
	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	if(eid>MAX_LEAF || eid<0){return RC_INVALID_CURSOR;}	
	int pos = (eid-1)*12;
	memcpy(&rid.pid, buffer+pos, sizeof(PageId));
	pos += 4;
	memcpy(&rid.sid, buffer+pos, sizeof(int));
	pos += 4;
	memcpy(&key, buffer+pos, sizeof(int));
	return 0;
 }

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId pid;
	memcpy(&pid, buffer+PageFile::PAGE_SIZE-8, sizeof(PageId));
	return pid; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if (pid < 0) {
		return RC_INVALID_PID;
	}
	int* buff = (int *) buffer;
	*(buff+254) = pid;
	strcpy(buffer, (char*)buff);
	//memcpy(buffer+PageFile::PAGE_SIZE-8, &pid, sizeof(PageId));
	return 0; 
}

/**************************************************************************************************/

BTNonLeafNode::BTNonLeafNode() {
	int reset = -1;
	for(int i=0; i < PageFile::PAGE_SIZE; i+=4) {
		memcpy(buffer+i, &reset, sizeof(int));
	}
	
	//a two at the end of buffer means it is a Non-Leaf Node
	int nodetype=2;
	memcpy(buffer+PageFile::PAGE_SIZE-4, &nodetype, sizeof(int));
	
	//set count to zero
	int count = 0;
	memcpy(buffer+PageFile::PAGE_SIZE-8, &count, sizeof(int));
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;
	if((rc = pf.read(pid, buffer)) < 0) {
		return RC_FILE_READ_FAILED;
	}
	return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;
	if((rc = pf.write(pid, buffer)) < 0) {
		return RC_FILE_WRITE_FAILED;
	}
	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	int count;
	memcpy(&count, buffer+PageFile::PAGE_SIZE-8, sizeof(int));
	return count; 
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	RC rc;
	int count = this->getKeyCount();
		
	//allow insertion of 100 keys since we will split after that
	if(count >= MAX_NON_LEAF) return RC_NODE_FULL;
	int eid = -1;
	//only search if there is at least one key in the node
		rc=this->locateKey(key, eid);
		//no key was found to be larger, so we can add the new key to the end
		//if there are no keys (i.e. when creating a sibling) then it should have the the first pid anyways
	if(rc<0 || eid < 0) {
		memcpy(buffer+count*PAIR_SIZE+4, &key, sizeof(int));
		memcpy(buffer+count*PAIR_SIZE+8, &pid, sizeof(PageId));
	}
	else {
		//we need to shift everthing to the right
		for(int i = count*PAIR_SIZE+3; i >= (eid)*8-4; i--) {
			buffer[i+8]=buffer[i];
		}
		//insert our new pid, key pair at specific position
		memcpy(buffer+eid*PAIR_SIZE-4, &key, sizeof(int));
		memcpy(buffer+eid*PAIR_SIZE, &pid, sizeof(PageId));
	}
	//increase count of entries and write to buffer
	count++;
	memcpy(buffer+PageFile::PAGE_SIZE-8,&count, sizeof(int));
	
	return 0; 
}



/**
	locate a key within a nonleaf node
	@param int searchKey[IN] the key to search for
	@param int& eid[OUT] the position of the eid. first entry has position 1
	return 0 if successful. error code
**/
RC BTNonLeafNode::locateKey(int searchKey, int& eid)
{ 
	int count = getKeyCount();
	int key;
	int counter = 0;
	for (int i = 4; i < count*PAIR_SIZE; i += 8) {
		counter++;
		memcpy(&key, buffer+i, sizeof(int));
		if(key>=searchKey) {
			eid = counter;
			return 0;
		}		
	}
	return RC_NO_SUCH_RECORD;
}
/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	//at this point the node should be full
	RC rc;
	//this is to reset the buffer where the entries where moved to the sibling
	int reset=-1;
	int count = this->getKeyCount();
	int eid = -1;
	RC find = this->locateKey(key, eid);
	//this means no key was found so set it should be inserted at the end
	if(eid<0) {
		eid = count+1;
	}
	int setcount;
	//the position of the middle key after insertion
	PageId leftpid;
	int skey;
	int spid;
	int midpos = (count+2)/2;
	//keeps track of number of entries in tehe node
	int counter = count;
	//this means that the key we are about to insert is going to be in the middle
	if(eid == midpos) {
			//copy the key to be inserted to be returned
			midKey = key;
			//set pid of sibling
			sibling.startPid(pid);
			//copy entries and move then to sibling
			for(int i = eid*PAIR_SIZE-4; i < count*PAIR_SIZE; i += 8) {
				memcpy(&skey, buffer+i, sizeof(int));
				memcpy(&spid, buffer+i+4, sizeof(PageId));
				sibling.insert(skey, spid);
				memcpy(buffer+i, &reset, sizeof(int));
				memcpy(buffer+i+4, &reset, sizeof(PageId));
				counter--;
			}
			memcpy(buffer+PageFile::PAGE_SIZE-8, &counter, sizeof(int));
			
	}
	//this means after insert of key, it is after the middle key, so it should be inserted into sibling key
	else if(eid > midpos) {
			//copy the key in the middle to return it
			memcpy(&midKey, buffer+midpos*PAIR_SIZE-4, sizeof(int));
			//copy the if of the middle key to set it as the leftpid in the sibling
			memcpy(&leftpid, buffer+midpos*PAIR_SIZE, sizeof(PageId));
			sibling.startPid(leftpid);
			
			counter--;
			
			//free the space where the middle key used to be
			memcpy(buffer+midpos*PAIR_SIZE-4, &reset, sizeof(int));
			memcpy(buffer+midpos*PAIR_SIZE, &reset, sizeof(PageId));
			//want to copy all the keys and insert them into the sibling
			for(int i = midpos*PAIR_SIZE+4; i < count*PAIR_SIZE;i += 8) {
				memcpy(&skey, buffer+i, sizeof(int));
				memcpy(&spid, buffer+i+4, sizeof(PageId));
				sibling.insert(skey,spid);
				memcpy(buffer+i, &reset, sizeof(int));
				memcpy(buffer+i+4, &reset, sizeof(PageId));
				counter--;
			}
			//insert new key to sibling
			sibling.insert(key, pid);
			//set new count of node
			memcpy(buffer+PageFile::PAGE_SIZE-8, &counter, sizeof(int));
	}
	else if(eid<midpos) {
		//the midkey should be one less
			midpos--;
			//copy the key in the middle to return it
			memcpy(&midKey, buffer+midpos*PAIR_SIZE-4, sizeof(int));
			//copy the if of the middle key to set it as the leftpid in the sibling
			memcpy(&leftpid, buffer+midpos*PAIR_SIZE, sizeof(PageId));
			
			memcpy(buffer+midpos*PAIR_SIZE-4, &reset, sizeof(int));
			memcpy(buffer+midpos*PAIR_SIZE, &reset, sizeof(int));
			counter--;
			sibling.startPid(leftpid);
			//want to copy all the keys and insert them into the sibling
			for(int i = midpos*PAIR_SIZE+4; i < count*PAIR_SIZE;i += 8) {
				memcpy(&skey, buffer+i, sizeof(int));
				memcpy(&spid, buffer+i+4, sizeof(PageId));
				sibling.insert(skey,spid);
				memcpy(buffer+i, &reset, sizeof(int));
				memcpy(buffer+i+4, &reset, sizeof(PageId));
				counter--;
			}
			memcpy(buffer+PageFile::PAGE_SIZE-8, &counter, sizeof(int));
			//insert new key to current node
			this->insert(key, pid);			
	}	
	return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	int count = getKeyCount();
	int key;
	//locate pointer to child
	for(int i=4; i < count*PAIR_SIZE; i+=8) {
		memcpy(&key, buffer+i, sizeof(int));
		if(key>searchKey) {
			//if the first key is larger than our search key then we need to get the first pid
			if(i==4) {
				memcpy(&pid, buffer, sizeof(PageId));
				return 0;
			}
			else {
				memcpy(&pid, buffer+i-4, sizeof(PageId));
				return 0;
			}
			
		}
	}
	//if no key was found larger than search key, return the last pid
	memcpy(&pid,buffer+count*PAIR_SIZE,sizeof(PageId));
	return 0; 
}
/**
	this is to insert the pid at the beginning of a new node that is not a root node
	@return 0 if successful. else return error code
**/
RC BTNonLeafNode::startPid(PageId pid) {
	if(pid <0) {return RC_INVALID_PID;}
	memcpy(buffer, &pid, sizeof(PageId));
	return 0;
}
/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	//return if pids are invalid
	if(pid1<0 || pid2<0) {return RC_INVALID_PID;}
	//set everything to -1
	int reset = -1;
	for(int i = 0; i < PageFile::PAGE_SIZE; i += 4) {
		memcpy(&reset, buffer, sizeof(int));
	}
	//insert the pids and key
	memcpy(buffer, &pid1, sizeof(PageId));
	memcpy(buffer+4, &key, sizeof(int));
	memcpy(buffer+8, &pid2, sizeof(PageId));
	//this represents the root
	int nodetype = 3;
	int count = 1;
	memcpy(buffer+PageFile::PAGE_SIZE-4, &nodetype, sizeof(int));
	memcpy(buffer+PageFile::PAGE_SIZE-8, &count, sizeof(int));
	return 0; 
}
