#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int openFiles = 0;

int hashFunction(int id, long int numBuckets){
  return id%numBuckets;
}

int HT_CreateFile(char *fileName,  int buckets){
  if(openFiles == BF_MAX_OPEN_FILES){
    return -1;
  }
  printf("Creating a File ...\n");
  int i;
  int fd;
  BF_Block* infoBlock;
  void* data;
  CALL_OR_DIE(BF_CreateFile(fileName));
  CALL_OR_DIE(BF_OpenFile(fileName, &fd));
  BF_Block_Init(&infoBlock);
  CALL_OR_DIE(BF_AllocateBlock(fd, infoBlock));
  data = BF_Block_GetData(infoBlock);
  HT_info hashInfo;
  hashInfo.fileDesc = fd;
  hashInfo.numBuckets = buckets;
  hashInfo.hashTable = malloc(buckets*sizeof(int));
  hashInfo.size = BF_BLOCK_SIZE/sizeof(Record);
  for(i=0 ; i<buckets ; i++){
    hashInfo.hashTable[i] = -1; // means that we have not allocated a block for this bucket
  }
  memcpy(data, &hashInfo, sizeof(HT_info));
  BF_Block_SetDirty(infoBlock);
  CALL_OR_DIE(BF_UnpinBlock(infoBlock));
  CALL_OR_DIE(BF_CloseFile(fd));
  return 0;
}

HT_info* HT_OpenFile(char *fileName){
  if(openFiles == BF_MAX_OPEN_FILES){
    return NULL;
  }
  printf("Opening a File ...\n");
  int fd;
  void* data;
  CALL_OR_DIE(BF_OpenFile(fileName, &fd));
  openFiles++;
  HT_info* hashInfo;
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(fd, 0, block));
  
  data = BF_Block_GetData(block);
  
  hashInfo = data;
  //return hashInfo;
  return hashInfo;
}


int HT_CloseFile( HT_info* HT_info ){
  printf("Closing a File ...\n");
  if(openFiles < 1){
    return -1;
  }
  int fd = HT_info->fileDesc;
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(fd, 0, block));
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  CALL_OR_DIE(BF_CloseFile(fd));
  printf("File closed ...\n");
  return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
  int fd = ht_info->fileDesc;
  void* data;
  int blockNum;
  int h = hashFunction(record.id, ht_info->numBuckets);
  if(ht_info->hashTable[h] == -1){ // an den exw akoma block gia tis eggrafes
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memcpy(data, &record, sizeof(Record));
    CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
    ht_info->hashTable[h] = blockNum-1;
    HT_block_info blockInfo;
    blockInfo.recordCount = 1; // molis balame mia eggrafh
    blockInfo.nextBlock = -1; // exoume mono ena block
    memcpy(data + sizeof(data) - sizeof(HT_block_info), &blockInfo, sizeof(HT_block_info)); // storing metadata
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
  }else {
    BF_Block* block;
    HT_block_info* metadata = malloc(sizeof(HT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, ht_info->hashTable[h], block));
    data = BF_Block_GetData(block);
    memcpy(metadata, data + sizeof(data) - sizeof(HT_block_info), sizeof(HT_block_info));
    if(metadata->recordCount != ht_info->size){
      int recCount = metadata->recordCount;
      memcpy(data + recCount*sizeof(Record), &record, sizeof(Record));
      (metadata->recordCount)++;
      memcpy(data + sizeof(data) - sizeof(HT_block_info), metadata, sizeof(HT_block_info));
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      // memcpy metadata
    }else {
      int previousBlock = ht_info->hashTable[h];
      BF_Block* newBlock;
      BF_Block_Init(&newBlock);
      CALL_OR_DIE(BF_AllocateBlock(fd, newBlock));
      data = BF_Block_GetData(newBlock);
      memcpy(data, &record, sizeof(Record));
      CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
      ht_info->hashTable[h] = blockNum-1;
      metadata->nextBlock = previousBlock;
      metadata->recordCount = 1;
      memcpy(data + sizeof(data) - sizeof(HT_block_info), metadata, sizeof(HT_block_info));
      BF_Block_SetDirty(newBlock);
      CALL_OR_DIE(BF_UnpinBlock(newBlock));
    }
  }
  return 0;
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
  printf("Start get all entries! ...\n");
  int id = *((int*)value);
  int blockCount = 0; // posa blocks diabasthkan 
  int i;
  Record* rec;
  int fd = ht_info->fileDesc;
  int h = hashFunction(id, ht_info->numBuckets);
  printf("Hash function got me : %d\n", h);
  int blockNumber = ht_info->hashTable[h];
  printf("Blocknumber to check : %d\n", blockNumber);
  while(blockNumber != -1){
    blockCount++;
    BF_Block* block;
    HT_block_info* metadata = malloc(sizeof(HT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, blockNumber, block));
    void* data = BF_Block_GetData(block);
    memcpy(metadata, data + sizeof(data) - sizeof(HT_block_info), sizeof(HT_block_info));
    int recCount = metadata->recordCount;
    printf("Recordcount : %d\n", recCount);
    for(i=0; i<recCount; i++){
      //memcpy(rec, data + i*sizeof(Record), sizeof(Record));
      rec = ((Record *)data);
      if(rec[i].id == id){
        printRecord(rec[i]);
        return blockCount;
      }
    }
    blockNumber = metadata->nextBlock; 
  }
  return -1;
}




