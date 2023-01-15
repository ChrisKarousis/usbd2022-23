#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
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

extern int openFiles;

int shtHashFunction(char* str, long int numBuckets){
  return (str[0]-'A')%numBuckets; // we take the first letter and then modulo to put the words in buckets
}

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
  if(openFiles == -1){
    return -1;
  }
  printf("Creating a File ...\n");
  int i;
  int fd1, fd2;
  BF_Block* infoBlock;
  void* data;
  CALL_OR_DIE(BF_OpenFile(fileName, &fd2));
  CALL_OR_DIE(BF_CreateFile(sfileName));
  CALL_OR_DIE(BF_OpenFile(sfileName, &fd1));
  BF_Block_Init(&infoBlock);
  CALL_OR_DIE(BF_AllocateBlock(fd1, infoBlock));
  data = BF_Block_GetData(infoBlock);
  SHT_info hashInfo;
  hashInfo.primary = fd2;
  hashInfo.fileDesc = fd1;
  hashInfo.numBuckets = buckets;
  hashInfo.shashTable = malloc(buckets*sizeof(int));
  hashInfo.size = BF_BLOCK_SIZE/sizeof(SHT_record);
  for(i=0 ; i<buckets ; i++){
    hashInfo.shashTable[i] = -1; // means that we have not allocated a block for this bucket
  }
  memcpy(data, &hashInfo, sizeof(SHT_info));
  BF_Block_SetDirty(infoBlock);
  CALL_OR_DIE(BF_UnpinBlock(infoBlock));
  CALL_OR_DIE(BF_CloseFile(fd1));
  CALL_OR_DIE(BF_CloseFile(fd2));
  return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  if(openFiles == BF_MAX_OPEN_FILES){
    return NULL;
  }
  printf("Opening a File ...\n");
  int fd;
  void* data;
  CALL_OR_DIE(BF_OpenFile(indexName, &fd));
  openFiles++;
  SHT_info* hashInfo;
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(fd, 0, block));
  
  data = BF_Block_GetData(block);
  
  hashInfo = data;
  return hashInfo;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
  printf("Closing a File ...\n");
  if(openFiles < 1){
    return -1;
  }
  int fd = SHT_info->fileDesc;
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(fd, 0, block));
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  CALL_OR_DIE(BF_CloseFile(fd));
  openFiles--;
  printf("File closed ...\n");
  return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
  int fd = sht_info->fileDesc;
  void* data;
  int blockNum;
  SHT_record shtRecord;
  memcpy(shtRecord.name, record.name, strlen(record.name) + 1);
  shtRecord.blockId = block_id;
  int h = shtHashFunction(record.name, sht_info->numBuckets);
  if(sht_info->shashTable[h] == -1){ // an den exw akoma block gia tis eggrafes
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
    sht_info->shashTable[h] = blockNum-1;
    memcpy(data, &shtRecord, sizeof(SHT_record));
    SHT_record* s = ((SHT_record *) data);
    SHT_block_info blockInfo;
    blockInfo.recordCount = 1; // molis balame mia eggrafh
    blockInfo.nextBlock = -1; // exoume mono ena block
    memcpy(data + BF_BLOCK_SIZE - sizeof(SHT_block_info), &blockInfo, sizeof(SHT_block_info));
    s = ((SHT_record *) data);
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
  }else {
    BF_Block* block;
    SHT_block_info* metadata = malloc(sizeof(SHT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, sht_info->shashTable[h], block));
    data = BF_Block_GetData(block);
    SHT_record* s = ((SHT_record *) data);
    memcpy(metadata, data + BF_BLOCK_SIZE - sizeof(SHT_block_info), sizeof(SHT_block_info));
    if(metadata->recordCount != sht_info->size){
      int recCount = metadata->recordCount;
      memcpy(data + recCount*sizeof(SHT_record), &shtRecord, sizeof(SHT_record));
      SHT_record* s = ((SHT_record *) data);
      (metadata->recordCount)++;
      memcpy(data + BF_BLOCK_SIZE  - sizeof(SHT_block_info), metadata, sizeof(SHT_block_info));
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      // memcpy metadata
    }else {
      int previousBlock = sht_info->shashTable[h];
      BF_Block* newBlock;
      BF_Block_Init(&newBlock);
      CALL_OR_DIE(BF_AllocateBlock(fd, newBlock));
      data = BF_Block_GetData(newBlock);
      memcpy(data, &shtRecord, sizeof(SHT_record));
      CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
      sht_info->shashTable[h] = blockNum-1;
      metadata->nextBlock = previousBlock;
      metadata->recordCount = 1;
      memcpy(data + BF_BLOCK_SIZE - sizeof(SHT_block_info), metadata, sizeof(SHT_block_info));
      BF_Block_SetDirty(newBlock);
      CALL_OR_DIE(BF_UnpinBlock(newBlock));
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
    }
  }
  
  return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
  printf("Start get all entries! ...\n");
  int i, j;
  int count = 0; // o arithmos twn block pou diabasthkan 
  // arxikopoiw sto 0 ton pinaka twn visited records
  int blockNum;
  CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDesc, &blockNum));
  int **visited = malloc(blockNum*sizeof(int*));
  for(i = 0 ; i < blockNum ; i++){
    int recordCount = ht_info->size;
    visited[i] = malloc(recordCount*sizeof(int));
    for(j = 0 ; j < recordCount ; j++){
      visited[i][j] = 0;
    }
  }
  //int blockCount = 0; // posa blocks diabasthkan 
  
  SHT_record* rec;
  int shtfd = sht_info->fileDesc;
  int h = shtHashFunction(name, sht_info->numBuckets);
  int blockNumber = sht_info->shashTable[h];
  while(blockNumber != -1){
    count++;
    BF_Block* block;
    SHT_block_info* shtmetadata = malloc(sizeof(SHT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(shtfd, blockNumber, block));
    void* data = BF_Block_GetData(block);
    memcpy(shtmetadata, data + BF_BLOCK_SIZE - sizeof(SHT_block_info), sizeof(SHT_block_info));
    int recCount = shtmetadata->recordCount;
    CALL_OR_DIE(BF_UnpinBlock(block));
    for(i=0; i<recCount; i++){
      //memcpy(rec, data + i*sizeof(Record), sizeof(Record));
      rec = ((SHT_record *)data);
      if(strcmp(rec[i].name, name) == 0){
        int fd = ht_info->fileDesc;
        int blockNumber = rec[i].blockId;
        Record* record;
        while(blockNumber != -1){
          count++;
          BF_Block* htblock;
          HT_block_info* metadata = malloc(sizeof(HT_block_info));
          BF_Block_Init(&htblock);
          CALL_OR_DIE(BF_GetBlock(fd, blockNumber, htblock));
          void* data = BF_Block_GetData(htblock);
          memcpy(metadata, data + BF_BLOCK_SIZE - sizeof(HT_block_info), sizeof(HT_block_info));
          int recCount = metadata->recordCount;
          CALL_OR_DIE(BF_UnpinBlock(htblock));
          int j;
          for(j=0; j<recCount; j++){
            //memcpy(rec, data + i*sizeof(Record), sizeof(Record));
            record = ((Record *)data);
            if((strcmp(record[j].name, name) == 0) && (visited[blockNumber][j] == 0)){
              printRecord(record[j]);
              visited[blockNumber][j] = 1;
            }
          }
          blockNumber = metadata->nextBlock;
        }
      }
    }
    blockNumber = shtmetadata->nextBlock; 
  }
  return count;
}



