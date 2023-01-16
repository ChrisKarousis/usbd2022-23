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
  // Δημιουργία και αποθήκευση δομής HT info
  HT_info hashInfo;
  hashInfo.fileDesc = fd;
  hashInfo.numBuckets = buckets;
  hashInfo.hashTable = malloc(buckets*sizeof(int));
  hashInfo.size = BF_BLOCK_SIZE/sizeof(Record);
  for(i=0 ; i<buckets ; i++){
    hashInfo.hashTable[i] = -1; // -1 means that we have not allocated a block for this bucket
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
  int blockNum, blockId;
  // περασμα απο συναρτηση κατακερματισμου για την ευρεση του καταλληλου bucket
  int h = hashFunction(record.id, ht_info->numBuckets); 
  if(ht_info->hashTable[h] == -1){ // Αν δεν εχω ακομα block για τις εγγραφες
    BF_Block* block;
    BF_Block_Init(&block);
    // Καινουριο block
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memcpy(data, &record, sizeof(Record)); 
    CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
    ht_info->hashTable[h] = blockNum-1; // αποθηκευση του αριθμου του μπλοκ στον πινακα
    blockId = blockNum -1;
    HT_block_info blockInfo;
    blockInfo.recordCount = 1; // μολις βαλαμε μια εγγραφη
    blockInfo.nextBlock = -1; // εχουμε μονο ενα μπλοκ σε αυτον τον καδο
    memcpy(data + BF_BLOCK_SIZE - sizeof(HT_block_info), &blockInfo, sizeof(HT_block_info)); // store metadata
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
  }
  else { // Αν εχω ηδη ενα μπλοκ
    BF_Block* block;
    HT_block_info* metadata = malloc(sizeof(HT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, ht_info->hashTable[h], block));
    data = BF_Block_GetData(block);
    memcpy(metadata, data + BF_BLOCK_SIZE - sizeof(HT_block_info), sizeof(HT_block_info));
    // Αν το μπλοκ δεν ειναι γεματο
    if(metadata->recordCount != ht_info->size){
      blockId = ht_info->hashTable[h];
      int recCount = metadata->recordCount;
      memcpy(data + recCount*sizeof(Record), &record, sizeof(Record));
      (metadata->recordCount)++;
      memcpy(data + BF_BLOCK_SIZE - sizeof(HT_block_info), metadata, sizeof(HT_block_info)); // update metadata
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      
    } // Αν το μπλοκ ειναι γεματο 
    else { 
      int previousBlock = ht_info->hashTable[h];
      BF_Block* newBlock;
      BF_Block_Init(&newBlock);
      CALL_OR_DIE(BF_AllocateBlock(fd, newBlock));
      data = BF_Block_GetData(newBlock);
      memcpy(data, &record, sizeof(Record));
      CALL_OR_DIE(BF_GetBlockCounter(fd, &blockNum));
      ht_info->hashTable[h] = blockNum-1; // ενημερωση του πινακα να δειχνει στο καινουριο μπλοκ
      blockId = blockNum-1;
      metadata->nextBlock = previousBlock; // συνδεση του καινουριου μπλοκ με το παλιο (το παλιο γινεται μπλοκ υπερχειλισης)
      metadata->recordCount = 1;
      memcpy(data + BF_BLOCK_SIZE - sizeof(HT_block_info), metadata, sizeof(HT_block_info));
      BF_Block_SetDirty(newBlock);
      CALL_OR_DIE(BF_UnpinBlock(newBlock));
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));
      }
      }
      return blockId;
  }

int HT_GetAllEntries(HT_info* ht_info, void *value ){
  printf("Start get all entries! ...\n");
  int id = *((int*)value);
  int blockCount = 0; // ποσα μπλοκ διαβαστηκαν 
  int i;
  Record* rec;
  int fd = ht_info->fileDesc;
  int h = hashFunction(id, ht_info->numBuckets);
  int blockNumber = ht_info->hashTable[h];
  while(blockNumber != -1){ // Αναζητηση για την εγγραφη οσο υπαρχει επομενο μπλοκ στην αλυσιδα
    blockCount++;
    BF_Block* block;
    HT_block_info* metadata = malloc(sizeof(HT_block_info));
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, blockNumber, block));
    void* data = BF_Block_GetData(block);
    memcpy(metadata, data + BF_BLOCK_SIZE - sizeof(HT_block_info), sizeof(HT_block_info));
    int recCount = metadata->recordCount;
    CALL_OR_DIE(BF_UnpinBlock(block));
    for(i=0; i<recCount; i++){ // Αναζητηση για καθε εγγραφη στο μπλοκ
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


int HashStatistics(char* fileName){
  HT_info* ht_info =  HT_OpenFile(fileName);
  int fd = ht_info->fileDesc;
  int numBlocks;
  CALL_OR_DIE(BF_GetBlockCounter(fd, &numBlocks));
  int numBuckets = ht_info->numBuckets;
  int* hashTable = ht_info->hashTable;
  int minRecords = numBlocks*ht_info->size; // Ελαχιστο: Αρχικοποιηση ως ενα upper bound
  int maxRecords = 0; // Μεγιστο: Αρχικοποιηση ως 0
  int sumRecords = 0; // Ο συνολικος αριθμος εγγραφων για ολο το αρχειο
  int* chainLengths = malloc(numBuckets*sizeof(int)); // Ο αριθμος των blocks για καθε bucket (αλυσιδες υπερχειλισης)

  for(int i = 0; i < numBuckets; i++){ // Για καθε bucket
    chainLengths[i] = 1; // Αρχικοποιηση σε 1 επειδη καθε bucket εχει τουλαχιστον 1 μπλοκ
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fd, hashTable[i], block));
    void* data = BF_Block_GetData(block);
    HT_block_info* metadata = malloc(sizeof(HT_block_info));
    memcpy(metadata, data + BF_BLOCK_SIZE - sizeof(HT_block_info), sizeof(HT_block_info));
    int recordCount = metadata->recordCount; // Ο αριθμος εγγραφων του συγκεκριμενου bucket
    int nextBlock = metadata->nextBlock;
    free(metadata);
    while(nextBlock != -1){ // Αν ο καδος περιεχει μπλοκ υπερχειλισης
      chainLengths[i]++;
      BF_Block* next;
      BF_Block_Init(&next);
      CALL_OR_DIE(BF_GetBlock(fd, nextBlock, next));
      void* nextData = BF_Block_GetData(next);
      HT_block_info* nextmetadata = malloc(sizeof(HT_block_info));
      memcpy(nextmetadata, nextData + BF_BLOCK_SIZE - sizeof(HT_block_info), sizeof(HT_block_info));
      recordCount += nextmetadata->recordCount; // Προσθηκη των εγγραφων του μπλοκ υπερχειλισης στο αθροισμα.
      nextBlock = nextmetadata->nextBlock;
      CALL_OR_DIE(BF_UnpinBlock(next));
      free(nextmetadata);
      }
    // Ενημερωσε τα στατιστικα για το συνολικο αριθμο εγγραφων, τον ελαχιστο και τον μεγιστο
    sumRecords += recordCount;
    if(recordCount < minRecords) minRecords = recordCount;
    if(recordCount > maxRecords) maxRecords = recordCount;
    CALL_OR_DIE(BF_UnpinBlock(block));
  }

  printf("Number of blocks: %d\n", numBlocks);
  printf("Minimum number of records per bucket: %d\n", minRecords);
  printf("Maximum number of records per bucket: %d\n", maxRecords);
  printf("Average number of records per bucket: %f\n", (float)sumRecords/numBuckets);
  printf("Average number of blocks per bucket: %f\n", (float)numBlocks/numBuckets);
  printf("Number of blocks for each bucket:\n");
  int chainCount = 0;
  for(int i = 0; i < numBuckets; i++){
    printf("Bucket %d has %d blocks\n", i, chainLengths[i]);
    if (chainLengths[i] > 1) {chainCount++;}
  }
  printf("Number of buckets with overflow chains: %d\n", chainCount);
  free(chainLengths);
  return 0;
}
