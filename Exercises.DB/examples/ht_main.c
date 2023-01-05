#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);

  HT_CreateFile(FILE_NAME,15);
  HT_info* info = HT_OpenFile(FILE_NAME);
  printf("Info size:%d\n", info->size);
  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    HT_InsertEntry(info, record);
    printRecord(record);
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  printf("I want to find id : %d\n", id);
  int blockCount = HT_GetAllEntries(info, &id);
  printf("Perasa apo %d block gia na brw to id\n", blockCount);
  id = 33;
  blockCount = HT_GetAllEntries(info, &id);
  printf("Perasa apo %d block gia na brw to id\n", blockCount);
  id = 47;
  blockCount = HT_GetAllEntries(info, &id);
  printf("Perasa apo %d block gia na brw to id\n", blockCount);
  HT_CloseFile(info);
  BF_Close();
}
