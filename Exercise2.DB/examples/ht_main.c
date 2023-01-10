#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define FILE_NAME "data.db"
#define FILES_COUNT 10

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
  char s[FILES_COUNT][9];
  HT_info** info = malloc(FILES_COUNT*sizeof(HT_info *));
  int i;
  for(i = 0 ; i<FILES_COUNT ; i++){ // dhmiourgw to onoma twn arxeiwn 
    sprintf(s[i], "data%d.db", i+1);
    HT_CreateFile(s[i],15);
    info[i] = HT_OpenFile(s[i]);
  }
  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    HT_InsertEntry(info[id%FILES_COUNT], record);
  }

  printf("RUN PrintAllEntries\n");
  for(i=0 ; i<100 ; i++){
    int id = rand() % RECORDS_NUM;
    printf("I want to find id : %d\n", id);
    int blockCount = HT_GetAllEntries(info[i%FILES_COUNT], &id);
    if(blockCount == -1){
      printf("This id does not exist in this file\n");
    }else {
      printf("I crossed %d blocks in order to find the id\n", blockCount);
    }
  }
  for(i=0 ; i<FILES_COUNT ; i++){
    HT_CloseFile(info[i]);
  }
  BF_Close();
}
