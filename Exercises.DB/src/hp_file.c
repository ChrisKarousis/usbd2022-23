#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}


int HP_CreateFile(char *fileName){
  int fd;
  BF_Block* block;
  BF_Block_Init(&block);
  // Δημιουργία και άνοιγμα αρχείου
  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fd));

  // Δημιουργία του πρώτου block το οποίο θα περιέχει μόνο τις πληροφορίες του αρχείου HP_info
  CALL_BF(BF_AllocateBlock(fd, block));
  //Εγγραφή πληροφοριών
  HP_info info;
  strcpy(info.file_type, "HP");
  info.fd = fd;
  info.last_block = 0;
  char* buffer = BF_Block_GetData(block);
  memcpy(buffer, &info, sizeof(info));

  // Κλείσιμο του αρχείου και εκτύπωση μηνύματος επιτυχίας
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(fd));
  printf("\nNew file created!\n");

  return 0;
}

HP_info* HP_OpenFile(char *fileName){
  int fd;
  BF_Block* block;
  BF_Block_Init(&block);
  // Ελεγχοι που επιστρεφουν NULL αντι για BF_ERROR
  if(BF_OpenFile(fileName, &fd) != BF_OK)
    return NULL;
  if (BF_GetBlock(fd, 0, block) != BF_OK)
    return NULL;
  //Ανακτηση πληροφοριας απ το πρωτο block
  char* buffer = BF_Block_GetData(block);
  HP_info* info = malloc(sizeof(HP_info));
  memcpy(info, buffer, sizeof(info));
  // Ελεγχος οτι το αρχειο ειναι αρχειο σωρου
  if (strcmp(info->file_type, "HP")!=0){
    perror("Error opening: File is not a Heap File");
    free (info);
    return NULL;
  }
  // Ενημερωση του file descriptor
  info-> fd = fd;
  memcpy(buffer, info, sizeof(info));
  BF_Block_SetDirty(block);

  printf("File opened!\n");
  return info;
}


int HP_CloseFile( HP_info* hp_info ){
  int fd = hp_info->fd;
  BF_Block* block;
  BF_Block_Init(&block);
  // Κλεισιμο και αποδεσμευση μνημης hp_info
  CALL_BF(BF_GetBlock(fd, 0, block));
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(fd));
  free(hp_info);

  printf("File closed!\n");
  return 0;
}

// Καλειται απ την HP_InsertEntry οταν χρειαζεται
int HP_insert_on_new_block (HP_info* hp_info, Record record){
    BF_Block* block;
    BF_Block_Init(&block);    
    CALL_BF(BF_AllocateBlock(hp_info->fd, block));
    char* buffer = BF_Block_GetData(block);
    //Εγγραφή record
    memcpy(buffer, &record, sizeof(record));
    //Εγγραφή των metadata στο τελος του block
    HP_block_info blockInfo;
    blockInfo.num_records = 1; // Πρωτη εγγραφη
    // το offset τοσο ωστε να χωραει το block_info ακριβως στο τελος
    memcpy(buffer + BF_BLOCK_SIZE - sizeof(blockInfo), &blockInfo, sizeof(blockInfo)); 
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    // Ενημέρωση του HP_info στο 1ο block οτι προστεθηκε καινουριο μπλοκ
    CALL_BF(BF_GetBlock(hp_info->fd, 0, block));
    buffer = BF_Block_GetData(block);
    hp_info->last_block++;
    memcpy(buffer, &hp_info, sizeof(hp_info));
    printf("New record inserted in new block:\n");
    printRecord(record);
    BF_Block_SetDirty(block);
    return BF_OK;
}


int HP_InsertEntry(HP_info* hp_info, Record record){
  // 1η περιπτωση: Το αρχειο ειναι αδειο
  if (hp_info->last_block == 0){ // αν το αρχειο περιεχει μονο το 1ο μπλοκ (που εχει το HP_info)
    CALL_BF(HP_insert_on_new_block(hp_info, record));
  } 
  
  //2η περιπτωση, το αρχειο δεν ειναι αδειο και παμε στο τελευταιο του μπλοκ
  else {
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(hp_info->fd, hp_info->last_block, block));
    char* buffer = BF_Block_GetData(block);
    // Ανακτηση blockInfo για το τελευταιο block
    HP_block_info blockInfo;
    memcpy(&blockInfo, buffer + BF_BLOCK_SIZE - sizeof(blockInfo), sizeof(blockInfo));

    // Υποπεριπτωση 1: το block δε χωραει αλλη εγγραφη
    if (blockInfo.num_records == HP_MAX_RECORDS){
      CALL_BF(HP_insert_on_new_block(hp_info, record));
    }
    
    // Υποπεριπτωση 2: το block χωραει κι αλλη εγγραφη
    else {
      char* buffer = BF_Block_GetData(block);
      //Εγγραφή record στην καταλληλη θεση, μετα απ τις προηγουμενες
      memcpy(buffer + (sizeof(Record)*blockInfo.num_records), &record, sizeof(record));
      //Ενημερωση του blockInfo οτι προστεθηκε μια καινουρια εγγραφη
      blockInfo.num_records += 1;
      memcpy(buffer + BF_BLOCK_SIZE - sizeof(blockInfo), &blockInfo, sizeof(blockInfo));
      printf("New record inserted in existing block:\n");
      printRecord(record);
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  return hp_info->last_block;
}

int HP_GetAllEntries(HP_info* hp_info, int value){
  BF_Block* block;
  BF_Block_Init(&block);
  int blocks_searched = 0; // Το πληθος των blocks που χρειαστηκαν για να βρεθουν ολες οι εγγραφες.
  // Γραμμικη αναζητηση σε ολα τα block
  for (int i=1; i<= hp_info->last_block; i++){
    CALL_BF(BF_GetBlock(hp_info->fd, i, block));
    char* buffer = BF_Block_GetData(block);
    // Ανακτηση blockInfo για το τωρινο block
    HP_block_info blockInfo;
    memcpy(&blockInfo, buffer + BF_BLOCK_SIZE - sizeof(blockInfo), sizeof(blockInfo));
    // Για καθε εγγραφη μεσα στο μπλοκ
    for (int j=0; j< blockInfo.num_records; j++){
      Record record;
      memcpy(&record, buffer + (sizeof(Record)*j), sizeof(record));
      if (record.id == value){
        printRecord(record);
        blocks_searched = i;
      }        
    }
    CALL_BF(BF_UnpinBlock(block));
  }
 
  return blocks_searched;
}