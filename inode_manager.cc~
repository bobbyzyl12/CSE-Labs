#include "inode_manager.h"

#define DBLOCK (BLOCK_NUM / BPB + INODE_NUM + 4)
char a[BLOCK_NUM][BLOCK_SIZE];

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block()
{
    /*
     * your lab1 code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.

     *hint: use macro IBLOCK and BBLOCK.
     use bit operation.
     remind yourself of the layout of disk.
     */
    char buf[BLOCK_SIZE];
   char temp;
   blockid_t id = IBLOCK(INODE_NUM,sb.nblocks) + 1;
   for(;id<BLOCK_NUM;++id){
     uint32_t a = BBLOCK(id);
     read_block(a,buf);
     temp = buf[id %(BLOCK_SIZE * 8) / 8];
     if(((int)temp&(1 <<(7 - id%8)))==0){
       temp |=(1<<(7 - id%8));
       buf[id % (BLOCK_SIZE * 8)/8] = temp;
       write_block(a,buf);
       break;
     }
   } 
   write_block(id,"");
   return id;
}

void block_manager::free_block(uint32_t id)
{
    /*
     * your lab1 code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */
   char buf[BLOCK_SIZE];
   if(id<0||id>BLOCK_NUM){
     printf("wrong id!\n");
     return;
   }
   read_block(BBLOCK(id),buf);
   int off = id %BPB;
   buf[off/6] &= ~(1<<(off%8));
   write_block(BBLOCK(id),buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

}

void block_manager::read_block(uint32_t id, char *buf)
{
    //d->read_block(id, buf);
    memcpy(buf,a[id],BLOCK_SIZE);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
    d->write_block(id, buf);
    memcpy(a[id],buf,BLOCK_SIZE);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type)
{
    /*
     * your lab1 code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().

     * if you get some heap memory, do not forget to free it.
     */
   bool isFull = true;
  for(uint32_t inum = 1;inum <= INODE_NUM;++inum){
    if(get_inode(inum) == NULL){
      isFull = false;
      struct inode newInode;
      newInode.type = type;
      newInode.size = 0;
      newInode.atime = time(0);
      newInode.ctime = time(0);
      newInode.mtime = time(0);
      put_inode(inum,&newInode);
      return inum;
    }
  }
  if(isFull)
  {
    printf("Error! The INODE numbers are all being used! \n ");
    exit(0);
  }
  //return 1;
}

void inode_manager::free_inode(uint32_t inum)
{
    /*
     * your lab1 code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     * do not forget to free memory if necessary.
     */
    struct inode* ino = get_inode(inum);
    if(ino == NULL)
        return;
    ino->type = 0;
    ino->mtime = time(NULL);
    put_inode(inum, ino);
    free(ino);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* inode_manager::get_inode(uint32_t inum)
{
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode*)buf + inum%IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode*)malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode*)buf + inum%IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}


uint32_t get_blocks(uint32_t size){
	uint32_t res=size/BLOCK_SIZE;
	if(size%BLOCK_SIZE==0)
		return res;
	else
		return res+1;
}
#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
    /*
     * your lab1 code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */
    if(buf_out == NULL || size == NULL)
        return;
    struct inode* ino = get_inode(inum);
    if(ino == NULL)
        return;
    ino->atime = time(NULL);
    put_inode(inum, ino);

    *size = ino->size;
    char* buf = (char*)malloc(*size);
    uint32_t num = (*size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    uint32_t i;
    char tmp[BLOCK_SIZE];
    for(i=0; i<MIN(num, NDIRECT); i++){
        bm->read_block(ino->blocks[i], tmp);
        memcpy(buf+i*BLOCK_SIZE, tmp,
                (i == num-1)? *size-i*BLOCK_SIZE:BLOCK_SIZE);
    }

    if(num > NDIRECT){
        char blk[BLOCK_SIZE];
        bm->read_block(ino->blocks[NDIRECT], blk);
        uint32_t* ip = (uint32_t*)blk;
        for(; i<num; i++){
            bm->read_block(ip[i-NDIRECT], tmp);
            memcpy(buf+i*BLOCK_SIZE, tmp,
                    (i == num-1)? *size-i*BLOCK_SIZE:BLOCK_SIZE);
        }
    }
    *buf_out = buf;
    free(ino);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
    /*
     * your lab1 code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode.
     * you should free some blocks if necessary.
     */
    if(size < 0 || (uint32_t)size > BLOCK_SIZE * MAXFILE){
        printf("\tim: error! size negative or too large.\n");
        return;
    }
    struct inode* ino = get_inode(inum);
    if(ino==NULL){
        printf("\tim: error! inode not exist.\n");
        return;
    }

    uint32_t pre_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    uint32_t num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    uint32_t i, id, *ip;
    char blk[BLOCK_SIZE];
    id = ino->blocks[NDIRECT];
    ip = (uint32_t*)blk;
    if(pre_num < num){
        if(pre_num > NDIRECT){
            bm->read_block(id, blk);
            for(i=pre_num; i<num; i++)
                ip[i-NDIRECT] = bm->alloc_block();
            bm->write_block(id, blk);
        }
        else if(num > NDIRECT){
            for(i=pre_num; i<NDIRECT; i++)
                ino->blocks[i] = bm->alloc_block();
            id = bm->alloc_block();
            ino->blocks[NDIRECT] = id;
            for(i=0; i<num-NDIRECT; i++)
                ip[i] = bm->alloc_block();
            bm->write_block(id, blk);
        }
        else{
            for(i=pre_num; i<num; i++)
                ino->blocks[i] = bm->alloc_block();
        }
    }
    else if(pre_num > num){
        if(num > NDIRECT){
            bm->read_block(id, blk);
            for(i=num; i<pre_num; i++)
                bm->free_block(ip[i-NDIRECT]);
        }
        else if(pre_num > NDIRECT){
            bm->read_block(id, blk);
            for(i=num; i<NDIRECT; i++)
                bm->free_block(ino->blocks[i]);
            for(i=0; i<num-NDIRECT; i++)
                bm->free_block(ip[i]);
        }
        else{
            for(i=num; i<pre_num; i++)
                bm->free_block(ino->blocks[i]);
        }
    }

    char tmp[BLOCK_SIZE];
    for(i=0; i<MIN(num, NDIRECT); i++){
        memcpy(tmp, buf+i*BLOCK_SIZE, BLOCK_SIZE);
        bm->write_block(ino->blocks[i], tmp);
    }

    if(num > NDIRECT){
        bm->read_block(ino->blocks[NDIRECT], blk);
        for(; i<num; i++){
            memcpy(tmp, buf+i*BLOCK_SIZE, BLOCK_SIZE);
            bm->write_block(ip[i-NDIRECT], tmp);
        }
    }

    ino->size = size;
    ino->mtime = time(NULL);
    ino->ctime = time(NULL);
    put_inode(inum, ino);
    free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode* currentInode = get_inode(inum);
  if(currentInode == NULL){return;}
  else{
     a.type = currentInode -> type;
     a.size = currentInode -> size;
     a.atime = currentInode -> atime;
     a.mtime = currentInode -> mtime;
     a.ctime = currentInode -> ctime;
     free(currentInode);
     return;
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
  struct inode *currentInode = get_inode(inum);
  uint32_t blockNum = 0;
  if(currentInode ->size %BLOCK_SIZE == 0){
    blockNum = currentInode->size /BLOCK_SIZE;
  }
  else{
    blockNum = currentInode->size/BLOCK_SIZE + 1;
  }
  
  if(blockNum <=NDIRECT){
     free_inode(inum);
  }
  else{
    blockid_t *buf =(blockid_t *)malloc(BLOCK_SIZE);
    bm ->read_block(currentInode->blocks[NDIRECT],(char *)buf);
    for(uint32_t i=0;i<blockNum-NDIRECT;++i){
      bm->free_block(buf[i]);
    }
  }
  return;
}

