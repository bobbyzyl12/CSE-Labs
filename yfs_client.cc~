// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}

std::string dirpack(std::string buf,std::string name,std::string fn){
    buf += std::string(name) + '\0'+ fn+'\0';
    return buf;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;

}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
	
	lc->acquire(inum);
	
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
	lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
    
    lc->acquire(inum);

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
	lc->release(inum);
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)	

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    lc->acquire(ino);
    
    fileinfo fin;
    if (getfile_nolock(ino, fin) != OK)
    {
        r =  IOERR;
    }
    else if(ec->get(ino,buf)!=extent_protocol::OK){
      r=IOERR;
    }
    else{
      buf = buf.substr(0,fin.size);
      if(fin.size > size)
      {
        buf = buf.substr(0,size);
        if(ec->put(ino,buf) != extent_protocol::OK)
        {
            r = IOERR;
        }
      }	
      else if(fin.size < size)
      {
        buf.append(size-fin.size,'\0');
        if(ec->put(ino,buf) != extent_protocol::OK)
        {
            r = IOERR;
        }
    }
    }
    
	
	lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    /*
    bool found = false;
    std::string buf;
    fileinfo fin;
    lookup(parent, name, found, ino_out);
    if (found) {
        r = EXIST;
	lc->release(parent);
	return r;
    }
    else if(!isdir(parent)){
      r = NOENT;
      lc->release(parent);
      return r;
    }
    else if((read(parent,fin.size,0,buf)!=OK)||(getfile(parent,fin)!=OK)||(ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)){
      r=IOERR;
      lc->release(parent);
      return r;
    }
    else{
      ec->get(parent, buf);
      buf = dirpack(buf,std::string(name),filename(ino_out));
      if(ec->put(parent, buf)!= extent_protocol::OK)
      {
	r=IOERR;
	lc->release(parent);
	return r;
      }
    }
    return r;
    */
    
    lc->acquire(parent);
    bool found=false;
    lookup(parent,name,found,ino_out);
    if (found) {
	lc->release(parent);
	return EXIST;
    }

    std::string buf;
    ec->create(extent_protocol::T_FILE,ino_out);
    if (ec->get(parent,buf) != extent_protocol::OK) {
	lc->release(parent);
	return IOERR;
    }
    else{
	  std::string s ="";
	  std::string temp = (std::string)name;
	  for(int i=0;i<temp.size();++i){
	    char a,b;
	    char c=temp[i];
	    a = 'a'+c/16;
	    b = 'a'+c%16;
	    s = s+a+b;
	  }
	  buf.append(s + " " + filename(ino_out) + " ");
	  if(ec->put(parent, buf)!= extent_protocol::OK)
	  {
	    r=IOERR;
	    lc->release(parent);
	    return r;
	  }
	}
	
	lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    lc->acquire(parent);
    bool found = false;
    if (lookup(parent, name, found, ino_out) != OK) {
    	r = IOERR;
    	lc->release(parent);
    	return r;
    }
    
	if (found) {
		lc->release(parent);
		return EXIST;
	}
	else{
		std::string buf;
		ec->create(extent_protocol::T_DIR,ino_out);
		ec->get(parent,buf);
		string s ="";
		string temp = (string)name;
		for(int i=0;i<temp.size();++i){
		  char a,b;
		  char c=temp[i];
		  a = 'a'+c/16;
		  b = 'a'+c%16;
		  s = s+a+b;
	}
	  buf.append(s + " " + filename(ino_out) + " ");
		ec->put(parent,buf);
	}
	
	lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
	
    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    if(!isdir(parent)){
      r=NOENT;
    }
    else{
      std::list<dirent> list;
      found=false;
      if(readdir(parent,list)!=OK){
	r=IOERR;
      }
      else{
	for (std::list<dirent>::iterator iter = list.begin(); iter != list.end(); ++iter) {
	  if((iter->name).compare(name)==0){
	    found = true;
            ino_out = iter->inum;
	  }
	}
      }
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    fileinfo file;
	
	if (getfile_nolock(dir, file) != OK) {
		r = IOERR;
		return r;
	}
	
	std::string buf;
	
	if(ec->get(dir,buf)!=extent_protocol::OK){
		return IOERR;
	}
	
	std::stringstream ss(buf);
	std::string filename;
	std::string ino;
	dirent dr;
	while(ss >> filename >> ino){
	    std::string s= "";
	    for(int i=0;i<filename.size();i+=2){
	      s = s + char(((char)filename[i]-'a')*16+(char)filename[i+1]-'a');
	    }
		dr.name = s;
		dr.inum = n2i(ino);
		list.push_back(dr);
	}
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    lc->acquire(ino);

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
	if (ec->get(ino, data) != extent_protocol::OK) {
		r = IOERR;
	}
	else{
	  data = data.substr(off, size);
	}
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	std::string buf;
	lc->acquire(ino);
	if(ec->get(ino,buf)!=extent_protocol::OK)
		return IOERR;
	if(off > (off_t)buf.size()){
		buf.resize(off,'\0');
		buf.append(data,size);
	}
	else{
		if(off+size<buf.size()){
			std::string str;
			size_t count=buf.size()-off-size;
			str=buf.substr(off+size,count);
			buf.resize(off);
			buf.append(data,size);
			buf += str;
		}
		else{
			buf.resize(off);
			buf.append(data,size);
		}
	}
	ec->put(ino,buf);
	bytes_written=size;
	lc->release(ino);		
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    
    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    
    
   lc->acquire(parent); 
   if (!isdir(parent)) {
   		lc->release(parent);
        return NOENT;
    }
    else{
      bool found = false;
      inum ino;
      if (lookup(parent, name, found, ino) != OK) {
        lc->release(parent);
        return IOERR;
    }
    if (!found) {
        lc->release(parent);
        return NOENT;
    }
    if (isdir(ino)) {
        lc->release(parent);
        return IOERR;
    }
    extent_protocol::status res = ec->remove(ino);
    if (res != extent_protocol::OK) {
        lc->release(parent);
        return IOERR;
    }
    std::string buf;
    fileinfo fin;
    if (getfile_nolock(parent, fin) != OK) {
        lc->release(parent);
        return IOERR;
    }
    if (ec->get(parent, buf) != extent_protocol::OK){
	lc->release(parent);
        return IOERR;
    }
    /*
      unsigned long int pos = buf.find(name);
	  unsigned int len =0;
	  while(buf[pos+len]!='\0'){len++;}
	  len++;
	  while(buf[pos+len]!='\0'){len++;}
	 
	  unsigned int size = std::string(name).size()+filename(ino).size()+2;
	  if((len+1)==size){
	    buf.erase(pos,len+1);
	  }
    */
    std::string str = "";
    std::string _name(name);
    for (unsigned int i=0; i<_name.size(); ++i) {
		char a, b;
		a = 'a' + _name[i] / 16;
		b = 'a' + _name[i] % 16;
		str = str + a + b;
	}
    unsigned long int pos = buf.find(str);
    unsigned int len =0;
    while(buf[pos+len]!='\0'){len++;}
    len++;
    while(buf[pos+len]!='\0'){len++;}
    int size = str.size() + filename(ino).size() + 1;
    buf.erase(pos, size);
    
    if(ec->put(parent,buf) != extent_protocol::OK) {
        lc->release(parent);
        return IOERR;
    }
    else{
      lc->release(parent);  
      return r;
    }
    }
    
}

int
yfs_client::symlink(inum parent,const std::string name,const std::string link,inum& ino)
{
	int r=OK;
	bool found;
	
	lc->acquire(parent);
	
	if(lookup(parent,name.c_str(),found,ino)!=OK) {
		lc->release(parent);
		return IOERR;
	}
	
	if(found) {
		lc->release(parent);
		return EXIST;
	}
	
	else{
		std::string buf;
		ec->create(extent_protocol::T_SYM,ino);
		ec->get(parent,buf);
		std::string s ="";
		std::string temp = (string)name;
		for(int i=0;i<temp.size();++i){
		  char a,b;
		  char c=temp[i];
		  a = 'a'+c/16;
		  b = 'a'+c%16;
		  s = s+a+b;
		}
		buf.append(s + " " + filename(ino) + " ");
		ec->put(parent,buf);
	}
	size_t size;
	size_t len=link.size();
	r = write(ino,len,0,link.c_str(),size);
	
	lc->release(parent);
	return r;
}

int
yfs_client::readlink(inum ino,std::string& link)
{
	lc->acquire(ino);
	int r=OK;
	string buf;
	r=ec->get(ino,buf);
	inum tmp;
	bool found = false;
	r=lookup(ino, (const char *)buf.c_str(), found, tmp);
	if (found == true) {
		lc->release(ino);
		r = IOERR;
	}
	link = buf;
	lc->release(ino);
	return r;
}

int
yfs_client::getfile_nolock(inum inum, fileinfo &fin)
{
    int r = OK;
	
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);
    return r;
}
