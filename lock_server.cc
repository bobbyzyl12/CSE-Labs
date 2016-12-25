// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}


lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab4 code goes here
  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t, int>::iterator iter = locks.find(lid);
  if(iter == locks.end()){
    locks.insert(std::pair<lock_protocol::lockid_t, int>(lid, 1));
  }
  else {
    if(iter->second == 0){iter->second = 1;}
    else{
      while(iter->second == 1){
	pthread_cond_wait(&cond, &mutex);
      }
      iter->second = 1;
    } 
  }
  r = lid;
  nacquire++;
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab4 code goes here
  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t, int>::iterator iter = locks.find(lid) ;
  if(iter == locks.end()){
    return lock_protocol::NOENT;
  }
  else {
     if(iter->second == 0){
      return lock_protocol::RETRY;
     }
     else{
	iter->second = 0;
      }
  }
  nacquire--;
  r = lid;
  pthread_mutex_unlock(&mutex);
  pthread_cond_signal(&cond);
  return ret;
}
