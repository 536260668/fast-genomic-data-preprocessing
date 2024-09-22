#include <iostream>
#include <cassert>
#include "bam_parser.h"

sam_hdr_t * BamParser::header;

BamParser::BamParser(): index(0), line(nullptr){

};

BamParser::~BamParser()
{
    if(line)
    {
        clear();
    }
}

bool BamParser::has_record(){
    if(!records.empty()){
        return true;
    }
    auto tmp = construct_BAMRecord();
    if(tmp != nullptr){
        records.push_back(tmp);
        return true;
    }else{
        return false;
    }
}

BAMRecord* BamParser::construct_BAMRecord(){
  // run out of read
  if(!line)
  {
      return nullptr;
  }

  if(index >= line->size())
  {
        clear();
        return nullptr;
  }

  auto p = new BAMRecord; //---this memory block needs to be moved to the buffer pool later
  int ret = sam_parse1(&(*line)[index], header, &p->record);
  
  if(ret < 0)
    std::cout << index << "\t" << (*line)[index].s << std::endl;
  assert(ret >= 0);
  index++;
  
  bam_set_mempolicy(&(p->record), BAM_USER_OWNS_STRUCT);
  if((p->record.core.flag & (BAM_FUNMAP | BAM_FSECONDARY | BAM_FSUPPLEMENTARY)) == 0){
    p->set_pairID(1);
  }else{
    p->set_pairID(0);
  }
  return p;
}

void BamParser::clear()
{
    if(line)
    {
        for(auto & item : *line)
        {
            free(item.s);
        }
        delete line;
        line = nullptr;
    } 
    index = 0;
}

std::unique_ptr<BAMRecord> BamParser::pop_record(const uint64_t pairID){
  auto ret = records.front();
  records.pop_front();
  if(!ret->ignorable()){
    ret->set_pairID(pairID);
  }
  return std::unique_ptr<BAMRecord>(ret);
}

std::unique_ptr<BAMRecord> BamParser::pop_record(const uint64_t pairID, const BAMRecord* hint){
  if(hint->ignorable() || has_record() == false){
    return nullptr;
  }
  BAMRecord* ret;
  for(auto iter = records.begin(); iter != records.end(); iter++){
    if(strcmp(hint->qname(), (*iter)->qname()) != 0){
      return nullptr;
    }
    if((*iter)->ignorable() == false){
      ret = *iter;
      records.erase(iter);
      ret->set_pairID(pairID);
      return std::unique_ptr<BAMRecord>(ret);
    }
  }
  while((ret = construct_BAMRecord()) != nullptr){
    records.push_back(ret);
    if(strcmp(hint->qname(), ret->qname()) != 0){
      return nullptr;
    }
    if(ret->ignorable() == false){
      records.pop_back();
      ret->set_pairID(pairID);
      return std::unique_ptr<BAMRecord>(ret);
    }
  }
  return nullptr;
}

void BamParser::add_line(std::vector<kstring_t> * line)
{
    if(this->line)
    {
        clear();
    }
    this->line = line;
}