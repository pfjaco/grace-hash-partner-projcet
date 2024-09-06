#include "Join.hpp"
#include <vector>
#include <utility>
#include <unordered_set>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
 vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
    
    // Partition the left relation
    for (uint i = left_rel.first; i < left_rel.second; i++) {
        mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
        Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        
        for (uint j = 0; j < page->size(); j++) {
            Record record = page->get_record(j);
            uint hash_value = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            
            if (mem->mem_page(hash_value)->full()) {
                partitions[hash_value].add_left_rel_page(mem->flushToDisk(disk, hash_value));
            }
            
            mem->mem_page(hash_value)->loadRecord(record);
        }    
    }
    
    // Flush any remaining pages for the left relation
    for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        if (!mem->mem_page(i)->empty()) {
            partitions[i].add_left_rel_page(mem->flushToDisk(disk, i));
        }
    }

    // Partition the right relation
    for (uint i = right_rel.first; i < right_rel.second; i++) {
        mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
        Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        
        for (uint j = 0; j < page->size(); j++) {
            Record record = page->get_record(j);
            uint hash_value = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            
            if (mem->mem_page(hash_value)->full()) {
                partitions[hash_value].add_right_rel_page(mem->flushToDisk(disk, hash_value));
            }            
            mem->mem_page(hash_value)->loadRecord(record);
        }
    }
    
    // Flush any remaining pages for the right relation
    for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        if (!mem->mem_page(i)->empty()) {
            partitions[i].add_right_rel_page(mem->flushToDisk(disk, i));
        }
    }    
    mem->mem_page(MEM_SIZE_IN_PAGE - 1)->reset();
    return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
    vector<uint> disk_pages;
    
    for (uint i = 0; i < partitions.size(); i++) {
        Bucket& bucket = partitions[i];
        
        bool left_rel_smaller = bucket.num_left_rel_record <= bucket.num_right_rel_record;
        
        vector<uint> smaller_rel_pages = left_rel_smaller ? bucket.get_left_rel() : bucket.get_right_rel();
        vector<uint> larger_rel_pages = left_rel_smaller ? bucket.get_right_rel() : bucket.get_left_rel();
        
        // Load the pages of the smaller relation into memory
        for (uint page_id : smaller_rel_pages) {
            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
            
            for (uint j = 0; j < page->size(); j++) {
                Record record = page->get_record(j);
                uint hash_value = record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                mem->mem_page(hash_value)->loadRecord(record);
            }
            page->reset();
        }

        
        // Probe the larger relation against the loaded records
        for (uint page_id : larger_rel_pages) {
            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
            
            for (uint j = 0; j < page->size(); j++) {
                Record record = page->get_record(j);
                uint hash_value = record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                Page* hash_page = mem->mem_page(hash_value);        
                
                for (uint k = 0; k < hash_page->size(); k++) {
                    Record stored_record = hash_page->get_record(k);
                    if (record == stored_record) {
                        
                            if (mem->mem_page(MEM_SIZE_IN_PAGE - 1)->full()) {
                                disk_pages.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
                            }
                            mem->mem_page(MEM_SIZE_IN_PAGE - 1)->loadPair(left_rel_smaller ? stored_record : record,
                                                                           left_rel_smaller ? record : stored_record);
                            
                        }
                    } 
            }            
        }
        for (uint q = 0; q < MEM_SIZE_IN_PAGE - 1; q++) {
                    if (!mem->mem_page(q)->empty()) {
                        mem->mem_page(q)->reset();
                    }
                }
    }
        
        // Flush any remaining join result pages to disk
        if (!mem->mem_page(MEM_SIZE_IN_PAGE - 1)->empty()) {
            disk_pages.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
        }
        
        // Clear the memory pages used for the hash table
        for (uint p = 0; p < MEM_SIZE_IN_PAGE - 2; p++) {
            mem->mem_page(p)->reset();
        }
    
    
    return disk_pages;
}
