#pragma once

#include <array>
#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#ifdef USE_PMDK
#include <libpmemobj.h>
#endif
#include <cmath>
#include <mutex>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>
// #include <gperftools/profiler.h>
#include "blindi/blindi_seqtree.hpp"


#define CACHE_LINE_SIZE 64
#define IS_FORWARD(c) (c % 2 == 0)

#define NO_CLFLUSH 

#ifndef KEYSIZE
#define KEYSIZE 1
#endif
#define CARDINALITY 20
constexpr size_t key_size = KEYSIZE;
using entry_key_t = std::array<uint8_t, key_size>;
inline bool operator<(const entry_key_t& a, const entry_key_t& b)
{
    assert(a.size() == b.size());
    for (int i = a.size() - 1; i >= 0; --i)
    {
        if (a[i] < b[i]) return true;
        if (a[i] > b[i]) return false;
    }
    return false;
}
inline bool operator> (const entry_key_t& lhs, const entry_key_t& rhs){ return rhs < lhs; }
inline bool operator<=(const entry_key_t& lhs, const entry_key_t& rhs){ return !(lhs > rhs); }
inline bool operator>=(const entry_key_t& lhs, const entry_key_t& rhs){ return !(lhs < rhs); }

std::ostream & operator<< (std::ostream & os, const entry_key_t & key)
{
    auto first = true;
    for (const auto & el : key)
    {
        if (!first)
        {
            std::cout << " ";
        }
        first = false;
        std::cout << (int) el;
    }
    return os;
}

pthread_mutex_t print_mtx;

const uint64_t SPACE_PER_THREAD = 150ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread uint8_t *start_addr;
extern __thread uint8_t *curr_addr;

using namespace std;

inline void mfence()
{
    asm volatile("mfence":::"memory");
}

inline void clflush(uint8_t *data, int len)
{
#ifdef NO_CLFLUSH	
    return;
#endif    
    volatile uint8_t *ptr = (uint8_t *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    mfence();
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile uint8_t *)ptr));
    }
    mfence();
}
namespace Blindi{
    template <typename T>
    struct list_node_t {
        T value;
        entry_key_t key_t;
        inline entry_key_t & key()
        {
            return key_t;
        }
        bool isUpdate;
        bool isDelete;
        struct list_node_t *next;
        void printAll();
    };
}


template <typename T>
void Blindi::list_node_t<T>::printAll() {
    printf("addr=%p, key=%d, ptr=%u, isUpdate=%d, isDelete=%d, next=%p\n",
                    this, this->key(), this->value, this->isUpdate, this->isDelete, this->next);
}
#ifdef USE_PMDK
POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, list_node_t);
POBJ_LAYOUT_END(btree);
PMEMobjpool *pop;
#endif
template <typename T>
T *alloc() {
    auto size = sizeof(T);
#ifdef USE_PMDK
    TOID(list_node_t) p;
    POBJ_ZALLOC(pop, &p, list_node_t, size);
    return reinterpret_cast<T*>(pmemobj_direct(p.oid));
#elif NVRAM
    auto ret = reinterpret_cast<T*>(curr_addr);
    if (reinterpret_cast<size_t>(curr_addr) % 8 != 0)
    {
        std::cerr << "Unaligned allocation at " << reinterpret_cast<size_t>(curr_addr) << " with size " << size << std::endl;
    }
    memset(ret, 0, size);
    curr_addr += size;
    if (curr_addr >= start_addr + SPACE_PER_THREAD) {
        printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no "
                     "free space to alloc\n",
                     start_addr, curr_addr, SPACE_PER_THREAD);
        exit(19);
    }
    return ret;
#else
    auto ret = new T;
    memset(ret, 0, size);
    return ret;
#endif
}

template <typename T>
class page;
template <typename T>
class page_internal;
template <typename T>
class page_leaf;

template <typename T>
class btree{
private:
    int height;

public:
    using U = typename std::remove_pointer_t<T>;
    Blindi::list_node_t<T> *list_head = nullptr;
    page<T>* root;
    btree();
    ~btree();
    std::vector<page<T>*> all_pages();
    size_t getMemoryUsed();
    size_t getPersistentMemoryUsed();
    std::vector<T> scan(entry_key_t, size_t size);
    std::vector<U> secondaryScan(entry_key_t, size_t size);
    void setNewRoot(page<T> *new_root);
    void btree_insert_pred(Blindi::list_node_t<T>*, Blindi::list_node_t<T> **pred, bool &);
    void btree_insert_internal(entry_key_t, page<T> *, uint32_t);
    void btree_delete(entry_key_t key);
    Blindi::list_node_t<T> *btree_search_pred(entry_key_t, bool & found, Blindi::list_node_t<T>**, bool debug = false);
    void printAll();
    void printAllKeys();
    void printAllKeysByPage();
    T* insert(entry_key_t, T value);
    void remove(entry_key_t);
    T* search(entry_key_t);
    T* search_exact(entry_key_t);

    void print()
    {
        int i = 0;
        Blindi::list_node_t<T> *tmp = list_head;
        while (tmp->next != nullptr) {
            //printf("%d-%d\t", tmp->next->key, tmp->next->ptr);
            tmp = tmp->next;
            printf("node=%d, ", i);
            tmp->printAll();
            i++;
        }
        printf("\n");
    }
    friend class page<T>;
};

template <typename T>
struct header_internal{
    page<T>* leftmost_ptr = nullptr;          // 8 bytes
    page_internal<T>* sibling_ptr = nullptr;  // 8 bytes
    page_internal<T>* pred_ptr = nullptr;     // 8 bytes
    uint32_t level = 0;                       // 4 bytes
    uint8_t switch_counter = 0;               // 1 bytes
    uint8_t is_deleted = false;               // 1 bytes
    int16_t last_index = -1;                  // 2 bytes
    std::unique_ptr<std::mutex> mtx;          // 8 bytes

    friend class btree<T>;

    header_internal() : mtx { std::make_unique<std::mutex>() } {}
};


template <typename T>
struct header_leaf{
    page_leaf<T>* sibling_ptr = nullptr;  // 8 bytes
    page_leaf<T>* pred_ptr = nullptr;     // 8 bytes
    uint8_t is_deleted = false;           // 1 bytes
#ifdef UTREE
    uint8_t switch_counter = 0;           // 1 bytes
    int16_t last_index = -1;              // 2 bytes
#endif
    std::unique_ptr<std::mutex> mtx;      // 8 bytes

    friend class btree<T>;

    header_leaf() : mtx { std::make_unique<std::mutex>() } {}
};

constexpr size_t nextPowerOf2(size_t n)
{
    size_t ret = 1;
    while(n > ret) {
        ret <<= 1;
    }
    return ret;
}

template <typename T>
class page{
public:
    friend class btree<T>;

    virtual ~page() = default;
    virtual bool is_internal() const = 0;

    virtual inline int count() = 0;
    virtual inline bool remove_key(entry_key_t key) = 0;

    virtual void print() = 0;
    virtual void printAll() = 0;
};

template <typename T>
struct entry_internal{
    entry_key_t key;
    page<T>* ptr = nullptr;
};

template <typename T>
class page_internal : public page<T>{

    constexpr static size_t PAGESIZE = nextPowerOf2(sizeof(header_internal<T>) + 20 * sizeof(entry_internal<T>));
    constexpr static size_t cardinality = (PAGESIZE-sizeof(header_internal<T>))/sizeof(entry_internal<T>);
private:
    header_internal<T> hdr;
    std::array<entry_internal<T>, cardinality> records; // slots in persistent memory, 16 bytes * n

public:
    friend class btree<T>;

    page_internal(uint32_t level = 0) {
        hdr.level = level;
        records[0].ptr = nullptr;
    }

    // this is called when tree grows
    page_internal(page<T>* left, entry_key_t key, page<T>* right, uint32_t level = 0) {
        hdr.leftmost_ptr = left;
        hdr.level = level;
        records[0].key = key;
        records[0].ptr = right;
        records[1].ptr = nullptr;

        hdr.last_index = 0;
    }

    void *operator new(size_t size) {
        void *ret;
        [[maybe_unused]] auto r = posix_memalign(&ret, 64, size);
        return ret;
    }

    void operator delete(void* ptr) noexcept {
        free(ptr);
    }

    virtual bool is_internal() const override { return true; }

    inline int count() override {
        uint8_t previous_switch_counter;
        int count = 0;
        do {
            previous_switch_counter = hdr.switch_counter;
            count = hdr.last_index + 1;

            while(count >= 0 && records[count].ptr != nullptr) {
                if(IS_FORWARD(previous_switch_counter))
                    ++count;
                else
                    --count;
            }

            if(count < 0) {
                count = 0;
                while(records[count].ptr != nullptr) {
                    ++count;
                }
            }

        } while(previous_switch_counter != hdr.switch_counter);

        return count;
    }

    inline bool remove_key(entry_key_t key) override {
        // Set the switch_counter
        if(IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        bool shift = false;
        int i;
        for(i = 0; records[i].ptr != nullptr; ++i) {
            if(!shift && records[i].key == key) {
                records[i].ptr = (i == 0) ?
                    hdr.leftmost_ptr : records[i - 1].ptr;
                shift = true;
            }

            if(shift) {
                records[i].key = records[i + 1].key;
                records[i].ptr = records[i + 1].ptr;
            }
        }

        if(shift) {
            --hdr.last_index;
        }
        return shift;
    }

    bool remove_internal(entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
        const std::lock_guard<std::mutex> lock(*hdr.mtx);
        return remove_key(key);
    }

    // revised
    inline void insert_key_internal(entry_key_t key, page<T> * ptr, int & num_entries,
                           bool update_last_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        // FAST
        if(num_entries == 0) {  // this page is empty
            auto new_entry = &records[0];
            auto array_end = &records[1];
            new_entry->key = key;
            new_entry->ptr = ptr;

            array_end->ptr = nullptr;
        }
        else {
            bool inserted = false;
            records.at(num_entries+1).ptr = records.at(num_entries).ptr;

            // FAST
            for(int i = num_entries - 1; i >= 0; i--) {
                if(key < records[i].key ) {
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = records[i].key;
                }
                else{
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = key;
                    records[i+1].ptr = ptr;
                    inserted = true;
                    break;
                }
            }
            if(!inserted){
                records[0].ptr = hdr.leftmost_ptr;
                records[0].key = key;
                records[0].ptr = ptr;
            }
        }

        if(update_last_index) {
            hdr.last_index = num_entries;
        }
        ++num_entries;
    }

    // Insert a new key - FAST and FAIR
    page<T> *store_internal(btree<T>* bt, entry_key_t key, page<T>* right,
         bool flush, bool with_lock) {
        std::unique_lock<std::mutex> lock(*hdr.mtx, std::defer_lock);
        if(with_lock) {
            lock.lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
            return nullptr;
        }

        int num_entries = count();

	// MOSHIK: if we have duplicated key we update the value to a new value
        for (int i = 0; i < num_entries; i++)
            if (key == records[i].key) {
                records[i].ptr = right;
                return this;
            }

        // If this node has a sibling node,
        if(hdr.sibling_ptr) {
            // Compare this key with the first key of the sibling
            if(key > hdr.sibling_ptr->records[0].key) {
                if(with_lock) {
                    lock.unlock(); // Unlock the write lock
                }
                return hdr.sibling_ptr->store_internal(bt, key, right,
                        true, with_lock);
            }
        }


        // FAST
        if(num_entries < cardinality - 1) {
            insert_key_internal(key, right, num_entries);
            return this;
        }
        else {// FAIR
            // overflow
            // create a new node
            auto sibling = new page_internal<T>(hdr.level);
            int m = (int) ceil(num_entries/2);
            entry_key_t split_key = records[m].key;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            for(int i=m+1;i<num_entries;++i){
                sibling->insert_key_internal(records[i].key, records[i].ptr, sibling_cnt, false);
            }
            sibling->hdr.leftmost_ptr = records[m].ptr;

            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            sibling->hdr.pred_ptr = this;
            if (sibling->hdr.sibling_ptr != nullptr)
                sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
            hdr.sibling_ptr = sibling;

            // set to nullptr
            if(IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter += 2;
            else
                ++hdr.switch_counter;
            records[m].ptr = nullptr;
            hdr.last_index = m - 1;
            num_entries = hdr.last_index + 1;

            page<T> *ret;

            // insert the key
            if(key < split_key) {
                insert_key_internal(key, right, num_entries);
                ret = this;
            }
            else {
                sibling->insert_key_internal(key, right, sibling_cnt);
                ret = sibling;
            }

            // Set a new root or insert the split key to the parent
            if(bt->root == this) { // only one node can update the root ptr
                auto new_root = new page_internal<T>(this, split_key, sibling, hdr.level + 1);
                bt->setNewRoot(new_root);
            }
            else {
                if(with_lock) {
                    lock.unlock(); // Unlock the write lock
                }
                bt->btree_insert_internal(split_key, sibling,
                        hdr.level + 1);
            }

            return ret;
        }

    }

    page<T> *linear_search_internal(entry_key_t key) {
        uint8_t previous_switch_counter;
        page<T> *ret = nullptr;

        do {
            previous_switch_counter = hdr.switch_counter;
            ret = nullptr;
            page<T> *t;
            entry_key_t k;

            if(IS_FORWARD(previous_switch_counter)) {
                if(key < (k = records[0].key)) {
                    if((t = hdr.leftmost_ptr) != records[0].ptr) {
                        ret = t;
                        continue;
                    }
                }
                int i = 1;
                for(i = 1; records[i].ptr != nullptr; ++i) {
                    if(key < (k = records[i].key)) {
                        if((t = records[i-1].ptr) != records[i].ptr) {
                            ret = t;
                            break;
                        }
                    }
                }

                if(!ret) {
                    ret = records[i - 1].ptr;
                    continue;
                }
            }
            else { // search from right to left
                for(int i = count() - 1; i >= 0; --i) {
                    if(key >= (k = records[i].key)) {
                        if(i == 0) {
                            if(hdr.leftmost_ptr != (t = records[i].ptr)) {
                                ret = t;
                                break;
                            }
                        }
                        else {
                            if(records[i - 1].ptr != (t = records[i].ptr)) {
                                ret = t;
                                break;
                            }
                        }
                    }
                }
            }
        } while(hdr.switch_counter != previous_switch_counter);

        auto t = hdr.sibling_ptr;
        if(t != nullptr) {
            assert(t->is_internal());
            if(key >= t->records[0].key)
                return t;
        }

        if(ret) {
            return ret;
        }
        return hdr.leftmost_ptr;
    }

    // print a node
    void print() override {
        printf("[%d] internal %x \n", this->hdr.level, this);
        printf("last_index: %d\n", hdr.last_index);
        printf("switch_counter: %d\n", hdr.switch_counter);
        printf("search direction: ");
        if(IS_FORWARD(hdr.switch_counter))
            printf("->\n");
        else
            printf("<-\n");

        
        printf("%x ",hdr.leftmost_ptr);

        for(int i=0;records[i].ptr != nullptr;++i)
            printf("%ld,%x ", records[i].key, records[i].ptr);

        printf("\n%x ", hdr.sibling_ptr);

        printf("\n");
    }

    void printAll() override {
        printf("printing internal node: ");
        print();
        hdr.leftmost_ptr->printAll();
        for(int i=0;records[i].ptr != nullptr;++i){
            records[i].ptr->printAll();
        }
    }
};


template <typename T>
struct entry_leaf{
    entry_key_t key;
    Blindi::list_node_t<T>* ptr = nullptr; // 8 bytes
};

template <typename T>
class page_leaf : public page<T> {
    friend class btree<T>;
    header_leaf<T> hdr;

public:
    void *operator new(size_t size) {
        void *ret;
        [[maybe_unused]] auto r = posix_memalign(&ret, 64, size);
        return ret;
    }

    void operator delete(void* ptr) noexcept {
        free(ptr);
    }

    virtual bool is_internal() const override { return false; }

#if UTREE
private:
    // constexpr static size_t PAGESIZE = nextPowerOf2(sizeof(header_leaf<T>) + 20 * sizeof(entry_leaf<T>));
    // constexpr static size_t cardinality = (PAGESIZE-sizeof(header_leaf<T>))/sizeof(entry_leaf<T>);
    constexpr static size_t cardinality = CARDINALITY;
    std::array<entry_leaf<T>, cardinality> records; // slots in persistent memory, 16 bytes * n
public:

    page_leaf() {
        records[0].ptr = nullptr;
    }

    inline int count() override {
        uint8_t previous_switch_counter;
        int count = 0;
        do {
            previous_switch_counter = hdr.switch_counter;
            count = hdr.last_index + 1;

            while(count >= 0 && records[count].ptr != nullptr) {
                if(IS_FORWARD(previous_switch_counter))
                    ++count;
                else
                    --count;
            }

            if(count < 0) {
                count = 0;
                while(records[count].ptr != nullptr) {
                    ++count;
                }
            }

        } while(previous_switch_counter != hdr.switch_counter);

        return count;
    }

    inline bool remove_key(entry_key_t key) override {
        // Set the switch_counter
        if(IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        bool shift = false;
        int i;
        for(i = 0; records[i].ptr != nullptr; ++i) {
            if(!shift && records[i].key == key) {
                records[i].ptr = (i == 0) ? nullptr : records[i - 1].ptr;
                shift = true;
            }

            if(shift) {
                records[i].key = records[i + 1].key;
                records[i].ptr = records[i + 1].ptr;
            }
        }

        if(shift) {
            --hdr.last_index;
        }
        return shift;
    }

    bool remove_leaf(entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
        const std::lock_guard<std::mutex> lock(*hdr.mtx);
        return remove_key(key);
    }

    // revised
    inline void insert_key_leaf(Blindi:: list_node_t<T> * ptr, int & num_entries, Blindi::list_node_t<T> **pred,
                           bool update_last_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        // FAST
        if(num_entries == 0) {  // this page is empty
            auto new_entry = &records[0];
            auto array_end = &records[1];
            new_entry->key = ptr->key();
            new_entry->ptr = ptr;

            array_end->ptr = nullptr;

            if (pred != nullptr && hdr.pred_ptr != nullptr) {
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
            }
        }
        else {
            bool inserted = false;
            records.at(num_entries+1).ptr = records.at(num_entries).ptr;

            // FAST
            for(int i = num_entries - 1; i >= 0; i--) {
                if(ptr->key() < records[i].key ) {
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = records[i].key;
                }
                else{
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = ptr->key();
                    records[i+1].ptr = ptr;
                    if (pred != nullptr) {
                        *pred = records[i].ptr;
                    }
                    inserted = true;
                    break;
                }
            }
            if(!inserted){
                records[0].ptr = nullptr;
                records[0].key = ptr->key();
                records[0].ptr = ptr;
                if (pred != nullptr && hdr.pred_ptr != nullptr) {
                    *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                }
            }
        }

        if(update_last_index) {
            hdr.last_index = num_entries;
        }
        ++num_entries;
    }

    page<T> *store_leaf(btree<T>* bt, Blindi::list_node_t<T>* right,
                bool with_lock, Blindi::list_node_t<T> **pred) {
        std::unique_lock<std::mutex> lock(*hdr.mtx, std::defer_lock);
        if(with_lock) {
            lock.lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
            return nullptr;
        }

        int num_entries = count();

        for (int i = 0; i < num_entries; i++) {
            if (right->key() == records[i].key) {
                // Already exists, we don't need to do anything, just return.
                *pred = records[i].ptr;
                return nullptr;
            }
        }

        // If this node has a sibling node,
        if(hdr.sibling_ptr) {
            // Compare this key with the first key of the sibling
            if(right->key() > hdr.sibling_ptr->records[0].key) {
                if(with_lock) {
                    lock.unlock(); // Unlock the write lock
                }
                return hdr.sibling_ptr->store_leaf(bt, right, with_lock, pred);
            }
        }


        // FAST
        if(num_entries < cardinality - 1) {
            insert_key_leaf(right, num_entries, pred);
            return this;
        } else {// FAIR
            // overflow
            // create a new node
            auto sibling = new page_leaf();
            int m = (int) ceil(num_entries/2);
            entry_key_t split_key = records[m].key;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            for(int i=m; i<num_entries; ++i){
                sibling->insert_key_leaf(records[i].ptr, sibling_cnt, nullptr, false);
            }

            // b+tree
            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            sibling->hdr.pred_ptr = this;
            if (sibling->hdr.sibling_ptr != nullptr)
                sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
            hdr.sibling_ptr = sibling;

            // set to nullptr
            if(IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter += 2;
            else
                ++hdr.switch_counter;
            records[m].ptr = nullptr;
            hdr.last_index = m - 1;
            num_entries = hdr.last_index + 1;

            page_leaf *ret;

            // insert the key
            if(right->key() < split_key) {
                insert_key_leaf(right, num_entries, pred);
                ret = this;
            }
            else {
                sibling->insert_key_leaf(right, sibling_cnt, pred);
                ret = sibling;
            }

            // Set a new root or insert the split key to the parent
            if(bt->root == this) { // only one node can update the root ptr
                auto new_root = new page_internal<T>(this, split_key, sibling, 1);
                bt->setNewRoot(new_root);
            }
            else {
                if(with_lock) {
                    lock.unlock();
                }
                bt->btree_insert_internal(split_key, sibling, 1);
            }
            return ret;
        }
    }

    void *search_pred(entry_key_t key, Blindi::list_node_t<T> **pred, bool debug=false) {
        uint8_t previous_switch_counter;
        Blindi::list_node_t<T> *ret = nullptr;
        Blindi::list_node_t<T> *t;

        do {
            previous_switch_counter = hdr.switch_counter;
            ret = nullptr;

            // search from left to right
            if(IS_FORWARD(previous_switch_counter)) {
                if (debug) {
                    printf("search from left to right\n");
                    printf("page:\n");
                    printAll();
                }
                entry_key_t k = records[0].key;
                if (key < k) {
                    if (hdr.pred_ptr != nullptr){
                        *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                        if (debug)
                            printf("line 752, *pred=%p\n", *pred);
                    }
                }
                if (key > k){
                    *pred = records[0].ptr;
                    if (debug)
                        printf("line 757, *pred=%p\n", *pred);
                }


                if(k == key) {
                    if (hdr.pred_ptr != nullptr) {
                        *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                        if (debug)
                            printf("line 772, *pred=%p\n", *pred);
                    }
                    if((t = records[0].ptr) != nullptr) {
                        if(k == records[0].key) {
                            ret = t;
                            continue;
                        }
                    }
                }

                for(int i=1; records[i].ptr != nullptr; ++i) {
                    entry_key_t k = records[i].key;
                    if (k < key){
                        *pred = records[i].ptr;
                        if (debug)
                            printf("line 775, *pred=%p\n", *pred);
                    }
                    if(k == key) {
                        if(records[i-1].ptr != (t = records[i].ptr)) {
                            if(k == records[i].key) {
                                ret = t;
                                break;
                            }
                        }
                    }
                }
            }else { // search from right to left
                if (debug){
                    printf("search from right to left\n");
                    printf("page:\n");
                    printAll();
                }
                bool once = true;

                for (int i = count() - 1; i > 0; --i) {
                    if (debug)
                        printf("line 793, i=%d, records[i].key=%d\n", i,
                                     records[i].key);
                    entry_key_t k = records[i].key;
                    entry_key_t k1 = records[i - 1].key;
                    if (k1 < key && once) {
                        *pred = records[i - 1].ptr;
                        if (debug)
                            printf("line 794, *pred=%p\n", *pred);
                        once = false;
                    }
                    if(k == key) {
                        if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                            if(k == records[i].key) {
                                ret = t;
                                break;
                            }
                        }
                    }
                }

                if(!ret) {
                    entry_key_t k = records[0].key;
                    if (key < k){
                        if (hdr.pred_ptr != nullptr){
                            *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                            if (debug)
                                printf("line 811, *pred=%p\n", *pred);
                        }
                    }
                    if (key > k)
                        *pred = records[0].ptr;
                    if(k == key) {
                        if (hdr.pred_ptr != nullptr) {
                            *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                            if (debug)
                                printf("line 844, *pred=%p\n", *pred);
                        }
                        if(nullptr != (t = records[0].ptr) && t) {
                            if(k == records[0].key) {
                                ret = t;
                                continue;
                            }
                        }
                    }
                }
            }
        } while(hdr.switch_counter != previous_switch_counter);

        if(ret) {
            return ret;
        }

        if(hdr.sibling_ptr && key >= hdr.sibling_ptr->records[0].key)
            return hdr.sibling_ptr;

        return nullptr;
    }

    void *search_exact(entry_key_t key, bool debug=false) {
        Blindi::list_node_t<T> *ptr;
        return search_pred(key, &ptr, debug);
    }

    // print a node
    void print() override{
        printf("[%d] leaf %x \n", 0, this);
        printf("last_index: %d\n", hdr.last_index);
        printf("switch_counter: %d\n", hdr.switch_counter);
        printf("search direction: ");
        if(IS_FORWARD(hdr.switch_counter))
            printf("->\n");
        else
            printf("<-\n");

        for(int i=0;records[i].ptr != nullptr;++i)
            printf("%ld,%x ", records[i].key, records[i].ptr);

        printf("\n%x ", (void*) hdr.sibling_ptr);
        printf("\n");
    }

#else // --------------------------blindi -------------------------------
private:
    constexpr static size_t cardinality = CARDINALITY;
    typedef GenericBlindiSeqTreeNode <SeqTreeBlindiNode<entry_key_t, cardinality>, entry_key_t, cardinality> blindi_node;
    blindi_node node1;
public:

    inline int count() override {
        return node1.get_valid_len();
    }

    inline void insert_key_leaf(Blindi::list_node_t<T> * ptr, Blindi::list_node_t<T> **pred) {
        node1.Insert2BlindiNodeWithKey(ptr->key().data(), ptr->key().size());
        bool hit, smaller_than_node = 0;
        uint16_t tree_traverse_len;
        uint16_t tree_traverse[cardinality] = {0};
        auto position = node1.SearchBlindiNode(ptr->key().data(), ptr->key().size(), SEARCH_TYPE::PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        *pred = reinterpret_cast<Blindi::list_node_t<T>*>(node1.get_key_ptr(position));
    }

    inline Blindi::list_node_t<T>* get_ptr(int i) {
        return reinterpret_cast<Blindi::list_node_t<T>*>(node1.get_key_ptr(i));
    }

    inline entry_key_t get_first_key() {
        return *reinterpret_cast<entry_key_t*>(node1.get_key_ptr(0));
    }



    inline bool remove_key(entry_key_t key) override {
         // Set the switch_counter
        auto result = node1.RemoveFromBlindiNodeWithKey(key.data());
        return result == REMOVE_SUCCESS;
    }

    bool remove_leaf(entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
        const std::lock_guard<std::mutex> lock(*hdr.mtx);
        return remove_key(key);
    }

    page<T> *store_leaf(btree<T>* bt, Blindi::list_node_t<T>* ptr,
                bool with_lock, Blindi::list_node_t<T> **pred) {
        std::unique_lock<std::mutex> lock(*hdr.mtx, std::defer_lock);
        if(with_lock) {
            lock.lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
            return nullptr;
        }

        bool hit, smaller_than_node = 0;
        uint16_t tree_traverse_len;
        uint16_t tree_traverse[cardinality] = {0};
        auto position = node1.SearchBlindiNode(ptr->key().data(), ptr->key().size(), SEARCH_TYPE::POINT_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        if (hit)
        {
            *pred = get_ptr(position);
            return nullptr;
        }

        // Compare this key with the first key of the sibling if it exists
        if(hdr.sibling_ptr && ptr->key() > hdr.sibling_ptr->get_first_key()) {
            if(with_lock) {
                lock.unlock(); // Unlock the write lock
            }
            return hdr.sibling_ptr->store_leaf(bt, ptr, with_lock, pred);
        }

        // normal insert
        if(count() < cardinality) {
            insert_key_leaf(ptr, pred);
            return this;
        }

        auto former_length = count();
        auto sibling = new page_leaf();
        blindi_node node_small;
        blindi_node node_large;
        node1.SplitBlindiNode(&node_small, &node_large);
        node1 = node_small;
        sibling->node1 = node_large;
        assert(node_small.get_valid_len() +  node_large.get_valid_len() == former_length);
        assert(*(entry_key_t*)node_small.get_key_ptr(0) < *(entry_key_t*)node_large.get_key_ptr(0));

        // b+tree
        sibling->hdr.sibling_ptr = hdr.sibling_ptr;
        sibling->hdr.pred_ptr = this;
        if (sibling->hdr.sibling_ptr != nullptr)
            sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
        hdr.sibling_ptr = sibling;

        auto ret = (ptr->key() < sibling->get_first_key()) ? this : sibling;
        ret->insert_key_leaf(ptr, pred);
        assert(sibling->node1.get_valid_len() +  node1.get_valid_len() == former_length + 1);

        // Set a new root or insert the split key to the parent
        if(bt->root == this) { // only one node can update the root ptr
            auto new_root = new page_internal<T>(this, sibling->get_first_key(), sibling, 1);
            bt->setNewRoot(new_root);
        }
        else {
            if(with_lock) {
                lock.unlock();
            }
            bt->btree_insert_internal(sibling->get_first_key(), sibling, 1);
        }
        return ret;
    }

    void *search_pred(entry_key_t key, Blindi::list_node_t<T> **pred, bool debug=false) {
        *pred = nullptr;
        if (count() == 0)
        {
            return nullptr;
        }
        if (key < get_first_key())
        {
            if (hdr.pred_ptr)
            {
                *pred = hdr.pred_ptr->get_ptr(hdr.pred_ptr->count() - 1);
            }
            return nullptr;
        }
        if(hdr.sibling_ptr && key >= hdr.sibling_ptr->get_first_key())
        {
            *pred = get_ptr(count() - 1);
            return hdr.sibling_ptr;
        }
        bool hit, smaller_than_node = 0;
        int position = 0;
        uint16_t tree_traverse_len;
        uint16_t tree_traverse[cardinality] = {0};
        position = node1.SearchBlindiNode(key.data(), key.size(), SEARCH_TYPE::PREDECESSOR_SEARCH,  &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        if (hit)
        {
            if (position == 0)
            {
                *pred = hdr.pred_ptr ? hdr.pred_ptr->get_ptr(hdr.pred_ptr->count() - 1) : nullptr;
            }
            else
            {
                *pred = get_ptr(position - 1);
            }
            return get_ptr(position);
        }
        *pred = get_ptr(position);
        return nullptr;
    }

    void *search_exact(entry_key_t key, bool debug=false) {
        bool hit, smaller_than_node = 0;
        int position = 0;
        uint16_t tree_traverse_len;
        uint16_t tree_traverse[cardinality] = {0};
        position = node1.SearchBlindiNode(key.data(), key.size(), SEARCH_TYPE::POINT_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        if (hit)
        {
            return get_ptr(position);
        }
        return nullptr;
    }

    // print a node
    void print() {
        printf("[%d] leaf %x \n", 0, this);

        for(int i=0; i < count(); ++i)
            printf("%lld,%x ", get_ptr(i)->key(), get_ptr(i));

        printf("\n%x ", (void*) hdr.sibling_ptr);
        printf("\n");
    }
#endif

    void printAll() override{
        printf("printing leaf node: ");
        print();
    }
};

#ifdef USE_PMDK
int file_exists(const uint8_t *filename) {
    struct stat buffer;
    return stat(filename, &buffer);
}

void openPmemobjPool() {
    printf("use pmdk!\n");
    uint8_t pathname[100] = "mount/pmem0/pool";
    int sds_write_value = 0;
    pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);
    if (file_exists(pathname) != 0) {
        printf("create new one.\n");
        if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree),
                                                            (uint64_t)400ULL * 1024ULL * 1024ULL * 1024ULL, 0666)) == nullptr) {
            perror("failed to create pool.\n");
            return;
        }
    } else {
        printf("open existing one.\n");
        if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(btree))) == nullptr) {
            perror("failed to open pool.\n");
            return;
        }
    }
}
#endif
/*
 * class btree
 */
template<typename T>
btree<T>::btree(){
#ifdef USE_PMDK
    openPmemobjPool();
#endif
    root = new page_leaf<T>();
    list_head = alloc<Blindi::list_node_t<T>>();
    // printf("list_head=%p\n", list_head);
    list_head->next = nullptr;
    height = 1;
}

template<typename T>
btree<T>::~btree() {
    auto current = list_head;
#ifndef NVRAM
    while (current != nullptr)
    {
        auto to_be_deleted = current;
        current = current->next;
        delete to_be_deleted;
    }
#endif
    for (auto ptr : all_pages())
    {
        delete ptr;
    }
#ifdef USE_PMDK
    pmemobj_close(pop);
#endif
}

template<typename T>
std::vector<page<T>*> btree<T>::all_pages()
{
    std::vector<page<T>*> result;
    auto current = root;
    while (current->is_internal()) {
        auto *sibling = dynamic_cast<page_internal<T>*>(current);
        current = sibling->hdr.leftmost_ptr;
        while(sibling) {
            result.push_back(sibling);
            sibling = sibling->hdr.sibling_ptr;
        }
    }
    auto leaf = dynamic_cast<page_leaf<T>*>(current);;
    while(leaf) {
        result.push_back(leaf);
        leaf = leaf->hdr.sibling_ptr;
    }
    return result;
}

template<typename T>
size_t btree<T>::getMemoryUsed()
{
    size_t memory = 0;
    for (auto ptr : all_pages())
    {
        memory += ptr->is_internal() ? sizeof(page_internal<T>) : sizeof(page_leaf<T>);
    }
    return memory;
}

template<typename T>
size_t btree<T>::getPersistentMemoryUsed()
{
    auto num_nodes = 0;
    auto current = list_head;
    while (current != nullptr)
    {
        num_nodes += 1;
        current = current->next;
    }
    return num_nodes * sizeof(Blindi::list_node_t<T>);
}

template<typename T>
void btree<T>::setNewRoot(page<T> *new_root) {
    this->root = new_root;
    ++height;
}

template<typename T>
T *btree<T>::search_exact(entry_key_t key){
    auto p = root;
    auto debug = false;

    while(p->is_internal()) {
        p = dynamic_cast<page_internal<T>*>(p)->linear_search_internal(key);
    }

    page_leaf<T> * cur = dynamic_cast<page_leaf<T>*>(p);
    void * t;
    while((t = cur->search_exact(key, debug)) == cur->hdr.sibling_ptr) {
        cur = reinterpret_cast<page_leaf<T>*>(t);
        if(!cur) {
            break;
        }
        assert(!cur->is_internal());
    }
    if (t)
    {
        return &(reinterpret_cast<Blindi::list_node_t<T> *>(t)->value);
    }
    return nullptr;
}

template<typename T>
Blindi::list_node_t<T> *btree<T>::btree_search_pred(entry_key_t key, bool & found, Blindi::list_node_t<T> **prev, bool debug){
    auto p = root;

    while(p->is_internal()) {
        p = dynamic_cast<page_internal<T>*>(p)->linear_search_internal(key);
    }

    page_leaf<T> * cur = dynamic_cast<page_leaf<T>*>(p);
    void * t;
    while((t = cur->search_pred(key, prev, debug)) == cur->hdr.sibling_ptr) {
        cur = reinterpret_cast<page_leaf<T>*>(t);
        if(!cur) {
            break;
        }
        assert(!cur->is_internal());
    }

    if(!t) {
        //printf("NOT FOUND %lu, t = %p\n", key, t);
        found = false;
        return nullptr;
    }

    found = true;
    return reinterpret_cast<Blindi::list_node_t<T> *>(t);
}


template<typename T>
T *btree<T>::search(entry_key_t key) {
    bool f = false;
    Blindi::list_node_t<T> *prev;
    auto ptr = btree_search_pred(key, f, &prev);
    if (f) {
        if (&(ptr->value) != nullptr) {
            return &(ptr->value);
        }
    } else {
        ;//printf("not found.\n");
    }
    return nullptr;
}

// insert the key in the leaf node
template<typename T>
void btree<T>::btree_insert_pred(Blindi::list_node_t<T>* insert, Blindi::list_node_t<T> **pred, bool & update){ //need to be string
    auto p = root;

    while(p->is_internal()) {
        p = dynamic_cast<page_internal<T>*>(p)->linear_search_internal(insert->key());
    }
    auto target = dynamic_cast<page_leaf<T>*>(p);
    *pred = nullptr;
    update = !target->store_leaf(this, insert, true, pred);
}

template<typename T>
T* btree<T>::insert(entry_key_t key, T value) {
    auto n = alloc<Blindi::list_node_t<T>>();
    // printf("n=%p\n", n);
    n->next = nullptr;
    n->value = value;
    n->key_t = key;
    n->isUpdate = false;
    n->isDelete = false;
    Blindi::list_node_t<T> *prev = nullptr;
    bool update;
    bool rt = false;
    // std::cout << "btree_insert_pred " << std::endl;
    btree_insert_pred(n, &prev, update);
    if (update && prev != nullptr) {
        // Overwrite.
        prev->value = value;
        //flush.
        clflush((uint8_t *)prev, sizeof(Blindi::list_node_t<T>));
    }
    else {
        int retry_number = 0, w=0;
retry:
    retry_number += 1;
    if (retry_number > 10 && w == 3) {
        return nullptr;
        }
        if (rt) {
            // we need to re-search the key!
            bool f;
            btree_search_pred(key, f, &prev);
            if (!f) {
                return nullptr;
                printf("error!!!!\n");
                exit(17);
            }
        }
        rt = true;
        // Insert a new key.
        if (list_head->next != nullptr) {

            if (prev == nullptr) {
                // Insert a smallest one.
                prev = list_head;
            }
            if (prev->isUpdate){
                w = 1;
                goto retry;
            }

            // check the order and CAS.
            Blindi::list_node_t<T> *next = prev->next;
            n->next = next;
            clflush((uint8_t *)n, sizeof(Blindi::list_node_t<T>));
            if (prev->key() < key && (next == nullptr || next->key() > key)) {
                if (!__sync_bool_compare_and_swap(&(prev->next), next, n)){
                    w = 2;
                    goto retry;
                }

                clflush((uint8_t *)prev, sizeof(Blindi::list_node_t<T>));
            } else {
                // View changed, retry.
                w = 3;
                goto retry;
            }
        } else {
            // This is the first insert!
            if (!__sync_bool_compare_and_swap(&(list_head->next), nullptr, n))
                goto retry;
        }
    }
    // std::cout << "btree_insert_pred_finish " << std::endl;
    return &(n->value);
}


template<typename T>
void btree<T>::remove(entry_key_t key) {
    bool f, debug=false;
    Blindi::list_node_t<T> *cur = nullptr, *prev = nullptr;
retry:
    cur = btree_search_pred(key, f, &prev, debug);
    if (!f) {
        printf("not found.\n");
        return;
    }
    if (prev == nullptr) {
        prev = list_head;
    }
    if (prev->next != cur) {
        if (debug){
            printf("prev list node:\n");
            prev->printAll();
            printf("current list node:\n");
            cur->printAll();
        }
        exit(18);
        goto retry;
    } else {
        // Delete it.
        if (!__sync_bool_compare_and_swap(&(prev->next), cur, cur->next))
            goto retry;
        clflush((uint8_t *)prev, sizeof(Blindi::list_node_t<T>));
        // Uncomment to prevent memory leak (maybe not threadsafe)
        // delete prev;
        btree_delete(key);
    }

}

// store the key into the node at the given level
template<typename T>
void btree<T>::btree_insert_internal(entry_key_t key, page<T> *right, uint32_t level) {
    assert(root->is_internal());

    auto p = dynamic_cast<page_internal<T>*>(root);
    assert(p->hdr.level >= level);

    while(p->hdr.level > level) {
        p = dynamic_cast<page_internal<T>*>(p->linear_search_internal(key));
    }

    if(!p->store_internal(this, key, right, true, true)) {
        btree_insert_internal(key, right, level);
    }
}

template<typename T>
void btree<T>::btree_delete(entry_key_t key) {
    auto p = root;

    while(p->is_internal()) {
        p = dynamic_cast<page_internal<T>*>(p)->linear_search_internal(key);
    }

    auto cur = dynamic_cast<page_leaf<T>*>(p);
    void * t;
    Blindi::list_node_t<T> *pred = nullptr;
    while((t = cur->search_pred(key, &pred, false)) == cur->hdr.sibling_ptr) {
        cur = reinterpret_cast<page_leaf<T>*>(t);
        if(!cur) {
            break;
        }
        assert(!cur->is_internal());
    }

    if(cur) {
        if(!cur->remove_leaf(key)) {
            btree_delete(key);
        }
    }
    else {
        printf("not found the key to delete %lu\n", key);
    }
}

template<typename T>
void btree<T>::printAll(){
    pthread_mutex_lock(&print_mtx);
    int total_keys = 0;
    for (auto ptr : all_pages())
    {
        if(!ptr->is_internal()) {
            total_keys += dynamic_cast<page_leaf<T>*>(ptr)->count();
            dynamic_cast<page_leaf<T>*>(ptr)->print();
        }
        else
        {
            ptr->print();
        }
    }
    printf("total number of keys: %d\n", total_keys);
    pthread_mutex_unlock(&print_mtx);
}

template<typename T>
void btree<T>::printAllKeys(){
    auto cur = list_head->next;
    while (cur)
    {
        std::cout << cur->key() << std::endl;
        cur = cur->next;
    }
}

template<typename T>
void btree<T>::printAllKeysByPage(){
    for (auto ptr : all_pages())
    {
        if (ptr->is_internal()) continue;
        auto leaf = dynamic_cast<page_leaf<T>*>(ptr);
        for (auto i = 0; i < leaf->size(); ++i)
        {
            auto key = leaf->get_ptr(i)->key();
            std::cout << key << std::endl;
        }
        std::cout << "----------- next leaf -------------" << std::endl;
    }
}

template <typename T>
std::vector<T> btree<T>::scan(entry_key_t key, size_t size)
{
    std::vector<T> result;
    bool f = false;
    Blindi::list_node_t<T> *prev;
    auto ptr = btree_search_pred(key, f, &prev);
    if (!f) {
        return {};
    }
    while (ptr != nullptr && result.size() < size)
    {
        result.push_back(ptr->value);
        ptr = ptr->next;
    }
    return result;
}

template <typename T>
std::vector<typename btree<T>::U> btree<T>::secondaryScan(entry_key_t key, size_t size)
{
    std::vector<U> result;
    bool f = false;
    Blindi::list_node_t<T> *prev;
    auto ptr = btree_search_pred(key, f, &prev);
    if (!f) {
        return {};
    }
    while (ptr != nullptr && result.size() < size)
    {
        result.push_back(*(ptr->value));
        ptr = ptr->next;
    }
    return result;
}
