#ifndef _BLINDI_SEQTREE_H_
#define _BLINDI_SEQTREE_H_

// C++ program for BlindI node
#include<iostream>
#include <cassert>
#include <type_traits>
#include "bit_manipulation.hpp"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define TREE_LEVELS 0


#ifndef _BLINDI_PARM_H_
#define _BLINDI_PARM_H_

enum SEARCH_TYPE {PREDECESSOR_SEARCH = 1, POINT_SEARCH = 0};
enum INSERT_RESULT {INSERT_SUCCESS = 0, INSERT_DUPLICATED_KEY = 1, INSERT_OVERFLOW = 2};
enum REMOVE_RESULT {REMOVE_SUCCESS = 0, REMOVE_NODE_EMPTY = 1, REMOVE_UNDERFLOW = 2, REMOVE_KEY_NOT_FOUND = 3};

#endif // _BLINDI_PARM_H_

//#define BREATHING_BLINDI_SIZE 4
//#define BREATHING_BLINDI_DATA_ONLY

//#define BREATHING_BLINDI_STSTS
#undef BREATHING_BLINDI_STSTS
#ifndef _BREATHING_BLINDI_STSTS_DEFINE_
#define _BREATHING_BLINDI_STSTS_DEFINE_
//static uint64_t breathing_sum = 0;
//static uint64_t breathing_count = 0;
//static uint64_t insert_count = 0;
#endif

#define END_TREE ((idx_t)-1)
#define DUPLICATE ((bitidx_t)-2)
#define MAX_HIGH_MISS ((bitidx_t)-3)
//#define DEBUG_AGG
// #define DEBUG_SEQTREE
#ifdef DEBUG_AGG
//uint64_t Agg_first_stage_tree = 0;
//uint64_t Agg_first_stage_seq = 0;
//uint64_t Agg_number = 0;
#define AGG_TH NUM_SLOTS
#endif

// The Key valid_length is contsant and up to 255 bits
//#define FINGERPRINT
//#define KEY_VAR_LEN
//#define ASSERT

using namespace std;

//int first_stage_len = 0;
static int CompareStage (const uint8_t *key1, const uint8_t *key2, bool *eq, bool *big_small, uint16_t key_len_key1, uint16_t key_len_key2);

// A BlindI node
template <typename _Key, int NUM_SLOTS>
class SeqTreeBlindiNode {

#define TREE_LEVEL_HYBRID 0
#define START_TREE_LEVELS 64 
#if TREE_LEVEL_HYBRID > 0 // till START_TREE_LEVELS LVL=0 elase LVL=2
#define TREE_LEVELS ((NUM_SLOTS + START_TREE_LEVELS)/START_TREE_LEVELS/2)*2L
#define TREE_SIZE 3 
#else // all the levels are the same
// #define TREE_LEVELS 2
#define TREE_SIZE (1 << (TREE_LEVELS)) - 1
#endif

	static_assert((NUM_SLOTS % 2) == 0, "NUM_SLOTS is not even");

	typedef typename std::conditional<sizeof(_Key) <= 31,
		uint8_t, uint16_t>::type bitidx_t;
	typedef typename std::conditional<NUM_SLOTS < 254,
		uint8_t, uint16_t>::type idx_t;

	idx_t valid_len;// the current number of blindiKeys in the blindiNode
	idx_t blindiTree[TREE_SIZE];  // level 1 [0] level 2 [1-2] level 3 [3-7] and so on...

#ifdef BREATHING_BLINDI_SIZE
	uint16_t currentmaxslot;
	uint8_t **key_ptr;// An array of pointers


#ifdef BREATHING_BLINDI_DATA_ONLY
	bitidx_t blindiKeys[NUM_SLOTS - 1];// An array of BlindIKeys
#else
	bitidx_t *blindiKeys;// An array of BlindIKeys
#endif    
#else
	bitidx_t blindiKeys[NUM_SLOTS - 1];// An array of BlindIKeys
	uint8_t *key_ptr[NUM_SLOTS]; // An array of pointers
#endif


#ifdef KEY_VAR_LEN
	uint16_t key_len[NUM_SLOTS]; // the length of the key
#endif
#ifdef FINGERPRINT
	uint8_t fingerprint[NUM_SLOTS]; // a hash to make sure this is the right key
#endif

	/* tree_traverse[0] = start_bk_seq_pos - for the seq search in the SearchSecondStage
	   tree_traverse[1] = end_bk_seq_pos - for the seq search in the SearchSecondStage
	   tree_traverse[2] = the last tree traverse
	   tree_traverse[3] = one previous form the laset tree_traverse
	   tree_traverse[4] = is the insert is also in the tree
	   tree_traverse_len = the len of the traverse
	 */
	public:
	SeqTreeBlindiNode() {
		valid_len = 0;
	}   // Constructor

	uint16_t get_valid_len() const {
		return this->valid_len;
	}

	uint8_t *get_key_ptr(int pos) const {
		return this->key_ptr[pos];
	}


	uint16_t get_blindiKey(int pos) const {
		return this->blindiKeys[pos];
	}

	uint8_t **get_ptr2key_ptr(){
		return this->key_ptr;
	}

	bitidx_t *get_ptr2blindiKeys(){
		return this->blindiKeys;
	}

	uint8_t *get_ptr2blindiTree(){
		if (TREE_LEVELS > 0) {
			return (uint8_t*)this->blindiTree;
		} else {
			return NULL;
		}
	}


	idx_t get_blindiTree(int pos) const {
		return this->blindiTree[pos];
	}	

#ifdef BREATHING_BLINDI_SIZE
	inline bool isneedbreath() const
	{
		return (this->valid_len == this->currentmaxslot);
	}
#endif

	void first_insert(uint8_t *insert_key, uint16_t key_len = 0, uint8_t fingerprint = 0) {

#ifdef BREATHING_BLINDI_STSTS 
		breathing_sum += (uint16_t)NUM_SLOTS;
		breathing_count++;
#endif
#ifdef BREATHING_BLINDI_SIZE
		this->currentmaxslot = (uint16_t)NUM_SLOTS;
		this->key_ptr = (uint8_t **)malloc (NUM_SLOTS * sizeof(uint8_t *));  
#ifndef BREATHING_BLINDI_DATA_ONLY 
		this->blindiKeys = (bitidx_t *)malloc ((NUM_SLOTS - 1) * sizeof(bitidx_t));  
#endif 
#endif

		this->blindiKeys[0] = 0;
		this->key_ptr[0] = insert_key;
		if (TREE_LEVELS > 0) {
			for (int i = 0; i < TREE_SIZE; i++) {
				blindiTree[i] = END_TREE;
			}
		}
#ifdef KEY_VAR_LEN
		this->key_len[0] = key_len;
#endif
#ifdef FINGERPRINT
		this->fingerprint[0] = fingerprint;
#endif
		this->valid_len = 1;
	}


	void transfer_btree2blindi(uint16_t valid_len, const _Key *keys, uint8_t **keys_ptr,  uint16_t *key_len = NULL, uint8_t *fingerprint = NULL)
	{
		this->valid_len = valid_len; 
		bool _hit, _large_small;
		uint8_t *keys_i = (uint8_t *)&keys[0];
		uint8_t *keys_i_p1;

#ifdef BREATHING_BLINDI_SIZE
		uint16_t cms;
		if (valid_len + BREATHING_BLINDI_SIZE > NUM_SLOTS) 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum += (uint16_t)NUM_SLOTS;
#endif
			cms = (uint16_t)NUM_SLOTS;
		}
		else 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum += valid_len + (uint16_t)BREATHING_BLINDI_SIZE;
#endif
			cms =  valid_len + BREATHING_BLINDI_SIZE;
		}
		this->currentmaxslot = cms;
		this->key_ptr = (uint8_t **)malloc (cms * sizeof(uint8_t *));  
#ifndef BREATHING_BLINDI_DATA_ONLY 
		this->blindiKeys = (bitidx_t *)malloc ((cms - 1) * sizeof(bitidx_t));  
#endif 
#endif

		if (TREE_LEVELS > 0) {
			this->blindiTree[0] = 0;
                }
		for (int i=0; i < (valid_len - 1); i++) 
		{ 
			keys_i_p1 = (uint8_t *)&keys[i+1];
			this->blindiKeys[i] = CompareStage(keys_i, keys_i_p1, &_hit, &_large_small, sizeof(_Key), sizeof(_Key));
			if (TREE_LEVELS > 0) {
				this->blindiTree[0] = (this->blindiKeys[i] < this->blindiKeys[this->blindiTree[0]]) ? i : this->blindiTree[0];
			}
			this->key_ptr[i] =  keys_ptr[i];
			keys_i = keys_i_p1;
		}
		this->key_ptr[valid_len - 1] = keys_ptr[valid_len - 1];
		if (TREE_LEVELS > 1) {
			for (int l = 2; l <= TREE_LEVELS; l++) {
				for (int i = pow(2,(l -1)) - 1; i <= pow(2,l) - 2; i++) {
					{
						// if my parent does not END_TREE update the vlaue 
						if (blindiTree[((i+1)>>1) - 1] != END_TREE)
						{ 
							update_blindiTree_from_seq(this, i);
						}
						else // the parent is END_TREE so also I get END_TREE
						{
							this->blindiTree[i] = END_TREE;
						}		
					}
				}
			}
		}

#ifdef KEY_VAR_LEN
		std::copy(key_len[0], key_len + NUM_SLOTS, this->key_len[0]);
#endif
#ifdef FINGERPRINT
		std::copy(fingerprint[0], fingerprint + NUM_SLOTS, this->fingerprint[0]);
#endif

#ifdef DEBUG_SEQTREE
			std::cout << "transfer_btree2blindi " << std::endl;
			print_node();
#endif

	}




	void bring_edges_seq_from_pos(idx_t *blindiTree, int valid_len, int pos, int *start_pos, int *end_pos)
	{
		switch (pos) {
			case 0 :
				*start_pos = 0;
				*end_pos = valid_len - 2;
				break;
                if (TREE_LEVELS > 1) {
			case 1 :
				*start_pos = 0;
				*end_pos = blindiTree[0] - 1;
				break;
			case 2 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = valid_len - 2;
				break;
                }
//#if (TREE_LEVELS > 2)
#if (0)
			case 3 :
				*start_pos = 0;
				*end_pos = blindiTree[1] - 1;
				break;
			case 4 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 5 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 6 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = valid_len - 2;
				break;
			case 7 :
				*start_pos = 0;
				*end_pos = blindiTree[3] - 1;
				break;
			case 8 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 9 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 10 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 11 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 12 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 13 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 14 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = valid_len - 2;
				break;
//#endif
//#if (TREE_LEVELS > 3)
			case 15 :
				*start_pos = 0;
				*end_pos = blindiTree[7] - 1;
				break;
			case 16 :
				*start_pos = blindiTree[7] + 1;
				*end_pos = blindiTree[3] - 1;
				break;
			case 17 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[8] - 1;
				break;
			case 18 :
				*start_pos = blindiTree[8] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 19 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[9] - 1;
				break;
			case 20 :
				*start_pos = blindiTree[9] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 21 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[10] - 1;
				break;
			case 22 :
				*start_pos = blindiTree[10] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 23 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[11] - 1;
				break;
			case 24 :
				*start_pos = blindiTree[11] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 25 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[12] - 1;
				break;
			case 26 :
				*start_pos = blindiTree[12] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 27 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[13] - 1;
				break;
			case 28 :
				*start_pos = blindiTree[13] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 29 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = blindiTree[14] - 1;
				break;
			case 30 :
				*start_pos = blindiTree[14] + 1;
				*end_pos = valid_len - 2;
				break;
//#endif
//#if (TREE_LEVELS > 4)
			case 31 :
				*start_pos = 0;
				*end_pos = blindiTree[15] - 1;
				break;
			case 32 :
				*start_pos = blindiTree[15] + 1;
				*end_pos = blindiTree[7] - 1;
				break;
			case 33 :
				*start_pos = blindiTree[7] + 1;
				*end_pos = blindiTree[16] - 1;
				break;
			case 34 :
				*start_pos = blindiTree[16] + 1;
				*end_pos = blindiTree[3] - 1;
				break;
			case 35 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[17] - 1;
				break;
			case 36 :
				*start_pos = blindiTree[17] + 1;
				*end_pos = blindiTree[8] - 1;
				break;
			case 37 :
				*start_pos = blindiTree[8] + 1;
				*end_pos = blindiTree[18] - 1;
				break;
			case 38 :
				*start_pos = blindiTree[18] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 39 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[19] - 1;
				break;
			case 40 :
				*start_pos = blindiTree[19] + 1;
				*end_pos = blindiTree[9] - 1;
				break;
			case 41 :
				*start_pos = blindiTree[9] + 1;
				*end_pos = blindiTree[20] - 1;
				break;
			case 42 :
				*start_pos = blindiTree[20] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 43 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[21] - 1;
				break;
			case 44 :
				*start_pos = blindiTree[21] + 1;
				*end_pos = blindiTree[10] - 1;
				break;
			case 45 :
				*start_pos = blindiTree[10] + 1;
				*end_pos = blindiTree[22] - 1;
				break;
			case 46 :
				*start_pos = blindiTree[22] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 47 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[23] - 1;
				break;
			case 48 :
				*start_pos = blindiTree[23] + 1;
				*end_pos = blindiTree[11] - 1;
				break;
			case 49 :
				*start_pos = blindiTree[11] + 1;
				*end_pos = blindiTree[24] - 1;
				break;
			case 50 :
				*start_pos = blindiTree[24] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 51 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[25] - 1;
				break;
			case 52 :
				*start_pos = blindiTree[25] + 1;
				*end_pos = blindiTree[12] - 1;
				break;
			case 53 :
				*start_pos = blindiTree[12] + 1;
				*end_pos = blindiTree[26] - 1;
				break;
			case 54 :
				*start_pos = blindiTree[26] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 55 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[27] - 1;
				break;
			case 56 :
				*start_pos = blindiTree[27] + 1;
				*end_pos = blindiTree[13] - 1;
				break;
			case 57 :
				*start_pos = blindiTree[13] + 1;
				*end_pos = blindiTree[28] - 1;
				break;
			case 58 :
				*start_pos = blindiTree[28] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 59 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = blindiTree[29] - 1;
				break;
			case 60 :
				*start_pos = blindiTree[29] + 1;
				*end_pos = blindiTree[14] - 1;
				break;
			case 61 :
				*start_pos = blindiTree[14] + 1;
				*end_pos = blindiTree[30] - 1;
				break;
			case 62 :
				*start_pos = blindiTree[30] + 1;
				*end_pos = valid_len - 2;
				break;
//#endif
//#if (TREE_LEVELS > 5)
			case 63 :
				*start_pos = 0;
				*end_pos = blindiTree[31] - 1;
				break;
			case 64 :
				*start_pos = blindiTree[31] + 1;
				*end_pos = blindiTree[15] - 1;
				break;
			case 65 :
				*start_pos = blindiTree[15] + 1;
				*end_pos = blindiTree[32] - 1;
				break;
			case 66 :
				*start_pos = blindiTree[32] + 1;
				*end_pos = blindiTree[7] - 1;
				break;
			case 67 :
				*start_pos = blindiTree[7] + 1;
				*end_pos = blindiTree[33] - 1;
				break;
			case 68 :
				*start_pos = blindiTree[33] + 1;
				*end_pos = blindiTree[16] - 1;
				break;
			case 69 :
				*start_pos = blindiTree[16] + 1;
				*end_pos = blindiTree[34] - 1;
				break;
			case 70 :
				*start_pos = blindiTree[34] + 1;
				*end_pos = blindiTree[3] - 1;
				break;
			case 71 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[35] - 1;
				break;
			case 72 :
				*start_pos = blindiTree[35] + 1;
				*end_pos = blindiTree[17] - 1;
				break;
			case 73 :
				*start_pos = blindiTree[17] + 1;
				*end_pos = blindiTree[36] - 1;
				break;
			case 74 :
				*start_pos = blindiTree[36] + 1;
				*end_pos = blindiTree[8] - 1;
				break;
			case 75 :
				*start_pos = blindiTree[8] + 1;
				*end_pos = blindiTree[37] - 1;
				break;
			case 76 :
				*start_pos = blindiTree[37] + 1;
				*end_pos = blindiTree[18] - 1;
				break;
			case 77 :
				*start_pos = blindiTree[18] + 1;
				*end_pos = blindiTree[38] - 1;
				break;
			case 78 :
				*start_pos = blindiTree[38] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 79 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[39] - 1;
				break;
			case 80 :
				*start_pos = blindiTree[39] + 1;
				*end_pos = blindiTree[19] - 1;
				break;
			case 81 :
				*start_pos = blindiTree[19] + 1;
				*end_pos = blindiTree[40] - 1;
				break;
			case 82 :
				*start_pos = blindiTree[40] + 1;
				*end_pos = blindiTree[9] - 1;
				break;
			case 83 :
				*start_pos = blindiTree[9] + 1;
				*end_pos = blindiTree[41] - 1;
				break;
			case 84 :
				*start_pos = blindiTree[41] + 1;
				*end_pos = blindiTree[20] - 1;
				break;
			case 85 :
				*start_pos = blindiTree[20] + 1;
				*end_pos = blindiTree[42] - 1;
				break;
			case 86 :
				*start_pos = blindiTree[42] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 87 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[43] - 1;
				break;
			case 88 :
				*start_pos = blindiTree[43] + 1;
				*end_pos = blindiTree[21] - 1;
				break;
			case 89 :
				*start_pos = blindiTree[21] + 1;
				*end_pos = blindiTree[44] - 1;
				break;
			case 90 :
				*start_pos = blindiTree[44] + 1;
				*end_pos = blindiTree[10] - 1;
				break;
			case 91 :
				*start_pos = blindiTree[10] + 1;
				*end_pos = blindiTree[45] - 1;
				break;
			case 92 :
				*start_pos = blindiTree[45] + 1;
				*end_pos = blindiTree[22] - 1;
				break;
			case 93 :
				*start_pos = blindiTree[22] + 1;
				*end_pos = blindiTree[46] - 1;
				break;
			case 94 :
				*start_pos = blindiTree[46] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 95 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[47] - 1;
				break;
			case 96 :
				*start_pos = blindiTree[47] + 1;
				*end_pos = blindiTree[23] - 1;
				break;
			case 97 :
				*start_pos = blindiTree[23] + 1;
				*end_pos = blindiTree[48] - 1;
				break;
			case 98 :
				*start_pos = blindiTree[48] + 1;
				*end_pos = blindiTree[11] - 1;
				break;
			case 99 :
				*start_pos = blindiTree[11] + 1;
				*end_pos = blindiTree[49] - 1;
				break;
			case 100 :
				*start_pos = blindiTree[49] + 1;
				*end_pos = blindiTree[24] - 1;
				break;
			case 101 :
				*start_pos = blindiTree[24] + 1;
				*end_pos = blindiTree[50] - 1;
				break;
			case 102 :
				*start_pos = blindiTree[50] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 103 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[51] - 1;
				break;
			case 104 :
				*start_pos = blindiTree[51] + 1;
				*end_pos = blindiTree[25] - 1;
				break;
			case 105 :
				*start_pos = blindiTree[25] + 1;
				*end_pos = blindiTree[52] - 1;
				break;
			case 106 :
				*start_pos = blindiTree[52] + 1;
				*end_pos = blindiTree[12] - 1;
				break;
			case 107 :
				*start_pos = blindiTree[12] + 1;
				*end_pos = blindiTree[53] - 1;
				break;
			case 108 :
				*start_pos = blindiTree[53] + 1;
				*end_pos = blindiTree[26] - 1;
				break;
			case 109 :
				*start_pos = blindiTree[26] + 1;
				*end_pos = blindiTree[54] - 1;
				break;
			case 110 :
				*start_pos = blindiTree[54] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 111 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[55] - 1;
				break;
			case 112 :
				*start_pos = blindiTree[55] + 1;
				*end_pos = blindiTree[27] - 1;
				break;
			case 113 :
				*start_pos = blindiTree[27] + 1;
				*end_pos = blindiTree[56] - 1;
				break;
			case 114 :
				*start_pos = blindiTree[56] + 1;
				*end_pos = blindiTree[13] - 1;
				break;
			case 115 :
				*start_pos = blindiTree[13] + 1;
				*end_pos = blindiTree[57] - 1;
				break;
			case 116 :
				*start_pos = blindiTree[57] + 1;
				*end_pos = blindiTree[28] - 1;
				break;
			case 117 :
				*start_pos = blindiTree[28] + 1;
				*end_pos = blindiTree[58] - 1;
				break;
			case 118 :
				*start_pos = blindiTree[58] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 119 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = blindiTree[59] - 1;
				break;
			case 120 :
				*start_pos = blindiTree[59] + 1;
				*end_pos = blindiTree[29] - 1;
				break;
			case 121 :
				*start_pos = blindiTree[29] + 1;
				*end_pos = blindiTree[60] - 1;
				break;
			case 122 :
				*start_pos = blindiTree[60] + 1;
				*end_pos = blindiTree[14] - 1;
				break;
			case 123 :
				*start_pos = blindiTree[14] + 1;
				*end_pos = blindiTree[61] - 1;
				break;
			case 124 :
				*start_pos = blindiTree[61] + 1;
				*end_pos = blindiTree[30] - 1;
				break;
			case 125 :
				*start_pos = blindiTree[30] + 1;
				*end_pos = blindiTree[62] - 1;
				break;
			case 126 :
				*start_pos = blindiTree[62] + 1;
				*end_pos = valid_len - 2;
				break;
//#endif
//#if (TREE_LEVELS > 6)
			case 127 :
				*start_pos = 0;
				*end_pos = blindiTree[63] - 1;
				break;
			case 128 :
				*start_pos = blindiTree[63] + 1;
				*end_pos = blindiTree[31] - 1;
				break;
			case 129 :
				*start_pos = blindiTree[31] + 1;
				*end_pos = blindiTree[64] - 1;
				break;
			case 130 :
				*start_pos = blindiTree[64] + 1;
				*end_pos = blindiTree[15] - 1;
				break;
			case 131 :
				*start_pos = blindiTree[15] + 1;
				*end_pos = blindiTree[65] - 1;
				break;
			case 132 :
				*start_pos = blindiTree[65] + 1;
				*end_pos = blindiTree[32] - 1;
				break;
			case 133 :
				*start_pos = blindiTree[32] + 1;
				*end_pos = blindiTree[66] - 1;
				break;
			case 134 :
				*start_pos = blindiTree[66] + 1;
				*end_pos = blindiTree[7] - 1;
				break;
			case 135 :
				*start_pos = blindiTree[7] + 1;
				*end_pos = blindiTree[67] - 1;
				break;
			case 136 :
				*start_pos = blindiTree[67] + 1;
				*end_pos = blindiTree[33] - 1;
				break;
			case 137 :
				*start_pos = blindiTree[33] + 1;
				*end_pos = blindiTree[68] - 1;
				break;
			case 138 :
				*start_pos = blindiTree[68] + 1;
				*end_pos = blindiTree[16] - 1;
				break;
			case 139 :
				*start_pos = blindiTree[16] + 1;
				*end_pos = blindiTree[69] - 1;
				break;
			case 140 :
				*start_pos = blindiTree[69] + 1;
				*end_pos = blindiTree[34] - 1;
				break;
			case 141 :
				*start_pos = blindiTree[34] + 1;
				*end_pos = blindiTree[70] - 1;
				break;
			case 142 :
				*start_pos = blindiTree[70] + 1;
				*end_pos = blindiTree[3] - 1;
				break;
			case 143 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[71] - 1;
				break;
			case 144 :
				*start_pos = blindiTree[71] + 1;
				*end_pos = blindiTree[35] - 1;
				break;
			case 145 :
				*start_pos = blindiTree[35] + 1;
				*end_pos = blindiTree[72] - 1;
				break;
			case 146 :
				*start_pos = blindiTree[72] + 1;
				*end_pos = blindiTree[17] - 1;
				break;
			case 147 :
				*start_pos = blindiTree[17] + 1;
				*end_pos = blindiTree[73] - 1;
				break;
			case 148 :
				*start_pos = blindiTree[73] + 1;
				*end_pos = blindiTree[36] - 1;
				break;
			case 149 :
				*start_pos = blindiTree[36] + 1;
				*end_pos = blindiTree[74] - 1;
				break;
			case 150 :
				*start_pos = blindiTree[74] + 1;
				*end_pos = blindiTree[8] - 1;
				break;
			case 151 :
				*start_pos = blindiTree[8] + 1;
				*end_pos = blindiTree[75] - 1;
				break;
			case 152 :
				*start_pos = blindiTree[75] + 1;
				*end_pos = blindiTree[37] - 1;
				break;
			case 153 :
				*start_pos = blindiTree[37] + 1;
				*end_pos = blindiTree[76] - 1;
				break;
			case 154 :
				*start_pos = blindiTree[76] + 1;
				*end_pos = blindiTree[18] - 1;
				break;
			case 155 :
				*start_pos = blindiTree[18] + 1;
				*end_pos = blindiTree[77] - 1;
				break;
			case 156 :
				*start_pos = blindiTree[77] + 1;
				*end_pos = blindiTree[38] - 1;
				break;
			case 157 :
				*start_pos = blindiTree[38] + 1;
				*end_pos = blindiTree[78] - 1;
				break;
			case 158 :
				*start_pos = blindiTree[78] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 159 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[79] - 1;
				break;
			case 160 :
				*start_pos = blindiTree[79] + 1;
				*end_pos = blindiTree[39] - 1;
				break;
			case 161 :
				*start_pos = blindiTree[39] + 1;
				*end_pos = blindiTree[80] - 1;
				break;
			case 162 :
				*start_pos = blindiTree[80] + 1;
				*end_pos = blindiTree[19] - 1;
				break;
			case 163 :
				*start_pos = blindiTree[19] + 1;
				*end_pos = blindiTree[81] - 1;
				break;
			case 164 :
				*start_pos = blindiTree[81] + 1;
				*end_pos = blindiTree[40] - 1;
				break;
			case 165 :
				*start_pos = blindiTree[40] + 1;
				*end_pos = blindiTree[82] - 1;
				break;
			case 166 :
				*start_pos = blindiTree[82] + 1;
				*end_pos = blindiTree[9] - 1;
				break;
			case 167 :
				*start_pos = blindiTree[9] + 1;
				*end_pos = blindiTree[83] - 1;
				break;
			case 168 :
				*start_pos = blindiTree[83] + 1;
				*end_pos = blindiTree[41] - 1;
				break;
			case 169 :
				*start_pos = blindiTree[41] + 1;
				*end_pos = blindiTree[84] - 1;
				break;
			case 170 :
				*start_pos = blindiTree[84] + 1;
				*end_pos = blindiTree[20] - 1;
				break;
			case 171 :
				*start_pos = blindiTree[20] + 1;
				*end_pos = blindiTree[85] - 1;
				break;
			case 172 :
				*start_pos = blindiTree[85] + 1;
				*end_pos = blindiTree[42] - 1;
				break;
			case 173 :
				*start_pos = blindiTree[42] + 1;
				*end_pos = blindiTree[86] - 1;
				break;
			case 174 :
				*start_pos = blindiTree[86] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 175 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[87] - 1;
				break;
			case 176 :
				*start_pos = blindiTree[87] + 1;
				*end_pos = blindiTree[43] - 1;
				break;
			case 177 :
				*start_pos = blindiTree[43] + 1;
				*end_pos = blindiTree[88] - 1;
				break;
			case 178 :
				*start_pos = blindiTree[88] + 1;
				*end_pos = blindiTree[21] - 1;
				break;
			case 179 :
				*start_pos = blindiTree[21] + 1;
				*end_pos = blindiTree[89] - 1;
				break;
			case 180 :
				*start_pos = blindiTree[89] + 1;
				*end_pos = blindiTree[44] - 1;
				break;
			case 181 :
				*start_pos = blindiTree[44] + 1;
				*end_pos = blindiTree[90] - 1;
				break;
			case 182 :
				*start_pos = blindiTree[90] + 1;
				*end_pos = blindiTree[10] - 1;
				break;
			case 183 :
				*start_pos = blindiTree[10] + 1;
				*end_pos = blindiTree[91] - 1;
				break;
			case 184 :
				*start_pos = blindiTree[91] + 1;
				*end_pos = blindiTree[45] - 1;
				break;
			case 185 :
				*start_pos = blindiTree[45] + 1;
				*end_pos = blindiTree[92] - 1;
				break;
			case 186 :
				*start_pos = blindiTree[92] + 1;
				*end_pos = blindiTree[22] - 1;
				break;
			case 187 :
				*start_pos = blindiTree[22] + 1;
				*end_pos = blindiTree[93] - 1;
				break;
			case 188 :
				*start_pos = blindiTree[93] + 1;
				*end_pos = blindiTree[46] - 1;
				break;
			case 189 :
				*start_pos = blindiTree[46] + 1;
				*end_pos = blindiTree[94] - 1;
				break;
			case 190 :
				*start_pos = blindiTree[94] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 191 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[95] - 1;
				break;
			case 192 :
				*start_pos = blindiTree[95] + 1;
				*end_pos = blindiTree[47] - 1;
				break;
			case 193 :
				*start_pos = blindiTree[47] + 1;
				*end_pos = blindiTree[96] - 1;
				break;
			case 194 :
				*start_pos = blindiTree[96] + 1;
				*end_pos = blindiTree[23] - 1;
				break;
			case 195 :
				*start_pos = blindiTree[23] + 1;
				*end_pos = blindiTree[97] - 1;
				break;
			case 196 :
				*start_pos = blindiTree[97] + 1;
				*end_pos = blindiTree[48] - 1;
				break;
			case 197 :
				*start_pos = blindiTree[48] + 1;
				*end_pos = blindiTree[98] - 1;
				break;
			case 198 :
				*start_pos = blindiTree[98] + 1;
				*end_pos = blindiTree[11] - 1;
				break;
			case 199 :
				*start_pos = blindiTree[11] + 1;
				*end_pos = blindiTree[99] - 1;
				break;
			case 200 :
				*start_pos = blindiTree[99] + 1;
				*end_pos = blindiTree[49] - 1;
				break;
			case 201 :
				*start_pos = blindiTree[49] + 1;
				*end_pos = blindiTree[100] - 1;
				break;
			case 202 :
				*start_pos = blindiTree[100] + 1;
				*end_pos = blindiTree[24] - 1;
				break;
			case 203 :
				*start_pos = blindiTree[24] + 1;
				*end_pos = blindiTree[101] - 1;
				break;
			case 204 :
				*start_pos = blindiTree[101] + 1;
				*end_pos = blindiTree[50] - 1;
				break;
			case 205 :
				*start_pos = blindiTree[50] + 1;
				*end_pos = blindiTree[102] - 1;
				break;
			case 206 :
				*start_pos = blindiTree[102] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 207 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[103] - 1;
				break;
			case 208 :
				*start_pos = blindiTree[103] + 1;
				*end_pos = blindiTree[51] - 1;
				break;
			case 209 :
				*start_pos = blindiTree[51] + 1;
				*end_pos = blindiTree[104] - 1;
				break;
			case 210 :
				*start_pos = blindiTree[104] + 1;
				*end_pos = blindiTree[25] - 1;
				break;
			case 211 :
				*start_pos = blindiTree[25] + 1;
				*end_pos = blindiTree[105] - 1;
				break;
			case 212 :
				*start_pos = blindiTree[105] + 1;
				*end_pos = blindiTree[52] - 1;
				break;
			case 213 :
				*start_pos = blindiTree[52] + 1;
				*end_pos = blindiTree[106] - 1;
				break;
			case 214 :
				*start_pos = blindiTree[106] + 1;
				*end_pos = blindiTree[12] - 1;
				break;
			case 215 :
				*start_pos = blindiTree[12] + 1;
				*end_pos = blindiTree[107] - 1;
				break;
			case 216 :
				*start_pos = blindiTree[107] + 1;
				*end_pos = blindiTree[53] - 1;
				break;
			case 217 :
				*start_pos = blindiTree[53] + 1;
				*end_pos = blindiTree[108] - 1;
				break;
			case 218 :
				*start_pos = blindiTree[108] + 1;
				*end_pos = blindiTree[26] - 1;
				break;
			case 219 :
				*start_pos = blindiTree[26] + 1;
				*end_pos = blindiTree[109] - 1;
				break;
			case 220 :
				*start_pos = blindiTree[109] + 1;
				*end_pos = blindiTree[54] - 1;
				break;
			case 221 :
				*start_pos = blindiTree[54] + 1;
				*end_pos = blindiTree[110] - 1;
				break;
			case 222 :
				*start_pos = blindiTree[110] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 223 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[111] - 1;
				break;
			case 224 :
				*start_pos = blindiTree[111] + 1;
				*end_pos = blindiTree[55] - 1;
				break;
			case 225 :
				*start_pos = blindiTree[55] + 1;
				*end_pos = blindiTree[112] - 1;
				break;
			case 226 :
				*start_pos = blindiTree[112] + 1;
				*end_pos = blindiTree[27] - 1;
				break;
			case 227 :
				*start_pos = blindiTree[27] + 1;
				*end_pos = blindiTree[113] - 1;
				break;
			case 228 :
				*start_pos = blindiTree[113] + 1;
				*end_pos = blindiTree[56] - 1;
				break;
			case 229 :
				*start_pos = blindiTree[56] + 1;
				*end_pos = blindiTree[114] - 1;
				break;
			case 230 :
				*start_pos = blindiTree[114] + 1;
				*end_pos = blindiTree[13] - 1;
				break;
			case 231 :
				*start_pos = blindiTree[13] + 1;
				*end_pos = blindiTree[115] - 1;
				break;
			case 232 :
				*start_pos = blindiTree[115] + 1;
				*end_pos = blindiTree[57] - 1;
				break;
			case 233 :
				*start_pos = blindiTree[57] + 1;
				*end_pos = blindiTree[116] - 1;
				break;
			case 234 :
				*start_pos = blindiTree[116] + 1;
				*end_pos = blindiTree[28] - 1;
				break;
			case 235 :
				*start_pos = blindiTree[28] + 1;
				*end_pos = blindiTree[117] - 1;
				break;
			case 236 :
				*start_pos = blindiTree[117] + 1;
				*end_pos = blindiTree[58] - 1;
				break;
			case 237 :
				*start_pos = blindiTree[58] + 1;
				*end_pos = blindiTree[118] - 1;
				break;
			case 238 :
				*start_pos = blindiTree[118] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 239 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = blindiTree[119] - 1;
				break;
			case 240 :
				*start_pos = blindiTree[119] + 1;
				*end_pos = blindiTree[59] - 1;
				break;
			case 241 :
				*start_pos = blindiTree[59] + 1;
				*end_pos = blindiTree[120] - 1;
				break;
			case 242 :
				*start_pos = blindiTree[120] + 1;
				*end_pos = blindiTree[29] - 1;
				break;
			case 243 :
				*start_pos = blindiTree[29] + 1;
				*end_pos = blindiTree[121] - 1;
				break;
			case 244 :
				*start_pos = blindiTree[121] + 1;
				*end_pos = blindiTree[60] - 1;
				break;
			case 245 :
				*start_pos = blindiTree[60] + 1;
				*end_pos = blindiTree[122] - 1;
				break;
			case 246 :
				*start_pos = blindiTree[122] + 1;
				*end_pos = blindiTree[14] - 1;
				break;
			case 247 :
				*start_pos = blindiTree[14] + 1;
				*end_pos = blindiTree[123] - 1;
				break;
			case 248 :
				*start_pos = blindiTree[123] + 1;
				*end_pos = blindiTree[61] - 1;
				break;
			case 249 :
				*start_pos = blindiTree[61] + 1;
				*end_pos = blindiTree[124] - 1;
				break;
			case 250 :
				*start_pos = blindiTree[124] + 1;
				*end_pos = blindiTree[30] - 1;
				break;
			case 251 :
				*start_pos = blindiTree[30] + 1;
				*end_pos = blindiTree[125] - 1;
				break;
			case 252 :
				*start_pos = blindiTree[125] + 1;
				*end_pos = blindiTree[62] - 1;
				break;
			case 253 :
				*start_pos = blindiTree[62] + 1;
				*end_pos = blindiTree[126] - 1;
				break;
			case 254 :
				*start_pos = blindiTree[126] + 1;
				*end_pos = valid_len - 2;
				break;
//#endif
//#if (TREE_LEVELS > 7)
			case 255 :
				*start_pos = 0;
				*end_pos = blindiTree[127] - 1;
				break;
			case 256 :
				*start_pos = blindiTree[127] + 1;
				*end_pos = blindiTree[63] - 1;
				break;
			case 257 :
				*start_pos = blindiTree[63] + 1;
				*end_pos = blindiTree[128] - 1;
				break;
			case 258 :
				*start_pos = blindiTree[128] + 1;
				*end_pos = blindiTree[31] - 1;
				break;
			case 259 :
				*start_pos = blindiTree[31] + 1;
				*end_pos = blindiTree[129] - 1;
				break;
			case 260 :
				*start_pos = blindiTree[129] + 1;
				*end_pos = blindiTree[64] - 1;
				break;
			case 261 :
				*start_pos = blindiTree[64] + 1;
				*end_pos = blindiTree[130] - 1;
				break;
			case 262 :
				*start_pos = blindiTree[130] + 1;
				*end_pos = blindiTree[15] - 1;
				break;
			case 263 :
				*start_pos = blindiTree[15] + 1;
				*end_pos = blindiTree[131] - 1;
				break;
			case 264 :
				*start_pos = blindiTree[131] + 1;
				*end_pos = blindiTree[65] - 1;
				break;
			case 265 :
				*start_pos = blindiTree[65] + 1;
				*end_pos = blindiTree[132] - 1;
				break;
			case 266 :
				*start_pos = blindiTree[132] + 1;
				*end_pos = blindiTree[32] - 1;
				break;
			case 267 :
				*start_pos = blindiTree[32] + 1;
				*end_pos = blindiTree[133] - 1;
				break;
			case 268 :
				*start_pos = blindiTree[133] + 1;
				*end_pos = blindiTree[66] - 1;
				break;
			case 269 :
				*start_pos = blindiTree[66] + 1;
				*end_pos = blindiTree[134] - 1;
				break;
			case 270 :
				*start_pos = blindiTree[134] + 1;
				*end_pos = blindiTree[7] - 1;
				break;
			case 271 :
				*start_pos = blindiTree[7] + 1;
				*end_pos = blindiTree[135] - 1;
				break;
			case 272 :
				*start_pos = blindiTree[135] + 1;
				*end_pos = blindiTree[67] - 1;
				break;
			case 273 :
				*start_pos = blindiTree[67] + 1;
				*end_pos = blindiTree[136] - 1;
				break;
			case 274 :
				*start_pos = blindiTree[136] + 1;
				*end_pos = blindiTree[33] - 1;
				break;
			case 275 :
				*start_pos = blindiTree[33] + 1;
				*end_pos = blindiTree[137] - 1;
				break;
			case 276 :
				*start_pos = blindiTree[137] + 1;
				*end_pos = blindiTree[68] - 1;
				break;
			case 277 :
				*start_pos = blindiTree[68] + 1;
				*end_pos = blindiTree[138] - 1;
				break;
			case 278 :
				*start_pos = blindiTree[138] + 1;
				*end_pos = blindiTree[16] - 1;
				break;
			case 279 :
				*start_pos = blindiTree[16] + 1;
				*end_pos = blindiTree[139] - 1;
				break;
			case 280 :
				*start_pos = blindiTree[139] + 1;
				*end_pos = blindiTree[69] - 1;
				break;
			case 281 :
				*start_pos = blindiTree[69] + 1;
				*end_pos = blindiTree[140] - 1;
				break;
			case 282 :
				*start_pos = blindiTree[140] + 1;
				*end_pos = blindiTree[34] - 1;
				break;
			case 283 :
				*start_pos = blindiTree[34] + 1;
				*end_pos = blindiTree[141] - 1;
				break;
			case 284 :
				*start_pos = blindiTree[141] + 1;
				*end_pos = blindiTree[70] - 1;
				break;
			case 285 :
				*start_pos = blindiTree[70] + 1;
				*end_pos = blindiTree[142] - 1;
				break;
			case 286 :
				*start_pos = blindiTree[142] + 1;
				*end_pos = blindiTree[3] - 1;
				break;
			case 287 :
				*start_pos = blindiTree[3] + 1;
				*end_pos = blindiTree[143] - 1;
				break;
			case 288 :
				*start_pos = blindiTree[143] + 1;
				*end_pos = blindiTree[71] - 1;
				break;
			case 289 :
				*start_pos = blindiTree[71] + 1;
				*end_pos = blindiTree[144] - 1;
				break;
			case 290 :
				*start_pos = blindiTree[144] + 1;
				*end_pos = blindiTree[35] - 1;
				break;
			case 291 :
				*start_pos = blindiTree[35] + 1;
				*end_pos = blindiTree[145] - 1;
				break;
			case 292 :
				*start_pos = blindiTree[145] + 1;
				*end_pos = blindiTree[72] - 1;
				break;
			case 293 :
				*start_pos = blindiTree[72] + 1;
				*end_pos = blindiTree[146] - 1;
				break;
			case 294 :
				*start_pos = blindiTree[146] + 1;
				*end_pos = blindiTree[17] - 1;
				break;
			case 295 :
				*start_pos = blindiTree[17] + 1;
				*end_pos = blindiTree[147] - 1;
				break;
			case 296 :
				*start_pos = blindiTree[147] + 1;
				*end_pos = blindiTree[73] - 1;
				break;
			case 297 :
				*start_pos = blindiTree[73] + 1;
				*end_pos = blindiTree[148] - 1;
				break;
			case 298 :
				*start_pos = blindiTree[148] + 1;
				*end_pos = blindiTree[36] - 1;
				break;
			case 299 :
				*start_pos = blindiTree[36] + 1;
				*end_pos = blindiTree[149] - 1;
				break;
			case 300 :
				*start_pos = blindiTree[149] + 1;
				*end_pos = blindiTree[74] - 1;
				break;
			case 301 :
				*start_pos = blindiTree[74] + 1;
				*end_pos = blindiTree[150] - 1;
				break;
			case 302 :
				*start_pos = blindiTree[150] + 1;
				*end_pos = blindiTree[8] - 1;
				break;
			case 303 :
				*start_pos = blindiTree[8] + 1;
				*end_pos = blindiTree[151] - 1;
				break;
			case 304 :
				*start_pos = blindiTree[151] + 1;
				*end_pos = blindiTree[75] - 1;
				break;
			case 305 :
				*start_pos = blindiTree[75] + 1;
				*end_pos = blindiTree[152] - 1;
				break;
			case 306 :
				*start_pos = blindiTree[152] + 1;
				*end_pos = blindiTree[37] - 1;
				break;
			case 307 :
				*start_pos = blindiTree[37] + 1;
				*end_pos = blindiTree[153] - 1;
				break;
			case 308 :
				*start_pos = blindiTree[153] + 1;
				*end_pos = blindiTree[76] - 1;
				break;
			case 309 :
				*start_pos = blindiTree[76] + 1;
				*end_pos = blindiTree[154] - 1;
				break;
			case 310 :
				*start_pos = blindiTree[154] + 1;
				*end_pos = blindiTree[18] - 1;
				break;
			case 311 :
				*start_pos = blindiTree[18] + 1;
				*end_pos = blindiTree[155] - 1;
				break;
			case 312 :
				*start_pos = blindiTree[155] + 1;
				*end_pos = blindiTree[77] - 1;
				break;
			case 313 :
				*start_pos = blindiTree[77] + 1;
				*end_pos = blindiTree[156] - 1;
				break;
			case 314 :
				*start_pos = blindiTree[156] + 1;
				*end_pos = blindiTree[38] - 1;
				break;
			case 315 :
				*start_pos = blindiTree[38] + 1;
				*end_pos = blindiTree[157] - 1;
				break;
			case 316 :
				*start_pos = blindiTree[157] + 1;
				*end_pos = blindiTree[78] - 1;
				break;
			case 317 :
				*start_pos = blindiTree[78] + 1;
				*end_pos = blindiTree[158] - 1;
				break;
			case 318 :
				*start_pos = blindiTree[158] + 1;
				*end_pos = blindiTree[1] - 1;
				break;
			case 319 :
				*start_pos = blindiTree[1] + 1;
				*end_pos = blindiTree[159] - 1;
				break;
			case 320 :
				*start_pos = blindiTree[159] + 1;
				*end_pos = blindiTree[79] - 1;
				break;
			case 321 :
				*start_pos = blindiTree[79] + 1;
				*end_pos = blindiTree[160] - 1;
				break;
			case 322 :
				*start_pos = blindiTree[160] + 1;
				*end_pos = blindiTree[39] - 1;
				break;
			case 323 :
				*start_pos = blindiTree[39] + 1;
				*end_pos = blindiTree[161] - 1;
				break;
			case 324 :
				*start_pos = blindiTree[161] + 1;
				*end_pos = blindiTree[80] - 1;
				break;
			case 325 :
				*start_pos = blindiTree[80] + 1;
				*end_pos = blindiTree[162] - 1;
				break;
			case 326 :
				*start_pos = blindiTree[162] + 1;
				*end_pos = blindiTree[19] - 1;
				break;
			case 327 :
				*start_pos = blindiTree[19] + 1;
				*end_pos = blindiTree[163] - 1;
				break;
			case 328 :
				*start_pos = blindiTree[163] + 1;
				*end_pos = blindiTree[81] - 1;
				break;
			case 329 :
				*start_pos = blindiTree[81] + 1;
				*end_pos = blindiTree[164] - 1;
				break;
			case 330 :
				*start_pos = blindiTree[164] + 1;
				*end_pos = blindiTree[40] - 1;
				break;
			case 331 :
				*start_pos = blindiTree[40] + 1;
				*end_pos = blindiTree[165] - 1;
				break;
			case 332 :
				*start_pos = blindiTree[165] + 1;
				*end_pos = blindiTree[82] - 1;
				break;
			case 333 :
				*start_pos = blindiTree[82] + 1;
				*end_pos = blindiTree[166] - 1;
				break;
			case 334 :
				*start_pos = blindiTree[166] + 1;
				*end_pos = blindiTree[9] - 1;
				break;
			case 335 :
				*start_pos = blindiTree[9] + 1;
				*end_pos = blindiTree[167] - 1;
				break;
			case 336 :
				*start_pos = blindiTree[167] + 1;
				*end_pos = blindiTree[83] - 1;
				break;
			case 337 :
				*start_pos = blindiTree[83] + 1;
				*end_pos = blindiTree[168] - 1;
				break;
			case 338 :
				*start_pos = blindiTree[168] + 1;
				*end_pos = blindiTree[41] - 1;
				break;
			case 339 :
				*start_pos = blindiTree[41] + 1;
				*end_pos = blindiTree[169] - 1;
				break;
			case 340 :
				*start_pos = blindiTree[169] + 1;
				*end_pos = blindiTree[84] - 1;
				break;
			case 341 :
				*start_pos = blindiTree[84] + 1;
				*end_pos = blindiTree[170] - 1;
				break;
			case 342 :
				*start_pos = blindiTree[170] + 1;
				*end_pos = blindiTree[20] - 1;
				break;
			case 343 :
				*start_pos = blindiTree[20] + 1;
				*end_pos = blindiTree[171] - 1;
				break;
			case 344 :
				*start_pos = blindiTree[171] + 1;
				*end_pos = blindiTree[85] - 1;
				break;
			case 345 :
				*start_pos = blindiTree[85] + 1;
				*end_pos = blindiTree[172] - 1;
				break;
			case 346 :
				*start_pos = blindiTree[172] + 1;
				*end_pos = blindiTree[42] - 1;
				break;
			case 347 :
				*start_pos = blindiTree[42] + 1;
				*end_pos = blindiTree[173] - 1;
				break;
			case 348 :
				*start_pos = blindiTree[173] + 1;
				*end_pos = blindiTree[86] - 1;
				break;
			case 349 :
				*start_pos = blindiTree[86] + 1;
				*end_pos = blindiTree[174] - 1;
				break;
			case 350 :
				*start_pos = blindiTree[174] + 1;
				*end_pos = blindiTree[4] - 1;
				break;
			case 351 :
				*start_pos = blindiTree[4] + 1;
				*end_pos = blindiTree[175] - 1;
				break;
			case 352 :
				*start_pos = blindiTree[175] + 1;
				*end_pos = blindiTree[87] - 1;
				break;
			case 353 :
				*start_pos = blindiTree[87] + 1;
				*end_pos = blindiTree[176] - 1;
				break;
			case 354 :
				*start_pos = blindiTree[176] + 1;
				*end_pos = blindiTree[43] - 1;
				break;
			case 355 :
				*start_pos = blindiTree[43] + 1;
				*end_pos = blindiTree[177] - 1;
				break;
			case 356 :
				*start_pos = blindiTree[177] + 1;
				*end_pos = blindiTree[88] - 1;
				break;
			case 357 :
				*start_pos = blindiTree[88] + 1;
				*end_pos = blindiTree[178] - 1;
				break;
			case 358 :
				*start_pos = blindiTree[178] + 1;
				*end_pos = blindiTree[21] - 1;
				break;
			case 359 :
				*start_pos = blindiTree[21] + 1;
				*end_pos = blindiTree[179] - 1;
				break;
			case 360 :
				*start_pos = blindiTree[179] + 1;
				*end_pos = blindiTree[89] - 1;
				break;
			case 361 :
				*start_pos = blindiTree[89] + 1;
				*end_pos = blindiTree[180] - 1;
				break;
			case 362 :
				*start_pos = blindiTree[180] + 1;
				*end_pos = blindiTree[44] - 1;
				break;
			case 363 :
				*start_pos = blindiTree[44] + 1;
				*end_pos = blindiTree[181] - 1;
				break;
			case 364 :
				*start_pos = blindiTree[181] + 1;
				*end_pos = blindiTree[90] - 1;
				break;
			case 365 :
				*start_pos = blindiTree[90] + 1;
				*end_pos = blindiTree[182] - 1;
				break;
			case 366 :
				*start_pos = blindiTree[182] + 1;
				*end_pos = blindiTree[10] - 1;
				break;
			case 367 :
				*start_pos = blindiTree[10] + 1;
				*end_pos = blindiTree[183] - 1;
				break;
			case 368 :
				*start_pos = blindiTree[183] + 1;
				*end_pos = blindiTree[91] - 1;
				break;
			case 369 :
				*start_pos = blindiTree[91] + 1;
				*end_pos = blindiTree[184] - 1;
				break;
			case 370 :
				*start_pos = blindiTree[184] + 1;
				*end_pos = blindiTree[45] - 1;
				break;
			case 371 :
				*start_pos = blindiTree[45] + 1;
				*end_pos = blindiTree[185] - 1;
				break;
			case 372 :
				*start_pos = blindiTree[185] + 1;
				*end_pos = blindiTree[92] - 1;
				break;
			case 373 :
				*start_pos = blindiTree[92] + 1;
				*end_pos = blindiTree[186] - 1;
				break;
			case 374 :
				*start_pos = blindiTree[186] + 1;
				*end_pos = blindiTree[22] - 1;
				break;
			case 375 :
				*start_pos = blindiTree[22] + 1;
				*end_pos = blindiTree[187] - 1;
				break;
			case 376 :
				*start_pos = blindiTree[187] + 1;
				*end_pos = blindiTree[93] - 1;
				break;
			case 377 :
				*start_pos = blindiTree[93] + 1;
				*end_pos = blindiTree[188] - 1;
				break;
			case 378 :
				*start_pos = blindiTree[188] + 1;
				*end_pos = blindiTree[46] - 1;
				break;
			case 379 :
				*start_pos = blindiTree[46] + 1;
				*end_pos = blindiTree[189] - 1;
				break;
			case 380 :
				*start_pos = blindiTree[189] + 1;
				*end_pos = blindiTree[94] - 1;
				break;
			case 381 :
				*start_pos = blindiTree[94] + 1;
				*end_pos = blindiTree[190] - 1;
				break;
			case 382 :
				*start_pos = blindiTree[190] + 1;
				*end_pos = blindiTree[0] - 1;
				break;
			case 383 :
				*start_pos = blindiTree[0] + 1;
				*end_pos = blindiTree[191] - 1;
				break;
			case 384 :
				*start_pos = blindiTree[191] + 1;
				*end_pos = blindiTree[95] - 1;
				break;
			case 385 :
				*start_pos = blindiTree[95] + 1;
				*end_pos = blindiTree[192] - 1;
				break;
			case 386 :
				*start_pos = blindiTree[192] + 1;
				*end_pos = blindiTree[47] - 1;
				break;
			case 387 :
				*start_pos = blindiTree[47] + 1;
				*end_pos = blindiTree[193] - 1;
				break;
			case 388 :
				*start_pos = blindiTree[193] + 1;
				*end_pos = blindiTree[96] - 1;
				break;
			case 389 :
				*start_pos = blindiTree[96] + 1;
				*end_pos = blindiTree[194] - 1;
				break;
			case 390 :
				*start_pos = blindiTree[194] + 1;
				*end_pos = blindiTree[23] - 1;
				break;
			case 391 :
				*start_pos = blindiTree[23] + 1;
				*end_pos = blindiTree[195] - 1;
				break;
			case 392 :
				*start_pos = blindiTree[195] + 1;
				*end_pos = blindiTree[97] - 1;
				break;
			case 393 :
				*start_pos = blindiTree[97] + 1;
				*end_pos = blindiTree[196] - 1;
				break;
			case 394 :
				*start_pos = blindiTree[196] + 1;
				*end_pos = blindiTree[48] - 1;
				break;
			case 395 :
				*start_pos = blindiTree[48] + 1;
				*end_pos = blindiTree[197] - 1;
				break;
			case 396 :
				*start_pos = blindiTree[197] + 1;
				*end_pos = blindiTree[98] - 1;
				break;
			case 397 :
				*start_pos = blindiTree[98] + 1;
				*end_pos = blindiTree[198] - 1;
				break;
			case 398 :
				*start_pos = blindiTree[198] + 1;
				*end_pos = blindiTree[11] - 1;
				break;
			case 399 :
				*start_pos = blindiTree[11] + 1;
				*end_pos = blindiTree[199] - 1;
				break;
			case 400 :
				*start_pos = blindiTree[199] + 1;
				*end_pos = blindiTree[99] - 1;
				break;
			case 401 :
				*start_pos = blindiTree[99] + 1;
				*end_pos = blindiTree[200] - 1;
				break;
			case 402 :
				*start_pos = blindiTree[200] + 1;
				*end_pos = blindiTree[49] - 1;
				break;
			case 403 :
				*start_pos = blindiTree[49] + 1;
				*end_pos = blindiTree[201] - 1;
				break;
			case 404 :
				*start_pos = blindiTree[201] + 1;
				*end_pos = blindiTree[100] - 1;
				break;
			case 405 :
				*start_pos = blindiTree[100] + 1;
				*end_pos = blindiTree[202] - 1;
				break;
			case 406 :
				*start_pos = blindiTree[202] + 1;
				*end_pos = blindiTree[24] - 1;
				break;
			case 407 :
				*start_pos = blindiTree[24] + 1;
				*end_pos = blindiTree[203] - 1;
				break;
			case 408 :
				*start_pos = blindiTree[203] + 1;
				*end_pos = blindiTree[101] - 1;
				break;
			case 409 :
				*start_pos = blindiTree[101] + 1;
				*end_pos = blindiTree[204] - 1;
				break;
			case 410 :
				*start_pos = blindiTree[204] + 1;
				*end_pos = blindiTree[50] - 1;
				break;
			case 411 :
				*start_pos = blindiTree[50] + 1;
				*end_pos = blindiTree[205] - 1;
				break;
			case 412 :
				*start_pos = blindiTree[205] + 1;
				*end_pos = blindiTree[102] - 1;
				break;
			case 413 :
				*start_pos = blindiTree[102] + 1;
				*end_pos = blindiTree[206] - 1;
				break;
			case 414 :
				*start_pos = blindiTree[206] + 1;
				*end_pos = blindiTree[5] - 1;
				break;
			case 415 :
				*start_pos = blindiTree[5] + 1;
				*end_pos = blindiTree[207] - 1;
				break;
			case 416 :
				*start_pos = blindiTree[207] + 1;
				*end_pos = blindiTree[103] - 1;
				break;
			case 417 :
				*start_pos = blindiTree[103] + 1;
				*end_pos = blindiTree[208] - 1;
				break;
			case 418 :
				*start_pos = blindiTree[208] + 1;
				*end_pos = blindiTree[51] - 1;
				break;
			case 419 :
				*start_pos = blindiTree[51] + 1;
				*end_pos = blindiTree[209] - 1;
				break;
			case 420 :
				*start_pos = blindiTree[209] + 1;
				*end_pos = blindiTree[104] - 1;
				break;
			case 421 :
				*start_pos = blindiTree[104] + 1;
				*end_pos = blindiTree[210] - 1;
				break;
			case 422 :
				*start_pos = blindiTree[210] + 1;
				*end_pos = blindiTree[25] - 1;
				break;
			case 423 :
				*start_pos = blindiTree[25] + 1;
				*end_pos = blindiTree[211] - 1;
				break;
			case 424 :
				*start_pos = blindiTree[211] + 1;
				*end_pos = blindiTree[105] - 1;
				break;
			case 425 :
				*start_pos = blindiTree[105] + 1;
				*end_pos = blindiTree[212] - 1;
				break;
			case 426 :
				*start_pos = blindiTree[212] + 1;
				*end_pos = blindiTree[52] - 1;
				break;
			case 427 :
				*start_pos = blindiTree[52] + 1;
				*end_pos = blindiTree[213] - 1;
				break;
			case 428 :
				*start_pos = blindiTree[213] + 1;
				*end_pos = blindiTree[106] - 1;
				break;
			case 429 :
				*start_pos = blindiTree[106] + 1;
				*end_pos = blindiTree[214] - 1;
				break;
			case 430 :
				*start_pos = blindiTree[214] + 1;
				*end_pos = blindiTree[12] - 1;
				break;
			case 431 :
				*start_pos = blindiTree[12] + 1;
				*end_pos = blindiTree[215] - 1;
				break;
			case 432 :
				*start_pos = blindiTree[215] + 1;
				*end_pos = blindiTree[107] - 1;
				break;
			case 433 :
				*start_pos = blindiTree[107] + 1;
				*end_pos = blindiTree[216] - 1;
				break;
			case 434 :
				*start_pos = blindiTree[216] + 1;
				*end_pos = blindiTree[53] - 1;
				break;
			case 435 :
				*start_pos = blindiTree[53] + 1;
				*end_pos = blindiTree[217] - 1;
				break;
			case 436 :
				*start_pos = blindiTree[217] + 1;
				*end_pos = blindiTree[108] - 1;
				break;
			case 437 :
				*start_pos = blindiTree[108] + 1;
				*end_pos = blindiTree[218] - 1;
				break;
			case 438 :
				*start_pos = blindiTree[218] + 1;
				*end_pos = blindiTree[26] - 1;
				break;
			case 439 :
				*start_pos = blindiTree[26] + 1;
				*end_pos = blindiTree[219] - 1;
				break;
			case 440 :
				*start_pos = blindiTree[219] + 1;
				*end_pos = blindiTree[109] - 1;
				break;
			case 441 :
				*start_pos = blindiTree[109] + 1;
				*end_pos = blindiTree[220] - 1;
				break;
			case 442 :
				*start_pos = blindiTree[220] + 1;
				*end_pos = blindiTree[54] - 1;
				break;
			case 443 :
				*start_pos = blindiTree[54] + 1;
				*end_pos = blindiTree[221] - 1;
				break;
			case 444 :
				*start_pos = blindiTree[221] + 1;
				*end_pos = blindiTree[110] - 1;
				break;
			case 445 :
				*start_pos = blindiTree[110] + 1;
				*end_pos = blindiTree[222] - 1;
				break;
			case 446 :
				*start_pos = blindiTree[222] + 1;
				*end_pos = blindiTree[2] - 1;
				break;
			case 447 :
				*start_pos = blindiTree[2] + 1;
				*end_pos = blindiTree[223] - 1;
				break;
			case 448 :
				*start_pos = blindiTree[223] + 1;
				*end_pos = blindiTree[111] - 1;
				break;
			case 449 :
				*start_pos = blindiTree[111] + 1;
				*end_pos = blindiTree[224] - 1;
				break;
			case 450 :
				*start_pos = blindiTree[224] + 1;
				*end_pos = blindiTree[55] - 1;
				break;
			case 451 :
				*start_pos = blindiTree[55] + 1;
				*end_pos = blindiTree[225] - 1;
				break;
			case 452 :
				*start_pos = blindiTree[225] + 1;
				*end_pos = blindiTree[112] - 1;
				break;
			case 453 :
				*start_pos = blindiTree[112] + 1;
				*end_pos = blindiTree[226] - 1;
				break;
			case 454 :
				*start_pos = blindiTree[226] + 1;
				*end_pos = blindiTree[27] - 1;
				break;
			case 455 :
				*start_pos = blindiTree[27] + 1;
				*end_pos = blindiTree[227] - 1;
				break;
			case 456 :
				*start_pos = blindiTree[227] + 1;
				*end_pos = blindiTree[113] - 1;
				break;
			case 457 :
				*start_pos = blindiTree[113] + 1;
				*end_pos = blindiTree[228] - 1;
				break;
			case 458 :
				*start_pos = blindiTree[228] + 1;
				*end_pos = blindiTree[56] - 1;
				break;
			case 459 :
				*start_pos = blindiTree[56] + 1;
				*end_pos = blindiTree[229] - 1;
				break;
			case 460 :
				*start_pos = blindiTree[229] + 1;
				*end_pos = blindiTree[114] - 1;
				break;
			case 461 :
				*start_pos = blindiTree[114] + 1;
				*end_pos = blindiTree[230] - 1;
				break;
			case 462 :
				*start_pos = blindiTree[230] + 1;
				*end_pos = blindiTree[13] - 1;
				break;
			case 463 :
				*start_pos = blindiTree[13] + 1;
				*end_pos = blindiTree[231] - 1;
				break;
			case 464 :
				*start_pos = blindiTree[231] + 1;
				*end_pos = blindiTree[115] - 1;
				break;
			case 465 :
				*start_pos = blindiTree[115] + 1;
				*end_pos = blindiTree[232] - 1;
				break;
			case 466 :
				*start_pos = blindiTree[232] + 1;
				*end_pos = blindiTree[57] - 1;
				break;
			case 467 :
				*start_pos = blindiTree[57] + 1;
				*end_pos = blindiTree[233] - 1;
				break;
			case 468 :
				*start_pos = blindiTree[233] + 1;
				*end_pos = blindiTree[116] - 1;
				break;
			case 469 :
				*start_pos = blindiTree[116] + 1;
				*end_pos = blindiTree[234] - 1;
				break;
			case 470 :
				*start_pos = blindiTree[234] + 1;
				*end_pos = blindiTree[28] - 1;
				break;
			case 471 :
				*start_pos = blindiTree[28] + 1;
				*end_pos = blindiTree[235] - 1;
				break;
			case 472 :
				*start_pos = blindiTree[235] + 1;
				*end_pos = blindiTree[117] - 1;
				break;
			case 473 :
				*start_pos = blindiTree[117] + 1;
				*end_pos = blindiTree[236] - 1;
				break;
			case 474 :
				*start_pos = blindiTree[236] + 1;
				*end_pos = blindiTree[58] - 1;
				break;
			case 475 :
				*start_pos = blindiTree[58] + 1;
				*end_pos = blindiTree[237] - 1;
				break;
			case 476 :
				*start_pos = blindiTree[237] + 1;
				*end_pos = blindiTree[118] - 1;
				break;
			case 477 :
				*start_pos = blindiTree[118] + 1;
				*end_pos = blindiTree[238] - 1;
				break;
			case 478 :
				*start_pos = blindiTree[238] + 1;
				*end_pos = blindiTree[6] - 1;
				break;
			case 479 :
				*start_pos = blindiTree[6] + 1;
				*end_pos = blindiTree[239] - 1;
				break;
			case 480 :
				*start_pos = blindiTree[239] + 1;
				*end_pos = blindiTree[119] - 1;
				break;
			case 481 :
				*start_pos = blindiTree[119] + 1;
				*end_pos = blindiTree[240] - 1;
				break;
			case 482 :
				*start_pos = blindiTree[240] + 1;
				*end_pos = blindiTree[59] - 1;
				break;
			case 483 :
				*start_pos = blindiTree[59] + 1;
				*end_pos = blindiTree[241] - 1;
				break;
			case 484 :
				*start_pos = blindiTree[241] + 1;
				*end_pos = blindiTree[120] - 1;
				break;
			case 485 :
				*start_pos = blindiTree[120] + 1;
				*end_pos = blindiTree[242] - 1;
				break;
			case 486 :
				*start_pos = blindiTree[242] + 1;
				*end_pos = blindiTree[29] - 1;
				break;
			case 487 :
				*start_pos = blindiTree[29] + 1;
				*end_pos = blindiTree[243] - 1;
				break;
			case 488 :
				*start_pos = blindiTree[243] + 1;
				*end_pos = blindiTree[121] - 1;
				break;
			case 489 :
				*start_pos = blindiTree[121] + 1;
				*end_pos = blindiTree[244] - 1;
				break;
			case 490 :
				*start_pos = blindiTree[244] + 1;
				*end_pos = blindiTree[60] - 1;
				break;
			case 491 :
				*start_pos = blindiTree[60] + 1;
				*end_pos = blindiTree[245] - 1;
				break;
			case 492 :
				*start_pos = blindiTree[245] + 1;
				*end_pos = blindiTree[122] - 1;
				break;
			case 493 :
				*start_pos = blindiTree[122] + 1;
				*end_pos = blindiTree[246] - 1;
				break;
			case 494 :
				*start_pos = blindiTree[246] + 1;
				*end_pos = blindiTree[14] - 1;
				break;
			case 495 :
				*start_pos = blindiTree[14] + 1;
				*end_pos = blindiTree[247] - 1;
				break;
			case 496 :
				*start_pos = blindiTree[247] + 1;
				*end_pos = blindiTree[123] - 1;
				break;
			case 497 :
				*start_pos = blindiTree[123] + 1;
				*end_pos = blindiTree[248] - 1;
				break;
			case 498 :
				*start_pos = blindiTree[248] + 1;
				*end_pos = blindiTree[61] - 1;
				break;
			case 499 :
				*start_pos = blindiTree[61] + 1;
				*end_pos = blindiTree[249] - 1;
				break;
			case 500 :
				*start_pos = blindiTree[249] + 1;
				*end_pos = blindiTree[124] - 1;
				break;
			case 501 :
				*start_pos = blindiTree[124] + 1;
				*end_pos = blindiTree[250] - 1;
				break;
			case 502 :
				*start_pos = blindiTree[250] + 1;
				*end_pos = blindiTree[30] - 1;
				break;
			case 503 :
				*start_pos = blindiTree[30] + 1;
				*end_pos = blindiTree[251] - 1;
				break;
			case 504 :
				*start_pos = blindiTree[251] + 1;
				*end_pos = blindiTree[125] - 1;
				break;
			case 505 :
				*start_pos = blindiTree[125] + 1;
				*end_pos = blindiTree[252] - 1;
				break;
			case 506 :
				*start_pos = blindiTree[252] + 1;
				*end_pos = blindiTree[62] - 1;
				break;
			case 507 :
				*start_pos = blindiTree[62] + 1;
				*end_pos = blindiTree[253] - 1;
				break;
			case 508 :
				*start_pos = blindiTree[253] + 1;
				*end_pos = blindiTree[126] - 1;
				break;
			case 509 :
				*start_pos = blindiTree[126] + 1;
				*end_pos = valid_len - 2;
				break;
			case 510 :
				*start_pos = 0;
				*end_pos = valid_len - 2;
				break;
#endif
		}
	}

	// bring_edges_seq_from_pos

	public:
	void print_node () const{
		printf ("valid_len %d\n", valid_len);
		printf ("key_ptr ");
		for (int i = 0; i<= valid_len - 1; i++) {
			for (int j = sizeof(_Key) - 1; j>= 0; j--) {
				printf ("%d ", get_key_ptr(i)[j]);
			}
			printf ("  ");
		}
		printf ("\nblindiKey ");
		for (int i = 0; i<= valid_len - 2; i++) {
			printf( "%d ", get_blindiKey(i));
		}
                if (TREE_LEVELS > 0) {
			int l = 1;
			printf ("\nblindiTree\n ");
			for (int i = 0; i< TREE_SIZE; i++) {
				for (int t = TREE_LEVELS - l; t>0; t--) {
					printf ("  ");
				}
				printf("%d ", get_blindiTree(i));
				if (i+1 == pow(2,l) - 1) {
					l += 1;
					printf("\n");
				}
			}
		}
		printf ("\n ");
	}


	// check smaller_than_node - check if the item is small than the small branch

	void check_smaller_than_node (uint16_t *tree_traverse, uint16_t len, bool *smaller_than_node) const {
		while (len >= 2) {
			if (blindiTree[tree_traverse[len - 1]] > tree_traverse[len]) { // not from the left branch
				*smaller_than_node = false;
				return;
			}
			len = len - 1;
		}
		*smaller_than_node = true;
		return;
	}

	uint16_t bring_largest_in_subtree (uint16_t *tree_traverse, uint16_t tree_traverse_len) const {
		int temp;
		int top = tree_traverse[3];
		int bottom = tree_traverse[2];
		for (int i = tree_traverse_len; i > 1; i--) {
			if (!((bottom + 1) & 1)) { // check that we arrive from the large part
				return blindiTree[top];
			}
			bottom = top;
			temp = ((top + 1) >> 1);
			top = temp > 0 ? temp - 1 : 0;
		}
		return valid_len - 1;
	}


	uint16_t bring_smallest_in_subtree (uint16_t *tree_traverse, uint16_t tree_traverse_len, bool *smaller_than_node) const {
		int temp;
		int top = tree_traverse[3];
		int bottom = tree_traverse[2];
		for (int i = tree_traverse_len; i > 1; i--) {
			if ((bottom + 1) & 1) { // check that we arrive from the small part
				return blindiTree[top];
			}
			bottom = top;
			temp = (top + 1) >> 1;
			top = temp > 0 ? temp - 1 : 0;
		}
		*smaller_than_node = 1;
		return 0;
	}

#ifdef KEY_VAR_LEN
	uint16_t get_key_len (uint16_t pos) const {
		return this->key_len[pos]; // the length of the key
	}
#endif

	/* The size of the blinfikeys array is valid_lenght -1 while the size of teh value is valid_length
	   Example for value valid_length 6 keys
	   BlindiKeys (5 elements) 	  0             1              2  	     3         4
	   Values  (6 elements)        0          1            2               3            4          5
	 */
	// A function to search first stage on this key
	// return the position
	// Tree traverase[0] is start_bk_seq_pos
	// Tree traverase[1] is end_bk_seq_pos
	// Tree traverase[2] - root of the tree and so on..
	uint16_t SearchFirstStage (const uint8_t *key_ptr, uint16_t key_len,  uint16_t *tree_traverse, uint16_t *tree_traverse_len) {
		int first_stage_key_position = 0; // the position of the potential value
		int start_bk_seq_pos = 0;
		int end_bk_seq_pos = valid_len - 2;
		int high_miss = MAX_HIGH_MISS; // the high subtree that was a miss
                bool is_start_pos = 0, is_end_pos = 0;
		bool is_last_large = 0;
#ifdef DEBUG_AGG
		if (valid_len >= AGG_TH) {
			Agg_number += 1;
		}
#endif
                if (TREE_LEVELS > 0) {
			tree_traverse[0] = start_bk_seq_pos;
			tree_traverse[1] = end_bk_seq_pos;
			tree_traverse[2] = 0;
			tree_traverse[3] = 0;
			tree_traverse[4] = 0;
			tree_traverse[5] = 0;
			uint16_t tree_pos_p1 = 1;
			*tree_traverse_len = 0;
			if (valid_len == 1 || blindiTree[0] == END_TREE) {
				return 0;
			}
                 
			for (int i = 0; i < TREE_LEVELS; i++) {
				tree_traverse[3] = tree_traverse[2];
				tree_traverse[2] = tree_pos_p1 - 1;
				*tree_traverse_len += 1;
				if (blindiTree[tree_pos_p1 - 1] == END_TREE) {
					return blindiTree[tree_traverse[3]] + is_last_large;
				}
				int tree_key = blindiKeys[blindiTree[tree_pos_p1 - 1]]; // if blindiKeys[blindiTree[tree_pos_p1 - 1]] == END_TREE
#ifdef DEBUG_AGG
				if (valid_len >= AGG_TH) {
					Agg_first_stage_tree += 1;
				}
#endif
				if (isNthBitSetInString(key_ptr, tree_key, key_len)) {
					high_miss = MAX_HIGH_MISS;
					first_stage_key_position = blindiTree[tree_pos_p1 - 1] + 1;
					start_bk_seq_pos = blindiTree[tree_pos_p1 - 1];
					is_start_pos = 1;
					tree_pos_p1 = ((tree_pos_p1 + 1) << 1) - 1;
					is_last_large = 1;
				} else {
					end_bk_seq_pos = blindiTree[tree_pos_p1 - 1];
					is_end_pos = 1;
					tree_pos_p1 = (tree_pos_p1 << 1);
					is_last_large = 0;
				}
			}
                } // WITH_TREE	
		tree_traverse[0] = start_bk_seq_pos;
		tree_traverse[1] = end_bk_seq_pos;
		// seq search
		for (int i= start_bk_seq_pos + is_start_pos; i<= end_bk_seq_pos - is_end_pos; i++) {
			int idx = this->blindiKeys[i];
			tree_traverse[5] = true;
#ifdef DEBUG_AGG
			if (valid_len >= AGG_TH) {
				Agg_first_stage_seq += 1;
			}
#endif
			if (idx <= high_miss) { // this position is a potintial one
				if (isNthBitSetInString(key_ptr, idx, sizeof(_Key))) {// check if the bit is set
					high_miss = MAX_HIGH_MISS;
					first_stage_key_position = i+1;
				}  //Else the subtree is still does not relevant
				else { // this position is not relevent -to be relevent need to be higher than idx and isNthBitSetInString = 1
					high_miss = min(high_miss, idx);
				}
			}
		}
		return first_stage_key_position;
	}


	// bring pointer to the key
	uint8_t *bring_key(int pos) {
		return  key_ptr[pos]; // bring the key from the DB
	}
#ifdef FINGERPRINT
	// bring pointer to the key
	uint8_t bring_fingerprint(int pos) {
		return  fingerprint[pos]; // bring the key from the DB
	}
#endif



	// A function to search second stage on this key - SEQ search
	int SearchSecondStageSeq (int first_stage_pos, int diff_pos, bool large_small, bool *smaller_than_node, uint16_t* tree_traverse, uint16_t *tree_traverse_len, bool len_diff = 0) {
		if (large_small) { // going large
			for (int i=first_stage_pos; i<= tree_traverse[1] ;  i++) {
				if (this->blindiKeys[i] < diff_pos) {
					return i;
				}
			}
			return tree_traverse[1] + 1;
		}

		// Going left
		if (first_stage_pos == 0) { // The key is smaller than the smallest element in the node - this search is smaller than the entire node, mark as miss
			*smaller_than_node = true;
			return 0;
		}
		first_stage_pos -= 1;
		for (int i=first_stage_pos; i>= tree_traverse[0]; i--) {
			if (this->blindiKeys[i] < diff_pos) {
				return i;
			}
		}
		*smaller_than_node = true;	 // The key is smaller than the smallest element in the node
		return tree_traverse[0];
	}

	// Tree SearchSecondStage
	int SearchSecondStageTree (int diff_pos, bool large_small, bool is_pointer_area, bool *smaller_than_node, uint16_t* tree_traverse, uint16_t *tree_traverse_len, bool len_diff = 0) {
		int temp;
		int i;
		for (i = *tree_traverse_len; i > 0; i--) {
			auto blindiKey = blindiKeys[blindiTree[tree_traverse[3]]];
			if (*tree_traverse_len == 1) { // we are at the head - we handle it outside this function
				assert(0);
			}
			if (blindiKey <= diff_pos) {
				if (((tree_traverse[2] + 1) & 1)) { // check that we arrive from the small part
					if (large_small) {
						return bring_largest_in_subtree(tree_traverse, *tree_traverse_len);
					} else {
						return blindiTree[tree_traverse[3]];
					}
				} else { // wee arrive from the large path
					if (large_small) {
						return 	blindiTree[tree_traverse[3]];
					} else {
						return bring_smallest_in_subtree(tree_traverse, *tree_traverse_len, smaller_than_node);
					}
				}
			}
			tree_traverse[2] = tree_traverse[3];
			temp = ((tree_traverse[3] + 1) >> 1);
			tree_traverse[3] = temp > 0 ? temp - 1 : 0;
			*tree_traverse_len -= 1;
		}
		// arrive to the top
		assert(0);
		return 0;
	}//WITH_TREE 

	// A function to insert a BlindiKey in position in non-full BlindiNode
	// the position - is the position in the blindiKeys

	void Insert2BlindiNodeInPosition(uint8_t *insert_key, uint16_t key_len, int insert_position,  int *diff_pos, bool *large_small, bool *smaller_than_node, uint16_t *tree_traverse, uint16_t *tree_traverse_len, uint8_t fingerprint = 0) {
		int insert_blindikey_position = insert_position;

#ifdef BREATHING_BLINDI_SIZE
		if (this->isneedbreath()) { 

			if (currentmaxslot + (uint16_t)BREATHING_BLINDI_SIZE > (uint16_t)NUM_SLOTS) 
			{
#ifdef BREATHING_BLINDI_STSTS 
				breathing_sum += (uint16_t)NUM_SLOTS - currentmaxslot;
				breathing_count++;
#endif
				currentmaxslot = (uint16_t)NUM_SLOTS;
			}
			else 
			{
				currentmaxslot = currentmaxslot + (uint16_t)BREATHING_BLINDI_SIZE;
#ifdef BREATHING_BLINDI_STSTS 
				breathing_sum += (uint16_t)BREATHING_BLINDI_SIZE;
				breathing_count++;
#endif
			}
			uint8_t **tmp_key_ptr = (uint8_t **)malloc (currentmaxslot * sizeof(uint8_t *)); 
			std::copy(key_ptr, key_ptr + valid_len,
					tmp_key_ptr);
			free(key_ptr);
			key_ptr = tmp_key_ptr;

#ifndef BREATHING_BLINDI_DATA_ONLY
			bitidx_t *tmp_blindiKeys = (bitidx_t *)malloc ((currentmaxslot - 1) * sizeof(bitidx_t));
			std::copy(blindiKeys, blindiKeys + valid_len - 1,
					tmp_blindiKeys);
			free(blindiKeys);
			blindiKeys = tmp_blindiKeys;
#endif 
		} 
#endif

		// Shift the blindiKeys/ key_ptr
		if (*smaller_than_node || (insert_position != valid_len - 1 && (valid_len != 0))) { // shift sucesssor key (If we don't insert to the last item)
			//shift the array key_ptr, blindiKeys
			// - shift  Data_ptr from position + 1 till len -1
			//  shift in blindiKeys from posion +1 till len -2
			uint8_t *temp_key = this->key_ptr[valid_len -1];
			for (int i = valid_len - 2; i > insert_position; i--) {
				this->blindiKeys[i+1] = this->blindiKeys[i];
				this->key_ptr[i+1] = this->key_ptr[i];
			}
			this->key_ptr[valid_len] = temp_key;
			if (*smaller_than_node) { // if smaller than the first element, need to shift also position 0
				this->key_ptr[1] = this->key_ptr[0];
				if (this->valid_len > 1) { // if we have just one item we don't have blindiKey
					this->blindiKeys[1] = this->blindiKeys[0];
				}
			}
			if (*large_small) { // like Example 2A
				this->blindiKeys[insert_position + 1 - *smaller_than_node] = this->blindiKeys[insert_position];
                               if (TREE_LEVELS > 0) {
				       insert_blindikey_position = insert_position;
                               }
			} else { // Example 2B
				this->blindiKeys[insert_position + 1 - *smaller_than_node] = *diff_pos;
                                if (TREE_LEVELS > 0) {
					insert_blindikey_position = insert_position + 1 -*smaller_than_node;
				}
			}
		}



		// calc and insert the new predecesor blindKey (If we don't smaller_than_node)
		if (!*smaller_than_node && (valid_len != 0)) { // calc the blindIkey predecessor key
			if (*large_small) { // like Example 2A
				this->blindiKeys[insert_position] = *diff_pos;
			}
		}

		// insert the pointer to the key_ptr
		this->key_ptr[insert_position + 1 - *smaller_than_node] = insert_key;

		// update the len
		this->valid_len += 1;
		// TREE cacluation
		// TREE
		// Add 1 to elemnts in the tree (when the item moved)
		int levels = 1;
		int i =0;


		// go over levels update position in blindiTree
                if (TREE_LEVELS > 0) 
		{
			for (int l = levels; l <= TREE_LEVELS; l++) {
				// go over the elements in the level
				for (i = pow(2,l) - 1; i >= pow(2, (l - 1)); i--) {
					if (((blindiTree[i- 1] != END_TREE) && (blindiTree[i - 1] > insert_position)) || ((blindiTree[i -1] == insert_position) && *diff_pos == blindiKeys[blindiTree[i -1]])) {
						blindiTree[i - 1] += 1;
					} else {
						if (blindiTree[i - 1] < insert_position) {
							break;
						}
					}
				}
			}


			// insert the new item to the tree
			if (tree_traverse[4] == 1) {
				int temp[TREE_SIZE];
				if (TREE_LEVELS > 1) {
					memset(temp, 0, sizeof(temp));
					temp[0] = blindiTree[tree_traverse[2]];
				}
				blindiTree[tree_traverse[2]] = insert_blindikey_position;
				if (TREE_LEVELS > 1) {
					// push the sub tree that tree_traverse[3] is the head down
					int levels = *tree_traverse_len + 1;
					int cur_pos = 0;
					int save_pos = 0;
					int end_tree_pos = 0;
					if (levels <= TREE_LEVELS) {
						cur_pos = ((tree_traverse[2] + 1) << 1) - 1;
						save_pos = cur_pos;
						end_tree_pos = cur_pos;
						if (levels + 1 <= TREE_LEVELS) {
							temp[1] = blindiTree[cur_pos];
							temp[2] = blindiTree[cur_pos + 1];
						}
						if ( temp[0] < insert_blindikey_position) {
							blindiTree[cur_pos]  = temp[0];
							blindiTree[cur_pos + 1]  = END_TREE;
							end_tree_pos += 1;
						} else {
							blindiTree[cur_pos]  = END_TREE;
							blindiTree[cur_pos + 1]  = temp[0];
							cur_pos += 1;
						}
					}
					levels += 1;
					int i = 2;
					//		bool should_return = 1;
					for (int l = levels; l <= TREE_LEVELS; l++) {
						cur_pos = ((cur_pos + 1) << 1) - 1;
						save_pos = ((save_pos + 1) << 1) - 1;
						end_tree_pos = ((end_tree_pos + 1) << 1) - 1;
						// save to temp
						if (l < TREE_LEVELS) {
							int m = 0;
							int power = pow(2, i);
							for (int j = 0;  j <  power ; j++) {
								temp[j + power - 1] = blindiTree[save_pos + m];
								m += 1;
							}
						}
						int m = 0;
						if (i == 1) {
							blindiTree[cur_pos] = temp[1];
							blindiTree[cur_pos +1] = temp[2];

						}
						int power = pow(2, i - 1);
						// insert to tree
						for (int j = 0;  j <  power ; j++) {
							blindiTree[cur_pos + m] = temp[j + power - 1];
							blindiTree[end_tree_pos + m] = END_TREE;
							m += 1;
						}
						i+= 1;
					}
				}
			}
		} // WITH_TREE	  
	}
	//    			0
	//    	   1   				2
	//     3      4                      5       6
	//   7  8   9   10                11  12   13   14
	//  15  8   9   10               11  12   13   14
	//(1 << 1)  + 2
	//(1 << 1)  + 1
	// asistant function for Split

	// 1101
	// parent - (x + 1 >> 1) - 1

	void update_blindiTree_from_tree (SeqTreeBlindiNode *target_node, int pos, idx_t* point_orig_2blindiTree, int half_node_size = 0) {
		if (target_node->blindiTree[0] == END_TREE) {
			target_node->blindiTree[0] = this->blindiTree[pos] - half_node_size;
			point_orig_2blindiTree[0] = blindiTree[pos];
			return;
		}
		int cur_pos = (blindiTree[pos] < point_orig_2blindiTree[0]) ? 1 : 2;
		for (int l = 2; l <= TREE_LEVELS; l++) {
			if (target_node->blindiTree[cur_pos] == END_TREE) {
				target_node->blindiTree[cur_pos] = this->blindiTree[pos] - half_node_size;
				point_orig_2blindiTree[cur_pos] = blindiTree[pos];
				return;
			}
			cur_pos =  blindiTree[pos] < point_orig_2blindiTree[cur_pos] ? ((cur_pos  + 1 )<< 1) - 1 : ((cur_pos + 1) << 1);
		}
		assert(0);
	}


	int min_pos_value_in_range(SeqTreeBlindiNode *node, int start_pos, int end_pos) {
		int min_pos = END_TREE;
		int min_value = END_TREE;
		for (int i = start_pos; i <= end_pos; i++) {
			if (node->blindiKeys[i] < min_value) {
				min_value = node->blindiKeys[i];
				min_pos = i;
			}
		}
		return min_pos;
	}

	void update_blindiTree_from_seq (SeqTreeBlindiNode *node, int pos) {
		int start_pos;
		int end_pos;
		bring_edges_seq_from_pos((idx_t *)node->blindiTree, node->valid_len, pos, &start_pos, &end_pos);
		node->blindiTree[pos] = min_pos_value_in_range(node, start_pos, end_pos);
	}


	// split
	// assume spilt on a valid_len is an even number
	// SEQ part: create two new nodes - small and large
	//  1. (0 - NUM_SLOTS /2 -1) -> copy it to node_small
	//  2. (NUM_SLOTS/2 : NUM_SLOTS -1) -> copy it to node_large
	// TREE part : create two new trees

	void SplitBlindiNode(SeqTreeBlindiNode *node_small, SeqTreeBlindiNode *node_large) {
#ifdef BREATHING_BLINDI_SIZE
		uint16_t cms;
		if ((uint16_t)NUM_SLOTS / 2 + (uint16_t)BREATHING_BLINDI_SIZE > (uint16_t)NUM_SLOTS) 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum += (uint16_t)NUM_SLOTS;
#endif
			cms = (uint16_t)NUM_SLOTS;
		}
		else 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum += 2 * (uint16_t)BREATHING_BLINDI_SIZE;
#endif
			cms = (uint16_t)NUM_SLOTS / 2 + (uint16_t)BREATHING_BLINDI_SIZE;
		}
		node_small->currentmaxslot = cms;
		node_large->currentmaxslot = cms;
		node_small->key_ptr = (uint8_t **)malloc (cms * sizeof(uint8_t *));  
		node_large->key_ptr = (uint8_t **)malloc (cms * sizeof(uint8_t *));  
#ifndef BREATHING_BLINDI_DATA_ONLY 
		node_small->blindiKeys = (bitidx_t *)malloc ((cms - 1) * sizeof(bitidx_t));  
		node_large->blindiKeys = (bitidx_t *)malloc ((cms - 1) * sizeof(bitidx_t));  
#endif 
#endif

		// Update the new node and the orignal_node
		int half_node_size =  this->valid_len /2;
		for (int i=0; i <= half_node_size -2; i++) {
			node_small->key_ptr[i] = this->key_ptr[i];
			node_large->key_ptr[i] = this->key_ptr[half_node_size + i];
			node_small->blindiKeys[i] = this->blindiKeys[i];
			node_large->blindiKeys[i] = this->blindiKeys[half_node_size + i];
		}

		node_small->key_ptr[half_node_size - 1] = this->key_ptr[half_node_size - 1];
		node_large->key_ptr[half_node_size - 1] = this->key_ptr[NUM_SLOTS - 1];
		node_small->valid_len = half_node_size;
		node_large->valid_len = half_node_size;


		// Build blindiTree
		// go over the original blindiTree and  split the original blindiTree between the two trees
                if (TREE_LEVELS > 0) {
			// default the blindiTree
			for (int i = 0; i < TREE_SIZE; i++) {
				node_small->blindiTree[i] = END_TREE;
				node_large->blindiTree[i] = END_TREE;
			}

			// update blindiTree from tree
			idx_t node_small_point_orig_blindiTree[TREE_SIZE];
			idx_t node_large_point_orig_blindiTree[TREE_SIZE];
			if ((this->blindiTree[0] <= half_node_size - 2)) {
				update_blindiTree_from_tree(node_small, 0, node_small_point_orig_blindiTree);
			} else if (this->blindiTree[0] >= half_node_size && this->blindiTree[0] != END_TREE) {
				update_blindiTree_from_tree(node_large, 0, node_large_point_orig_blindiTree, half_node_size);
			}

			for (int l = 2; l <= TREE_LEVELS; l++) {
				for (int i = pow(2,(l -1)) - 1; i <= pow(2,l) - 2; i++) {
					if (this->blindiTree[((i + 1) >> 1) - 1] != END_TREE )  {
						if ((this->blindiTree[i] <= half_node_size - 2)) {
							update_blindiTree_from_tree(node_small ,i, node_small_point_orig_blindiTree);
						} else if (this->blindiTree[i] >= half_node_size && this->blindiTree[i] != END_TREE) {
							update_blindiTree_from_tree(node_large, i, node_large_point_orig_blindiTree, half_node_size);
						}
					}
				}
			}


			// update the rest of blindiTree from seq
			if (node_small->blindiTree[0] == END_TREE) {
				update_blindiTree_from_seq(node_small, 0);
			}
			if (node_large->blindiTree[0] == END_TREE) {
				update_blindiTree_from_seq(node_large, 0);
			}
			for (int l = 2; l <= TREE_LEVELS; l++) {
				for (int i = pow(2,(l -1)) - 1; i <= pow(2,l) - 2; i++) {
					if ((node_small->blindiTree[i] == END_TREE) && (node_small->blindiTree[((i + 1) >> 1) - 1] != END_TREE)) {
						update_blindiTree_from_seq(node_small, i);
					}
					if ((node_large->blindiTree[i] == END_TREE) && (node_large->blindiTree[((i + 1) >> 1) - 1] != END_TREE)) {
						update_blindiTree_from_seq(node_large, i);
					}
				}
			}
		}
#ifdef BREATHING_BLINDI_SIZE
		free(key_ptr);  
#ifndef BREATHING_BLINDI_DATA_ONLY 
		free(blindiKeys);  
#endif 
#endif
	}
	//  Remove shfit key_ptr and shift BlindiKeys, & valid_len -= 1
	int RemoveFromBlindiNodeInPosition(uint16_t remove_position, uint16_t *tree_traverse, uint16_t *tree_traverse_len) {
		// SEQ
		bool is_remove_the_last_item = 0;
		bool is_remove_the_first_item = 0;
		// if the position is the last one or the valid_len is not larger than 2- zeros the position
		if (remove_position == this->valid_len - 1 || this->valid_len <= 2) {
			if (this->valid_len > 2 || (this->valid_len == 2 && remove_position == 1)) { //
				this->key_ptr[remove_position] = 0;
				this->blindiKeys[remove_position-1] = 0;
				is_remove_the_last_item = 2;
			} else {
				if (this->valid_len == 2) { // valid_len == 2 and the position is 0
					this->blindiKeys[0] = 0;
					this->key_ptr[0] = this->key_ptr[1];
					this->key_ptr[1] = 0;
					this->valid_len = 1;
					return REMOVE_SUCCESS;
				}
				// if valid_len == 1
				this->valid_len = 0;
				return REMOVE_NODE_EMPTY;
			}
		}
		// if valid_len above 2 and not the last one
		if (is_remove_the_last_item == 0) {
			if (remove_position != 0) {
				this->blindiKeys[remove_position - 1] = min(this->blindiKeys[remove_position], this->blindiKeys[remove_position-1]);
			}
			for (int i =remove_position + 1; i <= this->valid_len-2; i++) {
				this->key_ptr[i - 1] = this->key_ptr[i];
				this->blindiKeys[i - 1] = this->blindiKeys[i];
			}
			this->key_ptr[valid_len - 2] = this->key_ptr[valid_len-1];
		}
		this->valid_len -= 1;

               if (TREE_LEVELS > 0) {
		       // TREE
		       if (remove_position == 0) {
			       is_remove_the_first_item = 1;
		       }
		       bool range_min_value_array [TREE_SIZE] = {};

		       int levels = 1;
		       if (is_remove_the_first_item && blindiTree[0] == 0) {
			       if (levels < TREE_LEVELS) {
				       blindiTree[0] = blindiTree[2] - 1;
				       range_min_value_array[1] = true;
				       range_min_value_array[2] = true;
			       } else {
				       blindiTree[0] = min_pos_value_in_range(this, 0, valid_len - 2);
			       }
		       } else if (is_remove_the_last_item && (blindiTree[0] == (valid_len - 1))) {
			       if (TREE_LEVELS > 1) {
				       blindiTree[0] = blindiTree[1];
				       range_min_value_array[1] = true;
				       range_min_value_array[2] = true;
			       } else {
				       blindiTree[0] = min_pos_value_in_range(this, 0, valid_len - 2);
			       }
		       } else if (blindiTree[0] >=  remove_position) {
			       blindiTree[0] -= 1;
		       }


			if (TREE_LEVELS > 1) {

				levels += 1;
				for (int l = levels; l <= TREE_LEVELS; l++) {
					// go over the elements in the level
					for (int i = pow(2,l) - 1; i >= pow(2, (l - 1)); i--) {
						int pos = i -1;
						int parent_pos = ((pos + 1) >> 1) - 1;
						if (blindiTree[parent_pos] == END_TREE) {
							blindiTree[pos] = END_TREE;
							continue;
						}
						if ((blindiTree[parent_pos] == 0) && (pos & 1)) { // the parent points to the first and the current is odd ( arrive from the small part)
							blindiTree[pos] = END_TREE;
							continue;
						}
						if ((blindiTree[parent_pos] == valid_len - 2) && (pos % 2 == 0)) {// the parent points to the last and the current is even (arrive from the large part)
							blindiTree[pos] = END_TREE;
							continue;
						}
						if (range_min_value_array[pos]) {
							int start_pos, end_pos;
							bring_edges_seq_from_pos((idx_t *)blindiTree, valid_len, pos, &start_pos, &end_pos);
							blindiTree[pos] = min_pos_value_in_range(this, start_pos, end_pos);
							if (l < TREE_LEVELS) {
								int pos_lower_level = (pos + 1 << 1);
								range_min_value_array[pos_lower_level - 1] = 1;
								range_min_value_array[pos_lower_level] = 1;
							}
							continue;
						}
						if (blindiTree[pos] == END_TREE) {
							continue;
						}
						if ((is_remove_the_first_item && blindiTree[pos] == 0) || (is_remove_the_last_item && (blindiTree[pos] == (valid_len - 1)))) {
							int start_pos, end_pos;
							bring_edges_seq_from_pos((idx_t *)blindiTree, valid_len, pos, &start_pos, &end_pos);
							blindiTree[pos] = min_pos_value_in_range(this, start_pos, end_pos);
							if (l < TREE_LEVELS) {
								int pos_lower_level = (pos + 1 << 1);
								range_min_value_array[pos_lower_level - 1] = true;
								range_min_value_array[pos_lower_level] = true;
							}
							continue;
						}
						if ((blindiTree[pos] == blindiTree[parent_pos]) || ((blindiTree[pos]) ==  (remove_position - 1))) {
							int start_pos, end_pos;
							bring_edges_seq_from_pos((idx_t *)blindiTree, valid_len, pos, &start_pos, &end_pos);
							blindiTree[pos] = min_pos_value_in_range(this, start_pos, end_pos);
							if (l < TREE_LEVELS) {
								int pos_lower_level = (pos + 1 << 1);
								range_min_value_array[pos_lower_level - 1] = true;
								range_min_value_array[pos_lower_level] = true;
							}
							continue;
						}
						if (blindiTree[pos] >=  remove_position) {
							blindiTree[pos] -= 1;
						}
						if (blindiTree[pos] == blindiTree[parent_pos]) {
							int start_pos, end_pos;
							bring_edges_seq_from_pos((idx_t *)blindiTree, valid_len, pos, &start_pos, &end_pos);
							blindiTree[pos] = min_pos_value_in_range(this, start_pos, end_pos);
							if (l < TREE_LEVELS) {
								int pos_lower_level = (pos + 1 << 1);
								range_min_value_array[pos_lower_level - 1] = true;
								range_min_value_array[pos_lower_level] = true;
							}
						}

					}
				}
			}
	       }

		return REMOVE_SUCCESS;
	}

	void push_down_blindiTree (idx_t *source_blindiTree, uint16_t source_pos, uint16_t dest_pos, int cur_level, int offset) {
		int src_level = 0;
		int level_offset = 0;
		for (int l = cur_level; l < TREE_LEVELS; l++) {
			for (int i = 0; i <= pow(2, level_offset) - 1; i++) {
				blindiTree[dest_pos + i] = source_blindiTree[source_pos + i] == END_TREE ? END_TREE : source_blindiTree[source_pos + i] + offset;
			}
			dest_pos = ((dest_pos + 1) << 1) - 1;
			source_pos = ((source_pos + 1) << 1) - 1;
			level_offset += 1;
		}
	}


	// The merge is from the large node to small_node (this)
	int MergeBlindiNodes(SeqTreeBlindiNode *node_large) {
		// copy the key_ptr from large node to small_node (this) -> create two trees pointing to small node
		int small_node_len = this->get_valid_len();
		int large_node_len = node_large->get_valid_len();
		for (int i=0; i < large_node_len - 1; i++) {
			this->key_ptr[small_node_len + i] = node_large->key_ptr[i];
			this->blindiKeys[small_node_len + i] = node_large->blindiKeys[i];
#ifdef KEY_VAR_LEN
			this->key_len[small_node_len + i] =  node_large->key_len[i] ;
#endif
#ifdef FINGERPRINT
			this->fingerprint[small_node_len + i] = node_large->fingerprint[i];
#endif
		}

		this->key_ptr[small_node_len + large_node_len -1] = node_large->key_ptr[large_node_len -1];
#ifdef KEY_VAR_LEN
		this->key_len[small_node_len + large_node_len -1] =  node_large->key_len[large_node_len -1] ;
#endif
#ifdef FINGERPRINT
		this->fingerprint[small_node_len + large_node_len -1 ] = node_large->fingerprint[large_node_len -1];
#endif

		uint8_t *last_small_key = this->key_ptr[small_node_len - 1];
		uint8_t *first_large_key = node_large->key_ptr[0];
		bool _hit, _large_small;
		uint16_t _diff_pos;

		_diff_pos = CompareStage (last_small_key, first_large_key, &_hit, &_large_small, sizeof(_Key), sizeof(_Key));
		if (_hit) 
		{
			_diff_pos = DUPLICATE;	
		}
		this->blindiKeys[small_node_len - 1] = _diff_pos;

		// calc new len
		this->valid_len += large_node_len;


		//  CALC TREE
		idx_t small_node_blindiTree [TREE_SIZE];
		int next_pos = 0;
		int blindiTree_small_node_pos = 0;
		int blindiTree_large_node_pos = 0;

                if (TREE_LEVELS > 0) {
			for (int i = blindiTree_small_node_pos; i < TREE_SIZE; i++) {
				small_node_blindiTree[i] = blindiTree[i];
			}

			for (int l=0; l < TREE_LEVELS; l++) {
				if (((small_node_blindiTree[blindiTree_small_node_pos] == END_TREE) || (_diff_pos <  blindiKeys[small_node_blindiTree[blindiTree_small_node_pos]])) && (node_large->blindiTree[blindiTree_large_node_pos] == END_TREE || _diff_pos <  node_large->blindiKeys[node_large->blindiTree[blindiTree_large_node_pos]])) { // middle item wins
					blindiTree[next_pos] = small_node_len - 1;
					if (l < TREE_SIZE - 1) {
						push_down_blindiTree(small_node_blindiTree,blindiTree_small_node_pos, ((next_pos + 1) << 1) - 1 , l + 1, 0);
						push_down_blindiTree(node_large->blindiTree, blindiTree_large_node_pos, ((next_pos + 1) << 1),  l + 1, small_node_len);
					}
					break;
				}
				if (((small_node_blindiTree[blindiTree_small_node_pos] != END_TREE) && (node_large->blindiTree[blindiTree_large_node_pos] == END_TREE || blindiKeys[small_node_blindiTree[blindiTree_small_node_pos]] <  node_large->blindiKeys[node_large->blindiTree[blindiTree_large_node_pos]]))) { // the small tree win
					if (l < TREE_LEVELS) {
						blindiTree[next_pos] =  small_node_blindiTree[blindiTree_small_node_pos];
						if (l < TREE_LEVELS - 1) {
							next_pos = (next_pos + 1) << 1;
							blindiTree_small_node_pos = (blindiTree_small_node_pos + 1) << 1;
							push_down_blindiTree(small_node_blindiTree, blindiTree_small_node_pos - 1, next_pos - 1 , l + 1, 0);
						}
					}
				} else  { // the large tree win
					blindiTree[next_pos] = node_large->blindiTree[blindiTree_large_node_pos] + small_node_len;
					if (l < TREE_SIZE - 1) {
						blindiTree_large_node_pos = ((blindiTree_large_node_pos + 1) << 1) - 1;
						next_pos = ((next_pos + 1) << 1) - 1;
						push_down_blindiTree(node_large->blindiTree,blindiTree_large_node_pos + 1, next_pos + 1, l + 1, small_node_len);
					}
				}
			}
		}
		return 0;
	}

	int CopyNode(SeqTreeBlindiNode *node_large) {
		// copy the key_ptr from large node to small_node (this) -> create two trees pointing to small node
		int large_node_len = node_large->get_valid_len();
		for (int i=0; i < large_node_len - 1; i++) {
			this->key_ptr[i] = node_large->key_ptr[i];
			this->blindiKeys[i] = node_large->blindiKeys[i];
#ifdef KEY_VAR_LEN
			this->key_len[i] =  node_large->key_len[i] ;
#endif
#ifdef FINGERPRINT
			this->fingerprint[i] = node_large->fingerprint[i];
#endif
		}

		this->key_ptr[large_node_len -1] = node_large->key_ptr[large_node_len -1];
#ifdef KEY_VAR_LEN
		this->key_len[large_node_len -1] =  node_large->key_len[large_node_len -1] ;
#endif
#ifdef FINGERPRINT
		this->fingerprint[large_node_len -1 ] = node_large->fingerprint[large_node_len -1];
#endif

		// calc new len
		this->valid_len += large_node_len;
                if (TREE_LEVELS > 0) {
			for (int i=0; i < TREE_SIZE; i++) {
				this->blindiTree[i] = node_large->blindiTree[i];
			}
		}
		return 0;
	}

	int transferNode(uint16_t old_node_len, bitidx_t *oldblindiKeys, uint8_t **oldkey_ptr, uint8_t  *oldblindiTree) { 
		// copy the key_ptr from large node to small_node (this) -> create two trees pointing to small node

#ifdef BREATHING_BLINDI_SIZE
		uint16_t cms;
		if (NUM_SLOTS / 2 + BREATHING_BLINDI_SIZE > NUM_SLOTS) 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum += (uint16_t)NUM_SLOTS/2;
#endif
			cms = (uint16_t)NUM_SLOTS;
		}
		else 
		{
#ifdef BREATHING_BLINDI_STSTS 
			breathing_sum +=  (uint16_t)BREATHING_BLINDI_SIZE;
#endif
			cms = (uint16_t)NUM_SLOTS / 2 +(uint16_t)BREATHING_BLINDI_SIZE;
		}
		this->currentmaxslot = cms;
		this->key_ptr = (uint8_t **)malloc (cms * sizeof(uint8_t *));  
#ifndef BREATHING_BLINDI_DATA_ONLY 
		this->blindiKeys = (bitidx_t *)malloc ((cms - 1) * sizeof(bitidx_t));  
#endif 
#endif
		for (int i=0; i < old_node_len - 1; i++) {
			this->key_ptr[i] = oldkey_ptr[i];
			this->blindiKeys[i] = oldblindiKeys[i];
#ifdef KEY_VAR_LEN
			this->key_len[i] =  oldkey_len[i] ;
#endif
#ifdef FINGERPRINT
			this->fingerprint[i] = oldfingerprint[i];
#endif
		}

		this->key_ptr[old_node_len -1] = oldkey_ptr[old_node_len -1];
#ifdef KEY_VAR_LEN
		this->key_len[old_node_len -1] =  oldkey_len[large_node_len -1] ;
#endif
#ifdef FINGERPRINT
		this->fingerprint[old_node_len -1 ] = oldfingerprint[large_node_len -1];
#endif
		this->valid_len = old_node_len;  

// create new TREE_LEVEL
		if (TREE_LEVELS > 0) {
			if (TREE_LEVEL_HYBRID && NUM_SLOTS == START_TREE_LEVELS)
			{ // create new level from skrech
				this->blindiTree[0] = 0;
				for (int i=1; i < (valid_len - 1); i++) 
				{ 
					this->blindiTree[0] = oldblindiKeys[i] < oldblindiKeys[blindiTree[0]] ? i : this->blindiTree[0];
				}
				if (TREE_LEVELS > 1) { 
					for (int l = 2; l <= TREE_LEVELS; l++) {
						for (int i = pow(2,(l -1)) - 1; i <= pow(2,l) - 2; i++) {
							{
								if (blindiTree[i>>1] != END_TREE)
								{ 
									update_blindiTree_from_seq(this, i);
								}
								else // the parent is END_TREE so also I get END_TREE
								{
									this->blindiTree[i] = END_TREE;
								}		
							}
						}
					}
				}
			}
                        else { // copy the node  
				for (int i=0; i < TREE_SIZE; i++) {
					this->blindiTree[i] = oldblindiTree[i];
				}
			}
		}
// free old node
#ifdef BREATHING_BLINDI_SIZE
#ifndef BREATHING_BLINDI_DATA_ONLY
			    free(oldblindiKeys);
#endif  
			    free(oldkey_ptr);
#endif
			    return 0;
	}	
};





template<class NODE, typename _Key, int NUM_SLOTS>
class GenericBlindiSeqTreeNode {
	public:
		NODE node;

		GenericBlindiSeqTreeNode() : node() { }
		typedef typename std::conditional<sizeof(_Key) <= 31,
			uint8_t, uint16_t>::type bitidx_t;

		typedef typename std::conditional<NUM_SLOTS < 254,
			uint8_t, uint16_t>::type idx_t;



		uint16_t get_valid_len() const {
			return node.get_valid_len();
		}

		uint8_t *get_key_ptr(int pos) const {
			return node.get_key_ptr(pos);
		}

		uint8_t **get_ptr2key_ptr(){
			return node.get_ptr2key_ptr();
		}
	
		_Key get_max_key_in_node() const {
			return (*((_Key*) node.get_key_ptr(node.get_valid_len() - 1)));
		}

		_Key get_min_key_in_node() const {
			return (*((_Key*) node.get_key_ptr(0)));
		}


		_Key get_mid_key_in_node() const {
			return (*((_Key*) node.get_key_ptr(node.get_valid_len() / 2  - 1)));
		}
		// transfer from btree node 2 blindi_node
		void transfer_btree2blindi(uint16_t valid_len, const _Key *keys, uint8_t **keys_ptr, uint16_t *key_len = NULL, uint8_t *fingerprint = NULL)
		{
			node.transfer_btree2blindi(valid_len, keys, keys_ptr ,key_len, fingerprint);	
		}	

		// Predecessor search -> return the position of the largest key not greater than x.
		// get the node, the key and a pointer to hit, return a position in the node. If hit: the was exact as the key in the DB. If miss: the key does not appear in the DB
		// We return the value of diff_pos and large_small on demand (if they are not NULL)
		// Tree  search
		int SearchBlindiNode(const uint8_t *search_key, uint16_t key_len, bool search_type, bool *hit, bool *smaller_than_node, uint16_t *tree_traverse, uint16_t *tree_traverse_len, uint8_t fingerprint = 0, int *diff_pos = NULL, bool *large_small =  NULL) {
			if (get_valid_len() == 0)
			{
				*hit = false;
				return -1;
			}
			*smaller_than_node = false;
			bool _large_small = false;
			int _diff_pos = 0;
			int first_stage_pos = node.SearchFirstStage(search_key, key_len, tree_traverse, tree_traverse_len);
			bool is_pointer_area = 0;
			//	uint8_t *db_key = node.bring_key(is_pointer_area ? first_stage_pos - END_TREE: first_stage_pos);  // bring the key from the DB
			uint8_t *db_key = node.bring_key(first_stage_pos);  // bring the key from the DB
#ifdef FINGERPRINT
			if (search_type == POINT_SEARCH && fingerprint !=  node.bring_fingerprint(first_stage_pos)) { // We return a value if it is a Point search or if we had a hit

				*smaller_than_node = false;
				*hit = false;
				return first_stage_pos;
			}
#endif
#ifdef DEBUG_SEQTREE
                        std::cout << "find key "; 
				for (int j = sizeof(_Key) - 1; j>= 0; j--) {
					printf ("%d ", search_key[j]);
				}
                        std::cout << " " << std::endl; 
			node.print_node();
#endif

			_diff_pos = CompareStage (search_key, db_key, hit, &_large_small, sizeof(_Key), sizeof(_Key));
			if ((diff_pos != NULL) && (large_small != NULL)) {
				*diff_pos = _diff_pos;
				*large_small = _large_small;
			}

			if (search_type == POINT_SEARCH || *hit) { // We return a value if it is a Point search or if we had a hit
				*smaller_than_node = false;
				return first_stage_pos;
			}
                        if (TREE_LEVELS > 0){
				if ((node.get_blindiTree(tree_traverse[2]) == END_TREE) || (_diff_pos < node.get_blindiKey(node.get_blindiTree(tree_traverse[2]))))  { // tree second stage
					tree_traverse[4] = 1; // search result in the tree
					if (node.get_blindiTree(0) == END_TREE ||  _diff_pos < node.get_blindiKey(node.get_blindiTree(0))) { // smaller than the head
						*tree_traverse_len = 1;
						tree_traverse[2] = 0;
						if (_large_small) {
							return node.get_valid_len() - 1;
						} else {
							*smaller_than_node = 1;
							return 0;
						}
					}
					return node.SearchSecondStageTree(_diff_pos, _large_small, is_pointer_area, smaller_than_node, tree_traverse, tree_traverse_len);
				} else { // seq seacond stage
					return node.SearchSecondStageSeq(first_stage_pos , _diff_pos, _large_small, smaller_than_node, tree_traverse, tree_traverse_len);
				}
			}
			else {  // no tree - always seq second_stage_pos
                             
				return node.SearchSecondStageSeq(first_stage_pos , _diff_pos, _large_small, smaller_than_node, tree_traverse, tree_traverse_len);
			}

		}


		// SearchBlindiNode
		// Insert2BlindiNodeInPosition
		// return success or duplicated key or overflow
		// INSERT TREE
		int Insert2BlindiNodeWithKey(uint8_t *insert_key, uint16_t key_len = sizeof(_Key), uint8_t fingerprint = 0) 
		{
#ifdef BREATHING_BLINDI_STSTS 
			insert_count++;
#endif
			bool hit = 0;
			int diff_pos = 0;
			bool large_small = 0;
			uint16_t tree_traverse[NUM_SLOTS] = {0};
			uint16_t tree_traverse_len;
			bool smaller_than_node = false;
			uint16_t insert_position;
			int return_value = INSERT_SUCCESS;

			if (node.get_valid_len() ==(uint16_t)NUM_SLOTS) {
				return INSERT_OVERFLOW;
			}

			if (node.get_valid_len() != 0) { // if the node is not empty
				insert_position = SearchBlindiNode(insert_key, key_len, PREDECESSOR_SEARCH, &hit, &smaller_than_node, &tree_traverse[0], &tree_traverse_len, fingerprint, &diff_pos, &large_small);
			} else {
				node.first_insert(insert_key, key_len, fingerprint);
				return INSERT_SUCCESS;
			}

			if (hit) {  // it was exeact we have duplicated key
				//			printf("Error: Duplicated key\n");
				large_small = 1;
				diff_pos = DUPLICATE;
				return_value = INSERT_DUPLICATED_KEY;
			}

			node.Insert2BlindiNodeInPosition(insert_key, key_len, insert_position, &diff_pos, &large_small, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
#ifdef BREATHING_BLINDI_STSTS
//			if (!(insert_count % (1024*1024))){ 
//				std::cout << "insert_count " << insert_count <<" breathing_sum " << breathing_sum << " breathing_count " << breathing_count << std::endl;
//			}
#endif

#ifdef DEBUG_SEQTREE
			std::cout << "Insert key " << std::endl;
				for (int j = sizeof(_Key) - 1; j>= 0; j--) {
					printf ("%d ", insert_key[j]);
				}
                        std::cout << " " << std::endl; 

			node.print_node();
#endif
#ifdef BREATHING_BLINDI_STSTS
                          
			if (0) { //(!(insert_count % (1024*1024))){ 
				std::cout << "insert_count " << insert_count <<" breathing_sum " << breathing_sum << " breathing_count " << breathing_count << std::endl;
			}
#endif
			return return_value;
		}

		void SplitBlindiNode(GenericBlindiSeqTreeNode *node_small, GenericBlindiSeqTreeNode *node_large) {
			node.SplitBlindiNode(&node_small->node, &node_large->node);
#ifdef DEBUG_SEQTREE
			printf("split!!!\n"); 
			node_large->node.print_node();
#endif
		}


		// UPSERT either inserts a new key into the blindi node, or overwrites the key pointer
		// return success or duplicated key or overflow
		// UPSERT TREE
		uint16_t Upsert2BlindiNodeWithKey(uint8_t *insert_key, uint16_t key_len = sizeof(_Key), uint8_t fingerprint = 0) {
			bool hit = 0;
			int diff_pos = 0;
			bool large_small = 0;
			uint16_t tree_traverse[NUM_SLOTS] = {0};
			uint16_t tree_traverse_len;
			bool smaller_than_node = false;
			uint16_t insert_position;

			if (node.get_valid_len() == 0) {  // if the node is not empty
				node.first_insert(insert_key, key_len, fingerprint);
				return INSERT_SUCCESS;
			}

			insert_position = SearchBlindiNode(insert_key, key_len, PREDECESSOR_SEARCH, &hit, &smaller_than_node, &tree_traverse[0], &tree_traverse_len, fingerprint, &diff_pos, &large_small);

			if (node.get_valid_len() == NUM_SLOTS && !hit)
				return INSERT_OVERFLOW;

			node.Insert2BlindiNodeInPosition(insert_key, key_len, insert_position, &diff_pos, &large_small, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
#ifdef DEBUG_SEQTREE
			node.print_node();
#endif
#ifdef BREATHING_BLINDI_STSTS
//			if (!(insert_count % (1024*1024))){ 
				// std::cout << "insert_count " << insert_count <<" breathing_sum " << breathing_sum << " breathing_count " << breathing_count << std::endl;
//			}
#endif
			return (hit)? INSERT_DUPLICATED_KEY : INSERT_SUCCESS;
		}


		int MergeBlindiNodes(GenericBlindiSeqTreeNode *large_node) {
			int small_node_len = this->get_valid_len();
			int large_node_len = large_node->get_valid_len();
			if (node.get_valid_len() == 0) {
				this->node.CopyNode(&large_node->node);
				return 0;
			}	 
			if (large_node->node.get_valid_len() == 0){
				return 0;
			} 
			return this->node.MergeBlindiNodes(&large_node->node);
		}

		int RemoveFromBlindiNodeWithKey(uint8_t *remove_key, uint16_t key_len = sizeof(_Key), uint8_t fingerprint = 0) {
			bool hit = 0;
			uint16_t position = 0;
			uint16_t tree_traverse[NUM_SLOTS] = {0};
			uint16_t tree_traverse_len;
			bool smaller_than_node;


			position = SearchBlindiNode(remove_key, key_len, POINT_SEARCH, &hit, &smaller_than_node, &tree_traverse[0], &tree_traverse_len, fingerprint);
			if (hit == 0) {
				return REMOVE_KEY_NOT_FOUND;
			}
			int remove_result =  node.RemoveFromBlindiNodeInPosition(position, &tree_traverse[0], &tree_traverse_len);
			int valid_len = node.get_valid_len();
			if ((valid_len < NUM_SLOTS / 2)) {
				if (remove_result == REMOVE_SUCCESS) {
					return REMOVE_UNDERFLOW;
				}
			}
			return remove_result;
		}
};
#ifndef _BLINDI_COMPARE_H_
#define _BLINDI_COMPARE_H_
// compare the bring key with search key- find the first bit fdiifreance between the searched key and the key we bring from DB.
static int CompareStage (const uint8_t *key1, const uint8_t *key2, bool *eq, bool *big_small, uint16_t key_len_key1, uint16_t key_len_key2)
{
	*eq = false;
#ifdef KEY_VAR_LEN
	uint16_t key_len_byte = min(key_len_key1, key_len_key2);
#else
	uint16_t key_len_byte = key_len_key1;
#endif

#ifdef IS_STRING
	for (int i = 0; i <= key_len_byte - 1 ; i++) {
		int mask_byte =  *(key1 + i) ^ *(key2 + i);
#else
		for (int i = 1; i <= key_len_byte ; i++) {
			int mask_byte =  *(key1 + key_len_key1 - i) ^ *(key2 + key_len_key2 - i);
#endif
			if (mask_byte != 0) {
#ifdef IS_STRING
				if(*(key1 + i) > *(key2 + i)) {
#else
					if (*(key1 + key_len_key1 - i) > *(key2 + + key_len_key2 - i)) {
#endif
						*big_small = true;
					} else {
						*big_small = false;
					}
#ifdef IS_STRING
					return 8*(i) + firstMsbLookUpTable(mask_byte);
#else
					return 8*(i - 1) + firstMsbLookUpTable(mask_byte);
#endif
				}
			}
			*eq = true;
			return 0;
		}
#endif // _BLINDI_COMPARE_H_
#endif // _BLINDI_SEQTREE_H_
