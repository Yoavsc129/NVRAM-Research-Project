#include <iostream>
#include <stdint.h>
#include <blindi_seqtree.hpp>
#include <assert.h>


#define NUM_SLOTS 8

using namespace std;
class NODE {
};



typedef GenericBlindiSeqTreeNode <SeqTreeBlindiNode<uint8_t*, NUM_SLOTS>, uint8_t *, NUM_SLOTS> blindi_node;

void print_key_in_octet(uint8_t *key)
{
    for (int i= (NUM_SLOTS -1); i>= 0; i--) {
        printf("%03u ", key[i]);
    }
    printf(" ");
}

int cmp2keys(uint8_t *key1, uint8_t *key2)
{
    for (int i=sizeof(key1); i>= 0; i--) {
        if (key1[i] > key2[i])
            return 1;

            return -1;
    }
    return 0;
}

int main()
{
    int key_len = 8;
    uint8_t k[NUM_SLOTS][key_len];
    for (int i=0; i< NUM_SLOTS; i++) {
        for (int j=0; j< key_len; j++) {
            k[i][j] = {0};
        }
    }
    bool hit = 0;
    bool smaller_than_node = 0;
    bool large_small = 0;
    int position = 0;
    uint16_t tree_traverse_len;

    k[0][0] = {0};
    k[1][0] = {2};
    k[2][0] = {3};
    k[3][0] = {4};

    uint8_t z[key_len] = {1,0,0,0,0,0,0,0};


    blindi_node node1;

    uint16_t tree_traverse[NUM_SLOTS] = {0};
    for (int i=0; i< 4; i++) {
        node1.Insert2BlindiNodeWithKey(&k[i][0], sizeof(k[i]));
        position = node1.SearchBlindiNode(&k[i][0], key_len, SEARCH_TYPE::POINT_SEARCH,  &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        // checking
        printf("search key %d -> position %d hit %d smaller_than_node %d \n", k[i][0], position, hit, smaller_than_node);
        assert(hit == 1);
        position = node1.SearchBlindiNode(&k[i][0], key_len, SEARCH_TYPE::PREDECESSOR_SEARCH,  &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        printf("search key %d -> position %d hit %d smaller_than_node %d \n", k[i][0], position, hit, smaller_than_node);
    }
    position = node1.SearchBlindiNode(z, key_len, SEARCH_TYPE::PREDECESSOR_SEARCH,  &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
    printf("search key %d -> position %d hit %d smaller_than_node %d \n", z[0], position, hit, smaller_than_node);
}

