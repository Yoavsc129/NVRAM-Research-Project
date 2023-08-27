#include <iostream>
#include <stdint.h>
#include <blindi_seqtree.hpp>
#include <assert.h>

using namespace std;
#define TEST_MID1
#define TEST_MID2
#define TEST_RAND
#define TEST_SPLIT
#define TEST_REMOVE
#define TEST_INSERT
#define INSERT_SEARCH
#define TEST_MERGE
#define TEST_MIX
#define DEBUG

typedef GenericBlindiNode<TreeBlindiNode> BlindiNode;


int common_ancestor (int a , int b, int len)
{
    for (int i=1; i<= len; i++) {
        a = a >> 1;
        b = b >> 1;
        if (a == b) {
            return a;
        }
    }
    return END_TREE;
}


void print_key_in_octet(uint8_t *key)
{
    for (int i=KEY_SIZE_BYTE-1; i>= 0; i--) {
        printf("%03u ", key[i]);
    }
    printf(" ");
}

int cmp2keys(uint8_t *key1, uint8_t *key2)
{
    for (int i=KEY_SIZE_BYTE-1; i>= 0; i--) {
        if (key1[i] > key2[i])
            return 1;
        if (key1[i] < key2[i])
            return -1;
    }
    return 0;
}

#ifdef TEST_RAND
int rand_test(int in_seed, int number_of_return)
{
    BlindiNode node(MAX_NODE_SIZE);
    bool hit = 0;
    bool smaller_than_node = 0;
    bool large_small = 0;
    int position = 0;
    uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
    uint8_t tree_traverse_len;
    uint8_t fingerprint;
    bool eq_key_data = 0;
    // zeroes the inputs
    uint8_t k[MAX_NODE_SIZE][KEY_SIZE_BYTE];
    for (int i=0; i< MAX_NODE_SIZE; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            k[i][j] = {0};
        }
    }

    int test_key_size = 1; //4 // 16
    for (int i=0; i< MAX_NODE_SIZE; i++) {
        for (int j=0; j< test_key_size; j++) {
            k[i][j] = rand()%255;
            if (j == 0) {
                for (int m=0; m<i; m++) {
                    if (k[m][0] == k[i][0]) {
                        i--;
                        break;
                    }
                }

            }
        }
    }
    uint8_t tree_traverse_[MAX_NODE_SIZE] = {0};
    node.Insert2BlindiNodeWithKey(&k[0][0], &k[0][0]);
    node.node.print_node();
    position = node.SearchBlindiNode(&k[0][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
    printf("After insert search key %d -> position %d hit %d smaller_than_node %d \n ", k[0][0], position, hit, smaller_than_node);
    for (int i=1; i<MAX_NODE_SIZE; i++) {
        uint8_t tree_traverse_[MAX_NODE_SIZE] = {0};
        if (k[i][0] == 95) {
            printf("!!!!!!!!!!!!!!!!!!!!!!!hell0 world!!!!!!!!!!!!!!!!!!!!!!!!!\n");
        }
        node.Insert2BlindiNodeWithKey(&k[i][0], &k[i][0]);
#ifdef DEBUG
        node.node.print_node();
        position = node.SearchBlindiNode(&k[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
        printf("After insert search key %d -> position %d hit %d smaller_than_node %d \n ", k[i][0], position, hit, smaller_than_node);
        printf("%d search_key -> ", i);
        print_key_in_octet(&k[i][0]);
        printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
        CompareStage (&k[i][0], node.get_data_ptr(position), &eq_key_data, &large_small);
        assert(hit == 1 & eq_key_data == 1);

        for (int j=0; j< MAX_NODE_SIZE; j++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};

            position = node.SearchBlindiNode(&k[j][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search key %d -> position %d hit %d smaller_than_node %d \n ", k[j][0], position, hit, smaller_than_node);
            assert(((j <= i) && (hit == 1)) || ((j > i) && (hit == 0)));
        }
// check max min mid
        uint8_t *max = node.get_max_in_node();
        int sum = 0;
        for (int m =0 ; m < node.get_valid_len(); m++) {
            CompareStage (&k[m][0], max, &eq_key_data, &large_small);
            if (!large_small || eq_key_data) {
                sum += 1;
            }
        }
        printf ("CHECK max[%d] = %d sum = %d\n",test_key_size - 1, max[test_key_size - 1], sum);
        assert(sum == node.get_valid_len());

        uint8_t *min = node.get_min_in_node();
        sum = 0;
        for (int m =0 ; m < node.get_valid_len(); m++) {
            CompareStage (&k[m][0], min, &eq_key_data, &large_small);
            if (large_small || eq_key_data) {
                sum += 1;
            }
        }
        printf ("CHECK min[%d] = %d sum = %d\n", test_key_size - 1, min[test_key_size -1], sum);
        assert(sum == node.get_valid_len());

        if (node.get_valid_len()%2 == 0) {
            uint8_t *mid = node.get_mid_in_node();
            sum = 0;
            for (int m =0 ; m < node.get_valid_len(); m++) {
                CompareStage (&k[m][0], mid, &eq_key_data, &large_small);
                if (!large_small || eq_key_data) {
                    sum += 1;
                }
            }
            printf ("CHECK mid[%d] = %d sum = %d\n",test_key_size - 1, mid[test_key_size - 1], sum);
            assert(sum == node.get_valid_len() / 2);
        }


#endif //DEBUG
    }
#ifdef DEBUG
    // check item by order
    for (int i=0; i< node.get_valid_len() - 1; i++) {
        CompareStage (node.node.bring_key(i), node.node.bring_key(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
        CompareStage (node.get_data_ptr(i), node.get_data_ptr(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
    }

#endif // end DEBUG	
}
#endif// end RAND
int main()
{
    uint8_t k[MAX_NODE_SIZE][KEY_SIZE_BYTE];
    for (int i=0; i< MAX_NODE_SIZE; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            k[i][j] = {0};
        }
    }


//     srand(1);
    bool hit = 0;
    bool smaller_than_node = 0;
    bool large_small = 0;
    int position = 0;
    uint8_t tree_traverse_len;
    uint8_t fingerprint;
    bool eq_key_data = 0;
    uint8_t sorted_positions[MAX_NODE_SIZE] = {0};
    uint8_t desc_positions[MAX_NODE_SIZE] = {0};
    uint8_t *T[MAX_NODE_SIZE];


#ifdef TEST_EASY

    k[0][0] = {0};
    k[1][0] = {1};
    k[2][0] = {2};
    k[3][0] = {3};


    BlindiNode node1(MAX_NODE_SIZE);

    for (int i=0; i< 4; i++) {
        uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
        node1.Insert2BlindiNodeWithKey(&k[i][0], &k[i][0]);
        position = node1.SearchBlindiNode(&k[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH,  &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
        // checking
        printf("search key %d -> position %d hit %d smaller_than_node %d \n", k[i][0], position, hit, smaller_than_node);
        assert(hit == 1);
// search key 1 -> position 0 hit 1 smaller_than_node 0
// search key 2 -> position 1 hit 1 smaller_than_node 0
// search key 3 -> position 2 hit 1 smaller_than_node 0
// search key 4 -> position 3 hit 1 smaller_than_node 0
    }
    // sorted order
    node1.node.bring_sorted_items(sorted_positions);
    for (int i=0; i< 4; i++) {
        int pos = sorted_positions[i];
        uint8_t *T;
        T = node1.node.get_data_ptr(pos);
        printf(" %d insert key: %d tree key %d\n", i, k[i][0], *T);
        CompareStage (&k[i][0], &T[0], &eq_key_data, &large_small);
        assert (eq_key_data);
    }
    // desc order
    node1.node.bring_descending_items(desc_positions);
    for (int i=0; i< 4; i++) {
        int pos = desc_positions[3 - i];
        uint8_t *T;
        T = node1.node.get_data_ptr(pos);
        printf(" %d insert key: %d tree key %d\n", i, k[i][0], *T);
        CompareStage (&k[i][0], &T[0], &eq_key_data, &large_small);
        assert (eq_key_data);
    }


#endif// finish first test	

#ifdef TEST_MID1

    k[0][0] = {2};
    k[1][0] = {6};
    k[2][0] = {4};
    k[3][0] = {1};
    k[4][0] = {7};
    k[5][0] = {5};
    k[6][0] = {3};
    k[7][0] = {0};

    BlindiNode node2(8);
    for (int i=0; i<8; i++) {
        uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
        node2.Insert2BlindiNodeWithKey(&k[i][0], &k[i][0]);
        position = node2.SearchBlindiNode(&k[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);

        printf("After insert search key %d -> position %d hit %d smaller_than_node %d \n ", k[i][0], position, hit, smaller_than_node);
        node2.node.print_node();
        assert(hit == 1);

        for (int j=0; j<8; j++) {
            position = node2.SearchBlindiNode(&k[j][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len);
            printf("check search key %d -> position %d hit %d smaller_than_node %d \n ", k[j][0], position, hit, smaller_than_node);
            CompareStage (&k[j][0], node2.node.get_data_ptr(position), &eq_key_data, &large_small);
            assert ((eq_key_data | large_small) | (smaller_than_node & !large_small));
        }
    }
    // checking
    for (int i=0; i<8; i++) {
        uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
        position = node2.SearchBlindiNode(&k[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
//	     printf("search key %d -> position %d hit %d smaller_than_node %d \n ", k[i][0], position, hit, smaller_than_node);
        assert(hit == 1);
    }
    // check sorted order
    for (int i=0; i< 7; i++) {
        CompareStage (node2.node.bring_key(i), node2.node.bring_key(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
        CompareStage (node2.get_data_ptr(i), node2.get_data_ptr(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
    }
#ifdef WITH_TREE
    for (int i=0; i< 6; i++) {
        assert (node2.node.get_blindiKey(node2.node.get_blindiTree(0)) <= node2.node.get_blindiKey(i));
    }
#endif
#endif

#ifdef TEST_MID2

    k[0][0] = {7};
    k[1][0] = {6};
    k[2][0] = {5};
    k[3][0] = {0};
    k[4][0] = {1};
    k[5][0] = {2};
    k[6][0] = {3};
    k[7][0] = {4};

    BlindiNode node3(8);

    for (int i=0; i<8; i++) {
        uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
        node3.Insert2BlindiNodeWithKey(&k[i][0], &k[i][0]);
        position = node3.SearchBlindiNode(&k[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
        printf("After insert search key %d -> position %d hit %d smaller_than_node %d \n", k[i][0], position, hit, smaller_than_node);
//	     node3.node.print_tree_traverse(tree_traverse, &tree_traverse_len);
        node3.node.print_node();
        assert(hit == 1);
        CompareStage (node3.get_data_ptr(position), &k[i][0], &eq_key_data, &large_small);
        assert(eq_key_data == 1);
        for (int j=0; j<8; j++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            position = node3.SearchBlindiNode(&k[j][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search key %d -> position %d hit %d smaller_than_node %d \n ", k[j][0], position, hit, smaller_than_node);
            assert(((j <= i) && (hit == 1)) || ((j > i) && (hit == 0)));
        }


    }

    // check sorted order
    for (int i=0; i< 7; i++) {
        CompareStage (node3.node.bring_key(i), node3.node.bring_key(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
        CompareStage (node3.get_data_ptr(i), node3.get_data_ptr(i+1), &eq_key_data, &large_small);
        assert (!eq_key_data & !large_small);
    }
#ifdef DEBUG_AGG
    printf ("debug: Agg_first_stage_tree : %0lu,  Agg_first_stage_seq: %lu, Agg_number %lu AGG_TH %d \n", Agg_first_stage_tree, Agg_first_stage_seq, Agg_number, AGG_TH);
    printf ("debug: Avg tree length first stage: %02f,  Avg seq length first stage: %02f \n", 1.0 * Agg_first_stage_tree / Agg_number, 1.0 * Agg_first_stage_seq / Agg_number);
#endif


#endif // finish mod2 test

#ifdef TEST_RAND

    int number_of_return = 1;
    for (int in=1; in<= number_of_return; in++) {
#ifdef DEBUG
looping:
        printf("\n\n\n\n ############### ROUND number %d #################### \n\n\n", in);
#endif
        rand_test(1, number_of_return);
    }

#ifdef DEBUG_AGG
    printf ("debug: Agg_first_stage_tree : %0lu,  Agg_first_stage_seq: %lu, Agg_number %lu AGG_TH %d \n", Agg_first_stage_tree, Agg_first_stage_seq, Agg_number, AGG_TH);
    printf ("debug: Avg tree length first stage: %02f,  Avg seq length first stage: %02f \n", 1.0 * Agg_first_stage_tree / Agg_number, 1.0 * Agg_first_stage_seq / Agg_number);
#endif
#endif// TEST_RAND	

#ifdef TEST_INSERT
    uint8_t s[MAX_NODE_SIZE + 1][KEY_SIZE_BYTE];
    // zeroes the inputs
    for (int i=0; i< MAX_NODE_SIZE+1; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            s[i][j] = {0};
        }
    }
    for (int j=0; j<100000; j++) {
        BlindiNode node_s1(MAX_NODE_SIZE);
        BlindiNode node_n1(MAX_NODE_SIZE);
        BlindiNode node_n2(MAX_NODE_SIZE);
        int test_key_size = 16; //4 // 16
        for (int i=0; i< MAX_NODE_SIZE; i++) {
            s[i][0] = i; // lsb is diffrent in all nodes
            for (int j=1; j< test_key_size; j++) {
                s[i][j] = rand()%255;
            }
        }

        for (int i=0; i<MAX_NODE_SIZE; i++) {
            node_s1.Insert2BlindiNodeWithKey(&s[i][0], &s[i][0]);
        }
#ifdef INSERT_SEARCH
        for (int i=1; i<MAX_NODE_SIZE ; i++) {
            uint8_t tree_len;
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            position = node_s1.SearchBlindiNode(&s[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
        }
#endif
    }
#endif





#ifdef TEST_SPLIT
// create the function bring_edges_seq_from_pos ()
//	for (int l = 1; l <= TREE_LEVELS; l++) {
//		for(int i = pow(2, l); i <= pow(2,l + 1) - 1; i++) {
//			int start = common_ancestor (i - 1, i , l);
//			int end = common_ancestor (i, i + 1, l);
//			printf ("case %d : \n", i - 1);
//			if ( (start == END_TREE)) {
//				printf("\t*start_pos = 0;\n");
//			} else {
//				printf("\t*start_pos = blindiTree[%d] + 1;\n", start - 1);
//			}
//			if ((end == END_TREE)) {
//				printf("\t*start_pos = valid_len - 2;\n");
//			} else {
//				printf("\t*end_pos = blindiTree[%d] + 1;\n", end - 1);
//			}
//
//			printf ("\tbreak;\n");
//		}
//	}
//	exit(0);

    int nu_return_split = 1000; // 400
    uint8_t sq[MAX_NODE_SIZE + 1][KEY_SIZE_BYTE];
    // zeroes the inputs
    for (int i=0; i< MAX_NODE_SIZE+1; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            sq[i][j] = {0};
        }
    }
    for (int j=0; j< nu_return_split; j++) {

        printf ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 SPLIT ROUND nu %d !!!!!!!!!!!!!!!!!!!!!!!!!\n", j);
        BlindiNode node_s1(MAX_NODE_SIZE);
        BlindiNode node_n1(MAX_NODE_SIZE);
        BlindiNode node_n2(MAX_NODE_SIZE);
        int test_key_size = 16; //4 // 16
        for (int i=0; i< MAX_NODE_SIZE; i++) {
            sq[i][0] = i; // lsb is diffrent in all nodes
            for (int j=1; j< test_key_size; j++) {
                sq[i][j] = rand()%255;
            }
        }

        node_s1.Insert2BlindiNodeWithKey(&sq[0][0], &sq[0][0]);
        for (int i=1; i<MAX_NODE_SIZE; i++) {
            node_s1.Insert2BlindiNodeWithKey(&sq[i][0], &sq[i][0]);
        }

#ifdef DEBUG
        node_s1.node.print_node();
        for (int i=1; i<MAX_NODE_SIZE ; i++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            position = node_s1.SearchBlindiNode(&sq[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&sq[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            if (i != MAX_NODE_SIZE) assert(hit == 1);
        }
#endif


        int position2 = 0;
        bool hit2 = 0;
        bool smaller_than_node2 = 0;
        tree_traverse_len = 0;
        node_s1.SplitBlindiNode(&node_n1, &node_n2);

#ifdef DEBUG

        printf("Original node\n ");
        node_s1.node.print_node();
        printf("small node\n ");
        node_n1.node.print_node();
        printf("large node\n ");
        node_n2.node.print_node();
        for (int i=MAX_NODE_SIZE - 1; i>=0; i--) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            tree_traverse_len =0;
            printf("First Node After Split\n ");
            position = node_n1.SearchBlindiNode(&sq[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            tree_traverse_len =0;
            printf("search_key -> ");
            print_key_in_octet(&sq[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            printf("Second Node After Split\n ");
            position2 = node_n2.SearchBlindiNode(&sq[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit2, &smaller_than_node2, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&sq[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position2, hit2, smaller_than_node2);
            assert(hit2 ^ hit == 1);
            if ( hit == 1) {	// if hit == 1 than node_s1 data_ptr == search key
                CompareStage (&sq[i][0], node_n1.get_data_ptr(position), &eq_key_data, &large_small);
            } else {// if hit == 1 than node_s2 data_ptr == search key
                CompareStage (&sq[i][0], node_n2.get_data_ptr(position2), &eq_key_data, &large_small);
            }
            assert (eq_key_data == 1);

            assert(position + position2 < MAX_NODE_SIZE);
            if (hit == 1) {
                assert(smaller_than_node2 == 1);  // if we are in the first node than the second node is smaller_than_node2
            }
        }
#endif
    }


#endif// finish split test	

#ifdef TEST_REMOVE

    int test_key_size = 2; //4 // 16
    int nu_return = 100;
    for (int ra = 0; ra < nu_return; ra++) {
        printf ("This is remove ROUND nu %d !!!!!\n", ra);
        BlindiNode node_d1(MAX_NODE_SIZE);
        uint8_t d[MAX_NODE_SIZE + 1][KEY_SIZE_BYTE];
        int remove_result;

        // zeroes the inputs
        for (int i=0; i< MAX_NODE_SIZE; i++) {
            for (int j=0; j< KEY_SIZE_BYTE; j++) {
                d[i][j] = {0};
            }
        }


        for (int i=0; i< MAX_NODE_SIZE; i++) {
            d[i][0] = i;
            for (int j=1; j< test_key_size; j++) {
                d[i][j] = rand()%255;
            }
        }

        for (int i=0; i<MAX_NODE_SIZE; i++) {
            node_d1.Insert2BlindiNodeWithKey(&d[i][0], &d[i][0]);
        }

        int remove_nu = 0;
        int remove_pos_arry[MAX_NODE_SIZE] = {0};
        for (int i=0; i< MAX_NODE_SIZE; i++) {
            uint8_t tree_traverse1[MAX_NODE_SIZE] = {0};
            hit = 0;
            int remove_pos = rand()% MAX_NODE_SIZE;

            node_d1.SearchBlindiNode(&d[remove_pos][0], KEY_SIZE_BYTE, POINT_SEARCH, &hit, &smaller_than_node, tree_traverse1, &tree_traverse_len, fingerprint);
            if (hit != 1) {
                continue;
            }
            printf(" Print node before Remove\n");
            node_d1.node.print_node();
            remove_result = node_d1.RemoveFromBlindiNodeWithKey(&d[remove_pos][0]);
            printf(" Print node after remove\n");
            node_d1.node.print_node();
            remove_pos_arry [remove_pos] = 1;
#ifdef DEBUG
            printf("remove_result = %d remove_nu %d pos %d valid_len %d round %d \n", remove_result, remove_nu, remove_pos, node_d1.node.get_valid_len(), ra);
            if (remove_nu == MAX_NODE_SIZE - 1) {
                assert(remove_result == 1); // REMOVE_NODE_EMPTY
            } else if (remove_nu >=  MAX_NODE_SIZE /2) {
                assert(remove_result == 2); // REMOVE_UNDERFLOW
            } else {
                printf("remove_result = %d %d \n", remove_result, remove_nu);
                assert(remove_result == 0); // REMOVE_SUCCESS
            }
            remove_nu += 1;



            printf ("This is remove nu %d !!!!!\n", remove_nu);
            if (remove_result != 1) {// check this part just if the node not empty after the remove
                for (int r=0; r< MAX_NODE_SIZE; r++) {
                    printf("Search after Remove\n");
                    uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
                    smaller_than_node = 0;
                    hit = 0;
                    fingerprint =0;
                    position = node_d1.SearchBlindiNode(&d[r][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
                    printf("%d search_key -> ", r);
                    print_key_in_octet(&d[r][0]);
                    printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
                    assert (position < node_d1.node.get_valid_len());
                    if (remove_pos_arry[r] == 1) { // the key has been removed
                        assert(hit == 0);
                    } else { // the key is here
                        bool eq_key_data = 0;
                        CompareStage (&d[r][0], node_d1.get_data_ptr(position), &eq_key_data, &large_small);
                        assert(hit == 1 & eq_key_data == 1);
                    }

                }
            }
#endif
        }
    }


#endif// TEST REMOVE

#ifdef TEST_MERGE
    uint8_t m[MAX_NODE_SIZE + 1][KEY_SIZE_BYTE];
    // zeroes the inputs
    for (int i=0; i< MAX_NODE_SIZE; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            m[i][j] = {0};
        }
    }
    for (int j=0; j< 1000; j++) {
        BlindiNode node_m1(MAX_NODE_SIZE);
        BlindiNode node_m2(MAX_NODE_SIZE);
        int test_key_size = 16; //4 // 16
        int max_in_small_node;
        switch (j%3) {
        case 0:
            max_in_small_node = 96;
            break;
        case 1:
            max_in_small_node = 128;
            break;
        case 2:
            max_in_small_node = 168;
            break;
        }
        printf ("max_in_small_node %d %d\n", max_in_small_node, j%3);
        for (int i=0; i< MAX_NODE_SIZE / 2 ; i++) {
            for (int j = 0; j< test_key_size; j++) {
                m[i][j] = rand()%max_in_small_node;
            }
            m[i][0] = i; // lsb is diffrent in all nodes
        }
        for (int i=MAX_NODE_SIZE / 2 ; i< MAX_NODE_SIZE ; i++) {
            for (int j = 0; j< test_key_size; j++) {
                m[i][j] = rand()%(256 - max_in_small_node) + max_in_small_node;
            }
            m[i][0] = i; // lsb is diffrent in all nodes
        }


        for (int i=0; i<MAX_NODE_SIZE / 2 ; i++) {
            node_m1.Insert2BlindiNodeWithKey(&m[i][0], &m[i][0]);
            node_m2.Insert2BlindiNodeWithKey(&m[MAX_NODE_SIZE/2 + i][0], &m[MAX_NODE_SIZE/2 + i][0]);
        }
        printf("ROUND #%d\n ", j);
        printf ("SMALL NODE Before Merge\n");
        node_m1.node.print_node();
        printf ("LARGE NODE Before Merge\n");
        node_m2.node.print_node();
        node_m1.MergeBlindiNodes(&node_m2);
        printf ("SMALL NODE Afrer Merge\n");
        node_m1.node.print_node();

#ifdef DEBUG
        for (int i=0;  i < MAX_NODE_SIZE; i++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            uint8_t fingerprint = 0;
            printf("Node After Merge\n ");
            position = node_m1.SearchBlindiNode(&m[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&m[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            bool eq_key_data = 0;
            CompareStage (&m[i][0], node_m1.get_data_ptr(position), &eq_key_data, &large_small);
            assert(hit == 1 & eq_key_data == 1);
            if (i >= MAX_NODE_SIZE / 2) {
                assert(position >= MAX_NODE_SIZE /2);
            } else {
                assert(position < MAX_NODE_SIZE /2);
            }

        }
#endif
    }
#endif// TEST MERGE 

#ifdef TEST_MIX
    uint8_t z[MAX_NODE_SIZE + 1][KEY_SIZE_BYTE];
    // zeroes the inputs
    for (int i=0; i< MAX_NODE_SIZE; i++) {
        for (int j=0; j< KEY_SIZE_BYTE; j++) {
            z[i][j] = {0};
        }
    }
    for (int j=0; j< 1000; j++) {
        BlindiNode node_z1(MAX_NODE_SIZE);
        BlindiNode node_z2(MAX_NODE_SIZE);
        BlindiNode node_z3(MAX_NODE_SIZE);
        int test_key_size = 16; //4 // 16
        /*		for (int i=0; i< MAX_NODE_SIZE ;i++) {
        			for (int j = 0; j< test_key_size; j++) {
        				z[i][j] = rand()%128;
        			}
        			z[i][0] = i; // lsb is diffrent in all nodes
        			i = i+1;
        			for (int j = 0; j< test_key_size; j++) {
        				z[i][j] = rand()%128 + 128;
        			}
        			z[i][0] = i + 128; // lsb is diffrent in all nodes


        		}

        */
        int max_in_small_node;
        switch (j%3) {
        case 0:
            max_in_small_node = 96;
            break;
        case 1:
            max_in_small_node = 128;
            break;
        case 2:
            max_in_small_node = 168;
            break;
        }
        printf ("max_in_small_node %d %d\n", max_in_small_node, j%3);
        for (int i=0; i< MAX_NODE_SIZE; i+=2) {
            for (int j = 0; j< test_key_size; j++) {
                z[i][j] = rand()%max_in_small_node;
            }
            z[i][0] = i; // lsb is diffrent in all nodes
        }
        for (int i=1 ; i< MAX_NODE_SIZE ; i+=2) {
            for (int j = 0; j< test_key_size; j++) {
                z[i][j] = rand()%(256 - max_in_small_node) + max_in_small_node;
            }
            z[i][0] = i; // lsb is diffrent in all nodes
        }


        for (int i=0; i<MAX_NODE_SIZE; i++) {
            node_z1.Insert2BlindiNodeWithKey(&z[i][0], &z[i][0]);
        }


#ifdef DEBUG
        printf("MIX ROUND #%d\n ", j);
        printf("z1\n ");
        node_z1.node.print_node();
        for (int i=0;  i < MAX_NODE_SIZE; i++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            uint8_t fingerprint = 0;
            position = node_z1.SearchBlindiNode(&z[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&z[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            bool eq_key_data = 0;
        }
#endif



        node_z1.SplitBlindiNode(&node_z2, &node_z3);
        printf("z2\n ");
        node_z2.node.print_node();
        printf("z3\n ");
        node_z3.node.print_node();
        node_z2.MergeBlindiNodes(&node_z3);

#ifdef DEBUG
        printf("Node After Merge\n ");
        node_z2.node.print_node();
        for (int i=0;  i < MAX_NODE_SIZE; i++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            uint8_t fingerprint = 0;
            position = node_z2.SearchBlindiNode(&z[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&z[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            bool eq_key_data = 0;
            CompareStage (&z[i][0], node_z2.get_data_ptr(position), &eq_key_data, &large_small);
            assert(hit == 1 & eq_key_data == 1);
        }
#endif
        for (int i=1 ; i<MAX_NODE_SIZE; i=i+2) { // delete the large values from n2
            node_z2.RemoveFromBlindiNodeWithKey(&z[i][0]);
        }
        printf("Node After remove large values\n ");
        node_z2.node.print_node();
        node_z2.MergeBlindiNodes(&node_z3);
        printf("Node After Merge again\n ");
        node_z2.node.print_node();
#ifdef DEBUG
        printf("ROUND #%d\n ", j);
        for (int i=0;  i < MAX_NODE_SIZE; i++) {
            uint8_t tree_traverse[MAX_NODE_SIZE] = {0};
            uint8_t fingerprint = 0;
            printf("Node After MIX\n ");
            position = node_z2.SearchBlindiNode(&z[i][0], KEY_SIZE_BYTE, PREDECESSOR_SEARCH, &hit, &smaller_than_node, tree_traverse, &tree_traverse_len, fingerprint);
            printf("search_key -> ");
            print_key_in_octet(&z[i][0]);
            printf("-> position %d hit %d smaller_than_node %d \n ", position, hit, smaller_than_node);
            bool eq_key_data = 0;
            CompareStage (&z[i][0], node_z2.get_data_ptr(position), &eq_key_data, &large_small);
            assert(hit == 1 & eq_key_data == 1);
        }
#endif

    }




#endif// TEST MIX 



}

