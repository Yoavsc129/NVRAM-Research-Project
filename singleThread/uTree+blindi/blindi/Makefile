CXX =		g++
CXXFLAGS =	-g -std=c++11 -O3 -I.

all:	simple_test_blindi_seqtree simple_test_blindi_subtrie simple_test_blindi_trie simple_test_blindi_seqtree_with_duplicates

simple_test_blindi_seqtree:	simple_test_blindi_seqtree.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^


clean:
	rm -rf simple_test_blindi_seqtree simple_test_blindi_subtrie simple_test_blindi_trie simple_test_blindi_seqtree_with_duplicates
