#ifndef _BIT_MANIPULATION_H_
#define _BIT_MANIPULATION_H_

#include <iostream>
#include <stdint.h>
#include <assert.h>
//#define DEBUG
//#define IS_STRING  // big endian
using namespace std;

static bool isNthBitSetInByte (uint8_t byte, uint8_t n)
{
    static uint8_t mask[] = {128, 64, 32, 16, 8, 4, 2, 1};
    return ((byte & mask[n]) != 0);
}


static bool isNthBitSetInString (const uint8_t* ptr2key, int position, int len)
{
    int string_byte = position >> 3;
    uint8_t bit_position = position % 8;
#ifdef IS_STRING
    bool set = isNthBitSetInByte(ptr2key[string_byte], bit_position);
#else
    bool set = isNthBitSetInByte(ptr2key[len -1 - string_byte], bit_position);
#endif

#ifdef DEBUG
    cout << "the position is " << position << " the string byte is " << (uint8_t)ptr2key[string_byte] << " #" << string_byte <<  " the bit is  " << bit_position << " set " << set << "\n";
#endif
    return set;
}

static int firstMsbLookUpTable (uint8_t mask_byte)
{
    switch (mask_byte) {
    case 0:
        assert(0);
    case 1 :
        return 7;
        break;
    case 2 ... 3:
        return 6;
        break;
    case 4 ... 7:
        return 5;
        break;
    case 8 ... 15:
        return 4;
        break;
    case 16 ... 31:
        return 3;
        break;
    case 32 ... 63:
        return 2;
        break;
    case 64 ... 127:
        return 1;
        break;
    case 128 ... 255:
        return 0;
        break;

    }

}

#endif // _BIT_MANIPULATION_H_
