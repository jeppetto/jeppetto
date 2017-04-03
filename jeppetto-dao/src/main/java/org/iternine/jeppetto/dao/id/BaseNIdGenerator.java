/*
 * Copyright (c) 2011-2015 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.dao.id;


import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;


/**
 * This class produces randomly generated numbers and then encodes them using the characters provided in the
 * constructor.  The length of the character array corresponds to the base of the encoding (i.e. an array containing
 * the characters 0-9 and a-f would produce a hexadecimal representation).
 *
 * The number of possible identifiers is configured by the bitsPerId, the number of bits that make up each random
 * identifier.  The higher this number, the less likely an identifier is to have a collision.  The downside is that
 * more bits correspond to longer identifiers.  To minimize the size, identifiers can be encoded into a higher base.
 * The more characters available to encode with, the shorter an identifier can be for a given number of bits.
 *
 * Here are examples of identifiers with two different bit lengths and the size of the generated strings for
 * different bases.  As a note, UUIDs use 128 bits and are generally represented in hexadecimal.
 *
 *      Exponent: 64 (2^64 possible identifiers)
 *      Base 10 = 81559628491067131560 (length = 20)
 *      Base 16 = 857dbd8d413736a5 (length = 16)
 *      Base 36 = 0uykurg7cfhd1 (length = 13)
 *      Base 62 = IJkOeiKp8L7 (length = 11)
 *
 *      Exponent: 128 (2^128 possible identifiers)
 *      Base 10 = 34391415693059914568711289921679647638 (length = 39)
 *      Base 16 = fc45accd3d5d9ca5c7239174a693f147 (length = 32)
 *      Base 36 = f4nz9v3p3bpd1sftuut9hglkr1 (length = 25)
 *      Base 62 = RJJ4WidpgN7yib4ltd67Y9 (length = 22)
 *
 * Note that for a given exponent and character set, the generated values will be padded to ensure they are the
 * same length.  Also, this class uses ThreadLocalRandom to produce random numbers, so it should not have contention
 * issues at higher loads.
 *
 * To determine how many characters will be used to encode a number of bits into a character set, use the formula:
 *
 *     c = n / log2(b)
 *
 * where c is the length of the resulting identifier, n is the number of bits per identifier, and b is the base used
 * to encode the identifier.
 *
 * This table shows how many bits can be encoded into a string of length c in some common bases.  For example, if you
 * wanted a 10-character identifier in base 36, you could get 51.70 bits.  Since there isn't a partial bit, one would
 * select the next lower integer as the maximum number of bits to use when generating ids.  If instead you want to
 * know how many characters a certain number of bits will be encoded into for a given base, find the number just
 * greater than that in the corresponding column and then see what the value for 'c' is in that row.  For example, if
 * you wanted 128 bits to be encoded in base36, you would look down that column until you found 129.25 (the first
 * number larger than 128), then look to the first column in the row and see the number 25.
 *
 *     c	|  base10	|  base16	|  base36	|  base62
 *   ------------------------------------------------------
 *     1	|    3.32	|    4.00	|    5.17	|    5.95
 *     2	|    6.64	|    8.00	|   10.34	|   11.91
 *     3	|    9.97	|   12.00	|   15.51	|   17.86
 *     4	|   13.29	|   16.00	|   20.68	|   23.82
 *     5	|   16.61	|   20.00	|   25.85	|   29.77
 *     6	|   19.93	|   24.00	|   31.02	|   35.73
 *     7	|   23.25	|   28.00	|   36.19	|   41.68
 *     8	|   26.58	|   32.00	|   41.36	|   47.63
 *     9	|   29.90	|   36.00	|   46.53	|   53.59
 *    10	|   33.22	|   40.00	|   51.70	|   59.54
 *    11	|   36.54	|   44.00	|   56.87	|   65.50
 *    12	|   39.86	|   48.00	|   62.04	|   71.45
 *    13	|   43.19	|   52.00	|   67.21	|   77.40
 *    14	|   46.51	|   56.00	|   72.38	|   83.36
 *    15	|   49.83	|   60.00	|   77.55	|   89.31
 *    16	|   53.15	|   64.00	|   82.72	|   95.27
 *    17	|   56.47	|   68.00	|   87.89	|  101.22
 *    18	|   59.79	|   72.00	|   93.06	|  107.18
 *    19	|   63.12	|   76.00	|   98.23	|  113.13
 *    20	|   66.44	|   80.00	|  103.40	|  119.08
 *    21	|   69.76	|   84.00	|  108.57	|  125.04
 *    22	|   73.08	|   88.00	|  113.74	|  130.99
 *    23	|   76.40	|   92.00	|  118.91	|  136.95
 *    24	|   79.73	|   96.00	|  124.08	|  142.90
 *    25	|   83.05	|  100.00	|  129.25	|  148.85
 *    26	|   86.37	|  104.00	|  134.42	|  154.81
 *    27	|   89.69	|  108.00	|  139.59	|  160.76
 *    28	|   93.01	|  112.00	|  144.76	|  166.72
 *    29	|   96.34	|  116.00	|  149.93	|  172.67
 *    30	|   99.66	|  120.00	|  155.10	|  178.63
 *    31	|  102.98	|  124.00	|  160.27	|  184.58
 *    32	|  106.30	|  128.00	|  165.44	|  190.53
 *    33	|  109.62	|  132.00	|  170.61	|  196.49
 *    34	|  112.95	|  136.00	|  175.78	|  202.44
 *    35	|  116.27	|  140.00	|  180.95	|  208.40
 *    36	|  119.59	|  144.00	|  186.12	|  214.35
 *    37	|  122.91	|  148.00	|  191.29	|  220.31
 *    38	|  126.23	|  152.00	|  196.46	|  226.26
 *    39	|  129.56	|  156.00	|  201.63	|  232.21
 *    40	|  132.88	|  160.00	|  206.80	|  238.17
 *    41	|  136.20	|  164.00	|  211.97	|  244.12
 *    42	|  139.52	|  168.00	|  217.14	|  250.08
 *    43	|  142.84	|  172.00	|  222.31	|  256.03
 *    44	|  146.16	|  176.00	|  227.48	|  261.98
 *    45	|  149.49	|  180.00	|  232.65	|  267.94
 *    46	|  152.81	|  184.00	|  237.82	|  273.89
 *    47	|  156.13	|  188.00	|  242.99	|  279.85
 *    48	|  159.45	|  192.00	|  248.16	|  285.80
 *    49	|  162.77	|  196.00	|  253.33	|  291.76
 *    50	|  166.10	|  200.00	|  258.50	|  297.71
 *    51	|  169.42	|  204.00	|  263.67	|  303.66
 *    52	|  172.74	|  208.00	|  268.84	|  309.62
 *    53	|  176.06	|  212.00	|  274.01	|  315.57
 *    54	|  179.38	|  216.00	|  279.18	|  321.53
 *    55	|  182.71	|  220.00	|  284.35	|  327.48
 *    56	|  186.03	|  224.00	|  289.52	|  333.43
 *    57	|  189.35	|  228.00	|  294.69	|  339.39
 *    58	|  192.67	|  232.00	|  299.86	|  345.34
 *    59	|  195.99	|  236.00	|  305.03	|  351.30
 *    60	|  199.32	|  240.00	|  310.20	|  357.25
 *    61	|  202.64	|  244.00	|  315.37	|  363.21
 *    62	|  205.96	|  248.00	|  320.54	|  369.16
 *    63	|  209.28	|  252.00	|  325.71	|  375.11
 *    64	|  212.60	|  256.00	|  330.88	|  381.07
 */
public class BaseNIdGenerator
        implements IdGenerator<String> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    public static final char[] BASE62_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    public static final char[] BASE36_CHARACTERS = Arrays.copyOfRange(BASE62_CHARACTERS, 0, 36);
    public static final char[] BASE16_CHARACTERS = Arrays.copyOfRange(BASE62_CHARACTERS, 0, 16);
    public static final char[] BASE10_CHARACTERS = Arrays.copyOfRange(BASE62_CHARACTERS, 0, 10);

    private static final int BITS_PER_LONG = 64;


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final int bitsPerId;
    private final char[] characters;
    private final int base;
    private final int encodedIdLength;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    /**
     * Construct a new EncodedIdGenerator that randomly produces ids whose representation is encoded using the provided
     * characters.
     *
     * @param bitsPerId The number of bits produced for each identifier (UUIDs, for example, have 128 bits per id).
     * @param characters The characters used to encode the generated id.
     */
    public BaseNIdGenerator(int bitsPerId, char[] characters) {
        if (bitsPerId < 1) {
            throw new IllegalArgumentException("bitsPerId must be >= 1");
        }

        if (characters == null || characters.length < 2) {
            throw new IllegalArgumentException("characters must be non-null and have at least two values");
        }

        this.bitsPerId = bitsPerId;
        this.characters = characters;

        this.base = characters.length;
        this.encodedIdLength = (int) Math.ceil(bitsPerId / (Math.log(base) / Math.log(2)));
    }


    //-------------------------------------------------------------
    // Implementation - IdGenerator
    //-------------------------------------------------------------

    @Override
    public String generateId() {
        StringBuilder sb = new StringBuilder();

        int bitsLeft = bitsPerId;
        while (bitsLeft > 0) {
            long random = ThreadLocalRandom.current().nextLong();

            if (bitsLeft < BITS_PER_LONG) {
                random >>>= (BITS_PER_LONG - bitsLeft);
            }

            encode(random, sb);

            bitsLeft -= BITS_PER_LONG;
        }

        while (sb.length() < encodedIdLength) {
            sb.append(characters[0]);
        }

        // It's possible we could generate a string larger than we expect.  Cut it down if needed.
        return sb.length() == encodedIdLength ? sb.toString() : sb.substring(0, encodedIdLength);
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void encode(long bits, StringBuilder sb) {
        if (bits < 0) {
            sb.append(characters[(int) ((bits % base + base) % base)]);

            bits /= -base;
        }

        while (bits > 0) {
            sb.append(characters[(int) (bits % base)]);

            bits /= base;
        }
    }
}
