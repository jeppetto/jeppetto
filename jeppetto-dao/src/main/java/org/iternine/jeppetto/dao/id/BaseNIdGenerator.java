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
 *      Base 36 = f4nz9v3p3bpd1sftuut9hglkr1 (length = 26)
 *      Base 62 = RJJ4WidpgN7yib4ltd67Y9 (length = 22)
 *
 * Note that for a given exponent and character set, the generated values will be padded to ensure they are the
 * same length.  Also, this class uses ThreadLocalRandom to produce random numbers, so it should not have contention
 * issues at higher loads.
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
