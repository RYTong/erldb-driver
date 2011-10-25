/**
 * Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
 * All rights reserved.
 *
 * The contents of this file are subject to the Erlang Database Driver
 * Public License Version 1.0, (the "License"); you may not use this 
 * file except in compliance with the License. You should have received
 * a copy of the Erlang Database Driver Public License along with this
 * software. If not, it can be retrieved via the world wide web at
 * http://www.rytong.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * $Id$
 *
 *  @file EiEncoder.h
 *  @brief Encode data for erlang type.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Wed Dec 3 15:52:52 CST 2010
 */

#ifndef _RYT_EI_ENCODER_H
#define _RYT_EI_ENCODER_H

#include <ei.h>

namespace rytong {
/** @brief Encode data for erlang type.
 */
class EiEncoder {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    EiEncoder() {}
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~EiEncoder() {}

    /** @brief Encode a tuple like this {atom(), string()}.
     *  @param[in] atom The data for atom.
     *  @param[in] str The data for string.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_tuple_string(const char* atom, const char* str,
            ei_x_buff * const res) {
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, atom);
        ei_x_encode_string(res, str);
    }

    /** @brief Encode a tuple like this {atom(), integer()}.
     *  @param[in] atom The data for atom.
     *  @param[in] num The data for integer.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_tuple_long(const char* atom, long num,
            ei_x_buff * const res) {
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, atom);
        ei_x_encode_long(res, num);
    }

    /** @brief Encode a tuple like this {atom(), binary()}.
     *  @param[in] atom The data for atom.
     *  @param[in] str The data for binary.
     *  @param[in] size Binary data length.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_tuple_binary(const char* atom, const char* str, 
            int size, ei_x_buff * const res) {
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, atom);
        ei_x_encode_binary(res, str, size);
    }

    /** @brief Encode a tuple like this {error, string()}.
     *  @param[in] str The data for string.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_error_msg(const char* str, ei_x_buff * const res) {
        encode_tuple_string("error", str, res);
    }

    /** @brief Encode a tuple like this {ok, string()}.
     *  @param[in] str The data for string.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_ok_msg(const char* str, ei_x_buff * const res) {
        encode_tuple_string("ok", str, res);
    }

    /** @brief Encode a tuple like this {error, integer()}.
     *  @param[in] num The data for integer.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_error_number(long num, ei_x_buff * const res) {
        encode_tuple_long("error", num, res);
    }

    /** @brief Encode a tuple like this {ok, integer()}.
     *  @param[in] num The data for integer.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_ok_number(long num, ei_x_buff * const res) {
        encode_tuple_long("ok", num, res);
    }

    /** @brief Encode a tuple like this {ok, binary()}.
     *  @param[in] pointer The data for binary.
     *  @param[out] res The data for encode result.
     *  @return None.
     */
    static void encode_ok_pointer(void* pointer, ei_x_buff * const res) {
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, "ok");
        ei_x_encode_binary(res, &pointer, sizeof (pointer));
    }
private:
    EiEncoder(const EiEncoder& orig);

};
}/* end of namespace rytong */

#endif /* _RYT_EI_ENCODER_H */

