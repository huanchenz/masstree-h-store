/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MASSTREEORDEREDMULTIMAPINDEX_H_
#define MASSTREEORDEREDMULTIMAPINDEX_H_

#include <iostream>
#include "indexes/tableindex.h"
#include "common/tabletuple.h"

#include "masstree/mtIndexAPI.hh"
#include "masstree/str.hh"

namespace voltdb {

/**
 * Index implemented as a Binary Tree Multimap.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class MasstreeOrderedMultiMapIndex : public TableIndex
{

    friend class TableIndexFactory;
    friend class TableIndexWrapper;

    typedef mt_index<Masstree::default_table> MtiType;

public:

    ~MasstreeOrderedMultiMapIndex() {
      free(m_tmp1_str);
      free(m_tmp2_str);
      if (cur_values)
	free(cur_values);
    };

    bool addEntry(const TableTuple *tuple)
    {
      //std::cout << "MOM -- PUT tuple " << name_ << "\n";
      m_tmp1.setFromTupleLE(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);

      uint64_t addr = (uint64_t)tuple->address();
      uint64_t *addr_ptr = &addr;

      mt_entries.put_nuv((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8);
      ++m_inserts;
      item_count++;

      //std::cout << "item_count = " << item_count << " ic = " << mt_entries.get_ic() << " sic = " << mt_entries.get_sic() << "\n";
      return true;
    }

    bool deleteEntry(const TableTuple *tuple)
    {
      //std::cout << "MOM -- DELETE " << name_ << "\n";
      m_tmp1.setFromTupleLE(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);

      uint64_t addr = (uint64_t)tuple->address();
      uint64_t *addr_ptr = &addr;

      ++m_deletes;
      bool success = mt_entries.remove_nuv((const char*)m_tmp1_data, m_tmp1_size, (const char*) addr_ptr, 8);
      if (success)
	item_count--;
      /*
      std::cout << "item_count = " << item_count << " ic = " << mt_entries.get_ic() << " sic = " << mt_entries.get_sic() << "\n";
      if (!success)
	std::cout << "MOM -- DELETE FAIL " << name_ << "\n";
      */
      return success;
    }

    bool replaceEntry(const TableTuple *oldTupleValue,
                      const TableTuple* newTupleValue)
    {
      //std::cout << "MOM -- REPLACE " << name_ << "\n";
      m_tmp1.setFromTupleLE(oldTupleValue, column_indices_, m_keySchema);
      m_tmp2.setFromTupleLE(newTupleValue, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      char* m_tmp2_data = get_m_tmp2_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, oldTupleValue);
      int m_tmp2_size = m_tmp2.size(m_keySchema, column_indices_, newTupleValue);
      if (m_eq(m_tmp1, m_tmp2))
	return true;

      uint64_t addr = (uint64_t)newTupleValue->address();
      uint64_t *addr_ptr = &addr;

      mt_entries.remove_nuv((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8);
      mt_entries.put_nuv((const char*)m_tmp2_data, m_tmp2_size, (const char*)addr_ptr, 8);
      ++m_updates;
      return true;
    }
    
    bool setEntryToNewAddress(const TableTuple *tuple, const void* address, const void* oldAddress) {
      //std::cout << "MOM -- SETNEWADDR " << name_ << "\n";
      m_tmp1.setFromTupleLE(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);
      ++m_updates;
	
      uint64_t addr = (uint64_t)address;
      uint64_t *addr_ptr = &addr;
      uint64_t old_addr = (uint64_t)oldAddress;
      uint64_t *old_addr_ptr = &old_addr;

      mt_entries.replace((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8, (const char*)old_addr_ptr, 8);

      //mt_entries.replace_first((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8);
      return true;
    }

    bool checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs)
    {
      //std::cout << "MOM -- CHECKFORCHANGE " << name_ << "\n";
        m_tmp1.setFromTupleLE(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTupleLE(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const TableTuple* values)
    {
      //std::cout << "MOM -- EXISTS " << name_ << "\n";
      ++m_lookups;
      m_tmp1.setFromTupleLE(values, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, values);

      return mt_entries.exist_nuv((const char*)m_tmp1_data, m_tmp1_size);
    }

    bool moveToKey(const TableTuple *searchKey)
    {
      //std::cout << "MOM -- MOVETOKEY " << name_ << "\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromKeyLE(searchKey);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, searchKey);

      Str dynamic_values_str;
      Str static_values_str;
      if (!mt_entries.get_ordered_nuv((const char*)m_tmp1_data, m_tmp1_size, dynamic_values_str, static_values_str)) {
	m_match.move(NULL);
	return false;
      }
	
      if (cur_values)
	free(cur_values);
      cur_values = (char*)malloc(dynamic_values_str.len + static_values_str.len + 1);
      memcpy(cur_values, dynamic_values_str.s, dynamic_values_str.len);
      memcpy(cur_values + dynamic_values_str.len, static_values_str.s, static_values_str.len);
      cur_values_len = dynamic_values_str.len + static_values_str.len;

      char *retvalue = *(reinterpret_cast<char**>(cur_values));
      iter_pos = 0;
      m_match.move(retvalue);
      return m_match.address() != NULL;
    }

    bool moveToTuple(const TableTuple *searchTuple)
    {
      //std::cout << "MOM -- MOVETOTUPLE " << name_ << "\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromTupleLE(searchTuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, searchTuple); //???

      Str dynamic_values_str;
      Str static_values_str;
      if (!mt_entries.get_ordered_nuv((const char*)m_tmp1_data, m_tmp1_size, dynamic_values_str, static_values_str)) {
	m_match.move(NULL);
	return false;
      }
	
      if (cur_values)
	free(cur_values);
      cur_values = (char*)malloc(dynamic_values_str.len + static_values_str.len + 1);
      memcpy(cur_values, dynamic_values_str.s, dynamic_values_str.len);
      memcpy(cur_values + dynamic_values_str.len, static_values_str.s, static_values_str.len);
      cur_values_len = dynamic_values_str.len + static_values_str.len;

      char *retvalue = *(reinterpret_cast<char**>(cur_values));
      iter_pos = 0;
      m_match.move(retvalue);
      return m_match.address() != NULL;
    }

    void moveToKeyOrGreater(const TableTuple *searchKey)
    {
      //std::cout << "MOM -- ORGREATER " << name_ << "\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromKeyLE(searchKey);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, searchKey);

      mt_entries.get_upper_bound_or_equal_nuv((const char*)m_tmp1_data, m_tmp1_size);
      iter_pos = -8;
      cur_values_len = 0;
    }

    void moveToGreaterThanKey(const TableTuple *searchKey)
    {
      //std::cout << "MOM -- THANKEY " << name_ << "\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromKeyLE(searchKey);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, searchKey);

      mt_entries.get_upper_bound_nuv((const char*)m_tmp1_data, m_tmp1_size);
      iter_pos = -8;
      cur_values_len = 0;
    }

    void moveToEnd(bool begin)
    {
      //std::cout << "MOM -- MOVETOEND " << name_ << "\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = begin;
      if (begin) {
	mt_entries.get_first_nuv();
      }
      else {
	std::cout << "ERROR: MasstreeOrderedMultiIndex currently does NOT support reverse scan!";
      }
    }

    TableTuple nextValue()
    {
      //std::cout << "MOM -- NEXTVALUE " << name_ << "\n";
      TableTuple retval(m_tupleSchema);
      Str value;
      bool same_key = false;
      if (m_begin) {
	iter_pos += 8;
	if (iter_pos < cur_values_len)
	  same_key = true;
	else if (!mt_entries.get_next_nuv(value))
	  return TableTuple();

	if (!same_key) {
	  if (cur_values)
	    free(cur_values);
	  cur_values = (char*)malloc(value.len + 1);
	  memcpy(cur_values, value.s, value.len);
	  cur_values_len = value.len;
	  iter_pos = 0;
	}
	char *retvalue = *(reinterpret_cast<char**>(cur_values + iter_pos));
	retval.move(retvalue);
      }
      else {
	std::cout << "ERROR: MasstreeOrderedMultiMapIndex currently does NOT support reverse scan!";
      }
      return retval;
    }

    TableTuple nextValueAtKey()
    {
      //std::cout << "MOM -- NEXTATKEY " << name_ << "\n";
      if (m_match.isNullTuple())
	return m_match;
      TableTuple retval = m_match;
      iter_pos += 8;
      if (iter_pos >= cur_values_len)
        m_match.move(NULL);
      else {
	char *retvalue = *(reinterpret_cast<char**>(cur_values + iter_pos));
        m_match.move(retvalue);
      }
      return retval;
    }

    bool advanceToNextKey()
    {
      //std::cout << "MOM -- ADVANCEKEY " << name_ << "\n";
      if (m_begin) {
	Str dynamic_values_str;
	Str static_values_str;
	if (!mt_entries.advance_nuv(dynamic_values_str, static_values_str)) {
	  m_match.move(NULL);
	  return false;
	}
	if (cur_values)
	  free(cur_values);
	cur_values = (char*)malloc(dynamic_values_str.len + static_values_str.len + 1);
	memcpy(cur_values, dynamic_values_str.s, dynamic_values_str.len);
	memcpy(cur_values + dynamic_values_str.len, static_values_str.s, static_values_str.len);
	cur_values_len = dynamic_values_str.len + static_values_str.len;
	iter_pos = 0;

	char *retvalue = *(reinterpret_cast<char**>(cur_values));
	  
	m_match.move(retvalue);
      }
      else {
	std::cout << "ERROR: MasstreeOrderedMultiMapIndex currently does NOT support reverse scan!";
      }
      return !m_match.isNullTuple();
    }

    size_t getSize() const { return item_count; }
    
    int64_t getMemoryEstimate() const {
      return (int64_t)mt_entries.memory_consumption();
    }
    /*
    void printTreeStats() {
      std::cout << name_ << "\n";
      std::cout << "*********************************************\n";
      mt_entries.tree_stats();
      std::cout << "*********************************************\n";
    }
    */
    std::string getTypeName() const { return "MasstreeOrderedMultiMapIndex"; };

protected:
    MasstreeOrderedMultiMapIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_begin(true),
        m_eq(m_keySchema)
    {
      //std::cout << "MasstreeOrderedMultiMapIndex\t" << scheme.name << ": " << m_tmp1.size() << "\n";
      //std::cout << "BEGIN\t" << scheme.name << "\n";
        ints_only = scheme.intsOnly;
	item_count = 0;
	mt_entries.setup(m_tmp1.size(), m_keySchema->tupleLength(), true);
        m_match = TableTuple(m_tupleSchema);
	m_memoryEstimate = 1;
	m_tmp1_str = (char*)malloc(m_keySchema->tupleLength() * 2);
	m_tmp2_str = (char*)malloc(m_keySchema->tupleLength() * 2);
	cur_values = (char*)malloc(8);
	cur_values_len = 8;
    }

    inline void m_tmp1_char_to_lesschar () {
      char* m_tmp1_char = (char*)m_tmp1.data;
      int curpos = 0;
      TableTuple keyTuple(m_keySchema);
      keyTuple.moveNoHeader(reinterpret_cast<void*>(m_tmp1.data));
      for (int i = 0; i < m_keySchema->columnCount(); i++) {
	int offset = keyTuple.getDataPosNoHeader(i);
	int len = keyTuple.getNValue(i).getColLength();
	if (len == 0)
	  len += 1;
	else
	  if (keyTuple.getType(i) == VALUE_TYPE_VARCHAR) {
	    offset++;
	    len--;
	  }
	for (int j = offset; j < (offset + len); j++) {
	  m_tmp1_str[curpos] = m_tmp1_char[j];
	  curpos++;
	}
      }
    }

    inline void m_tmp2_char_to_lesschar () {
      char* m_tmp2_char = (char*)m_tmp2.data;
      int curpos = 0;
      TableTuple keyTuple(m_keySchema);
      keyTuple.moveNoHeader(reinterpret_cast<void*>(m_tmp2.data));
      for (int i = 0; i < m_keySchema->columnCount(); i++) {
	int offset = keyTuple.getDataPosNoHeader(i);
	int len = keyTuple.getNValue(i).getColLength();
	if (len == 0)
	  len += 1;
	else 
	  if (keyTuple.getType(i) == VALUE_TYPE_VARCHAR) {
	    offset++;
	    len--;
	  }
	for (int j = offset; j < (offset + len); j++) {
	  m_tmp2_str[curpos] = m_tmp2_char[j];
	  curpos++;
	}
      }
    }

    inline char* get_m_tmp1_data() {
      if (ints_only)
	return (char*)m_tmp1.data;
      else {
	m_tmp1_char_to_lesschar();
	return m_tmp1_str;
      }
    }

    inline char* get_m_tmp2_data() {
      if (ints_only)
	return (char*)m_tmp2.data;
      else {
	m_tmp2_char_to_lesschar();
	return m_tmp2_str;
      }
    }

    MtiType mt_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;
    char* m_tmp1_str;
    char* m_tmp2_str;

    // iteration stuff
    bool m_begin;
    TableTuple m_match;
    char* cur_values;
    int cur_values_len;
    int iter_pos;

    // comparison stuff
    KeyEqualityChecker m_eq;

    bool ints_only;
    int item_count;
};

}

#endif // MASSTREEORDEREDMULTIMAPINDEX_H_
