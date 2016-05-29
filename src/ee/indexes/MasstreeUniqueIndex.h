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

#ifndef MASSTREEUNIQUEINDEX_H_
#define MASSTREEUNIQUEINDEX_H_

#define __STDC_FORMAT_MACROS
//#include <map>
#include "stx/btree_map.h"
#include <iostream>
#include <stdio.h>
#include <inttypes.h>
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"

#include "masstree/mtIndexAPI.hh"
#include "masstree/str.hh"

namespace voltdb {

/**
 * Index implemented as a Binary Unique Map.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class MasstreeUniqueIndex : public TableIndex
{
    friend class TableIndexFactory;
    friend class TableIndexWrapper;

    //typedef h_index::AllocatorTracker<pair<const KeyType, const void*> > AllocatorType;
    typedef mt_index<Masstree::default_table> MtiType;

public:

    ~MasstreeUniqueIndex() {
      free(m_tmp1_str);
      free(m_tmp2_str);
    }

    bool addEntry(const TableTuple* tuple)
    {
      //std::cout << "MU -- PUT tuple\n";
      m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);

      uint64_t addr = (uint64_t)tuple->address();
      uint64_t *addr_ptr = &addr;

      if (!mt_entries.put_uv((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8))
	return false;
      ++m_inserts;
      item_count++;
      return true;
    }

    bool deleteEntry(const TableTuple* tuple)
    {
      //std::cout << "MU -- DELETE\n";
      m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);

      ++m_deletes;
      bool success = mt_entries.remove((const char*)m_tmp1_data, m_tmp1_size);
      if (success)
	item_count--;
      return success;
    }

    bool replaceEntry(const TableTuple* oldTupleValue,
                      const TableTuple* newTupleValue)
    {
      //std::cout << "MU -- REPLACE\n";
      m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
      m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      char* m_tmp2_data = get_m_tmp2_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, oldTupleValue);
      int m_tmp2_size = m_tmp2.size(m_keySchema, column_indices_, newTupleValue);
      if (m_eq(m_tmp1, m_tmp2))
	return true;
      uint64_t addr = (uint64_t)newTupleValue->address();
      uint64_t *addr_ptr = &addr;

      bool remove_success = mt_entries.remove((const char*)m_tmp1_data, m_tmp1_size);
      bool insert_success = mt_entries.put_uv((const char*)m_tmp2_data, m_tmp2_size, (const char*)addr_ptr, 8);
      ++m_updates;
      return (remove_success && insert_success);
    }
    
    bool setEntryToNewAddress(const TableTuple *tuple, const void* address, const void* oldAddress) {
      //std::cout << "MU -- SETNEWADDR\n";
      m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, tuple);

      uint64_t addr = (uint64_t)address;
      uint64_t *addr_ptr = &addr;

      //mt_entries.remove((const char*)m_tmp1_data, m_tmp1_size);
      mt_entries.update_uv((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr);
      //mt_entries.put((const char*)m_tmp1_data, m_tmp1_size, (const char*)addr_ptr, 8);
      ++m_updates; 
      return true;
    }

    bool checkForIndexChange(const TableTuple* lhs, const TableTuple* rhs)
    {
      //std::cout << "MU -- CHECKFORCHANGE\n";
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const TableTuple* values)
    {
      //std::cout << "MU -- EXISTS\n";
      ++m_lookups;
      m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, values);

      Str value;
      bool success = mt_entries.get((const char*)m_tmp1_data, m_tmp1_size, value);
      return success;
    }

    bool moveToKey(const TableTuple* searchKey)
    {
      //std::cout << "MU -- MOVETOKEY\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromKey(searchKey);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, searchKey);

      Str value;
      if (!mt_entries.get((const char*)m_tmp1_data, m_tmp1_size, value)) {
	m_match.move(NULL);
	return false;
      }

      char *retvalue = *(reinterpret_cast<char**>(const_cast<char*>(value.s)));
      m_match.move(retvalue);
      return m_match.address() != NULL;
    }

    bool moveToTuple(const TableTuple* searchTuple)
    {
      //std::cout << "MU -- MOVETOTUPLE\n";
      //hack
      //if (unlikely(m_lookups == 0))
      //mt_entries.merge();

      ++m_lookups;
      m_begin = true;
      m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
      char* m_tmp1_data = get_m_tmp1_data();
      int m_tmp1_size = m_tmp1.size(m_keySchema, column_indices_, searchTuple); //???

      Str value;
      if (!mt_entries.get((const char*)m_tmp1_data, m_tmp1_size, value)) {
	m_match.move(NULL);
	return false;
      }

      char *retvalue = *(reinterpret_cast<char**>(const_cast<char*>(value.s)));
      m_match.moveC(retvalue);
      return m_match.address() != NULL;
    }

    TableTuple nextValueAtKey()
    {
      //std::cout << "MU -- NEXTATKEY\n";
      TableTuple retval = m_match;
      m_match.move(NULL);
      return retval;
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
    std::string getTypeName() const { return "MasstreeUniqueIndex"; };

protected:
    MasstreeUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_begin(true),
	m_eq(m_keySchema)
    {
      //std::cout << "MasstreeUniqueIndex\t" << scheme.name << ": " << m_tmp1.size() << "\n";
      //std::cout << "BEGIN\t" << scheme.name << "\n";
        ints_only = scheme.intsOnly;
	item_count = 0;
        m_match = TableTuple(m_tupleSchema);
	m_memoryEstimate = 1;
	mt_entries.setup(m_tmp1.size(), false);
	m_tmp1_str = (char*)malloc(m_keySchema->tupleLength() * 2);
	m_tmp2_str = (char*)malloc(m_keySchema->tupleLength() * 2);
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

    // comparison stuff
    KeyEqualityChecker m_eq;

    bool ints_only;
    int item_count;
};

}

#endif // MASSTREEUNIQUEINDEX_H_
