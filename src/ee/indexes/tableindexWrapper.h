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

#ifndef TABLEINDEXWRAPPER_H_
#define TABLEINDEXWRAPPER_H_

#include <iostream>
#include <fstream>
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"

#include <string>

namespace voltdb {

class TableIndexWrapper : public TableIndex
{
    friend class TableIndexFactory;

public:

    ~TableIndexWrapper() {};

    bool addEntry(const TableTuple* tuple) {
      index_file_ << "CMD\taddEntry\n";
      dumpTuple(tuple);
      return ti_->addEntry(tuple);
    }

    bool deleteEntry(const TableTuple* tuple) {
      index_file_ << "CMD\tdeleteEntry\n";
      dumpTuple(tuple);
      return ti_->deleteEntry(tuple);
    }

    bool replaceEntry(const TableTuple* oldTupleValue,
                      const TableTuple* newTupleValue) {
      index_file_ << "CMD\treplaceEntry\n";
      index_file_ << "ARG\toldTupleValue\n";
      dumpTuple(oldTupleValue);

      index_file_ << "ARG\tnewTupleValue\n";
      dumpTuple(newTupleValue);

      return ti_->replaceEntry(oldTupleValue, newTupleValue);
    }
    
    bool setEntryToNewAddress(const TableTuple *tuple, const void* address, const void* oldAddress) {
      index_file_ << "CMD\tsetEntryToNewAddress\n";
      index_file_ << "ARG\ttuple\n";
      dumpTuple(tuple);

      index_file_ << "ARG\taddress\n";
      index_file_ << "VALUE\t" << address << "\n";
      return ti_->setEntryToNewAddress(tuple, address, oldAddress);
    }

    bool checkForIndexChange(const TableTuple* lhs, const TableTuple* rhs) {
      index_file_ << "CMD\tcheckForIndexChange\n";
      index_file_ << "ARG\tlhs\n";
      dumpTuple(lhs);

      index_file_ << "ARG\trhs\n";
      dumpTuple(rhs);

      return ti_->checkForIndexChange(lhs, rhs);
    }

    bool exists(const TableTuple* values) {
      index_file_ << "CMD\texists\n";
      dumpTuple(values);

      return ti_->exists(values);
    }

    bool moveToKey(const TableTuple* searchKey) {
      index_file_ << "CMD\tmoveToKey\n";
      dumpKey(searchKey);
      //index_file_ << static_cast<void*>(const_cast<TableTuple*>(searchKey)->address()) << "\n";

      return ti_->moveToKey(searchKey);
    }

    bool moveToTuple(const TableTuple* searchTuple) {
      index_file_ << "CMD\tmoveToTuple\n";
      dumpTuple(searchTuple);

      return ti_->moveToTuple(searchTuple);
    }

    void moveToKeyOrGreater(const TableTuple* searchKey) {
      index_file_ << "CMD\tmoveToKeyOrGreater\n";
      dumpKey(searchKey);

      ti_->moveToKeyOrGreater(searchKey);
    }

    void moveToGreaterThanKey(const TableTuple* searchKey) {
      index_file_ << "CMD\tmoveToGreaterThanKey\n";
      dumpKey(searchKey);

      ti_->moveToGreaterThanKey(searchKey);
    }

    void moveToEnd(bool begin) {
      index_file_ << "CMD\tmoveToEnd\n";
      index_file_ << "ARG\tbegin" << begin << "\n";
      ti_->moveToEnd(begin);
    }

    TableTuple nextValue() {
      index_file_ << "CMD\tnextValue\n";
      return ti_->nextValue();
    }

    TableTuple nextValueAtKey() {
      index_file_ << "CMD\tnextValueAtKey\n";
      return ti_->nextValueAtKey();
    }

    bool advanceToNextKey() {
      index_file_ << "CMD\tadvanceToNextKey\n";
      return ti_->advanceToNextKey();
    }

    size_t getSize() const {
      return ti_->getSize();
    }

    int64_t getMemoryEstimate() const {
      return 0;
    }

    std::string getTypeName() const {
      //std::cout << "getTypeName\n";
      //std::cout << ti_->getTypeName() << "\n";
      return ti_->getTypeName();
    }

    /*
    void ensureCapacity(uint32_t capacity) {
      std::cout << "ensureCapacity\n";
      index_file_ << "ensureCapacity\n";
      ti_->ensureCapacity(capacity);
    }
    */
    
protected:
    TableIndexWrapper(const TableIndexScheme &scheme) : TableIndex(scheme)
    {
      std::string dir("./indexFiles/");
      std::string path;
      path = dir + scheme.name;
      index_file_.open(path.c_str());
      index_file_ << "TableIndexWrapper\t" << scheme.name.c_str() << "\n";
    }

   TableIndexWrapper(const TableIndexScheme &scheme, TableIndex* ti) : TableIndex(scheme)
    {
      ti_ = ti;
      sync();
      std::string dir("./indexFiles/");
      std::string path;
      path = dir + scheme.name;
      index_file_.open(path.c_str());
      index_file_ << "TableIndexWrapper\t" << scheme.name.c_str() << "\n";
    }

    void sync() {
      this->column_indices_vector_ = ti_->column_indices_vector_;
      this->column_types_vector_ = ti_->column_types_vector_;
      this->colCount_ = ti_->colCount_;
      this->name_ = ti_->name_;
      this->m_keySchema = ti_->m_keySchema;
      this->column_indices_ = ti_->column_indices_;
    }

    void dumpTuple(const TableTuple* tuple) {
      int numCols = ti_->colCount_;
      for (int i = 0; i < numCols; i++) {
	index_file_ << "KEY\t" << (tuple->getNValue((ti_->column_indices_)[i])).debug() << "\n";
      }
      index_file_ << "VALUE\t" << static_cast<void*>(const_cast<TableTuple*>(tuple)->address()) << "\n";
    }

    void dumpKey(const TableTuple* key) {
      int numCols = ti_->colCount_;
      for (int i = 0; i < numCols; i++) {
	if (key->getSchema()->columnType(i) == VALUE_TYPE_INVALID) {
	  index_file_ << "KEY\t" << "INVALID\n";
	  //std::cout << "KEY\t" << "INVALID\n";
	  continue;
	}
	index_file_ << "KEY\t" << (key->getNValue(i)).debug() << "\n";
	std::cout << "KEY\t" << (key->getNValue(i)).debug() << "\n";
      }
    }

    TableIndex* ti_;
    ofstream index_file_;
};

}

#endif // TABLEINDEXWRAPPER_H_
