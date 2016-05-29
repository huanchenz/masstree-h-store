#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "execution/VoltDBEngine.h"

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 7
#define NUM_OF_TUPLES 1000
#define STRING_MAX_LENGTH 30

static vector<string> col5;
static vector<string> col6;

enum TestCases {
  INTS_UO,
  INTS_UH,
  COMPOSITE_UO,
  COMPOSITE_UH,
  INTS_MO,
  INTS_MH,
  COMPOSITE_MO,
  COMPOSITE_MH
};

class IndexMoreTest : public Test {
public:
  IndexMoreTest() : table(NULL) {}
  ~IndexMoreTest() {
    delete table;
    delete[] m_exceptionBuffer;
    delete m_engine;
  }

  void init(TableIndexScheme index) {
    CatalogId database_id = 1000;
    vector<boost::shared_ptr<const TableColumn> > columns;
    string *columnNames = new string[NUM_OF_COLUMNS];

    char buffer[32];

    //create tuple schema
    vector<ValueType> columnTypes;
    columnTypes.push_back(VALUE_TYPE_SMALLINT);
    columnTypes.push_back(VALUE_TYPE_TINYINT);
    columnTypes.push_back(VALUE_TYPE_INTEGER);
    columnTypes.push_back(VALUE_TYPE_BIGINT);
    columnTypes.push_back(VALUE_TYPE_BIGINT);
    columnTypes.push_back(VALUE_TYPE_VARCHAR);
    columnTypes.push_back(VALUE_TYPE_VARCHAR);
    vector<int32_t> columnLengths;
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_SMALLINT));
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    columnLengths.push_back(STRING_MAX_LENGTH);
    columnLengths.push_back(STRING_MAX_LENGTH);
    vector<bool> columnAllowNull(NUM_OF_COLUMNS, false);
    for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
      snprintf(buffer, 32, "column%02d", ctr);
      columnNames[ctr] = buffer;
    }
    TupleSchema* schema = 
      TupleSchema::createTupleSchema(columnTypes,
				     columnLengths,
				     columnAllowNull,
				     false); //allowInlinedStrings

    //create primary key scheme
    index.tupleSchema = schema;
    vector<int> pkey_column_indices;
    vector<ValueType> pkey_column_types;
    pkey_column_indices.push_back(0);
    pkey_column_indices.push_back(1);
    pkey_column_types.push_back(VALUE_TYPE_SMALLINT);
    pkey_column_types.push_back(VALUE_TYPE_TINYINT);
    TableIndexScheme pkey("index_pkey", //name
			  BALANCED_TREE_INDEX, //type
			  pkey_column_indices, //which columns are pkey
			  pkey_column_types, //types of each pkey column
			  true, //unique
			  true, //intsOnly
			  schema); //tupleSchema

    //include secondary indices 
    vector<TableIndexScheme> indexes;
    indexes.push_back(index);

    //create the table
    m_engine = new VoltDBEngine();
    m_exceptionBuffer = new char[4096];
    m_engine->setBuffers(NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
    m_engine->initialize(0, 0, 0, 0, "");
    table = dynamic_cast<PersistentTable*>
      (TableFactory::getPersistentTable(database_id,
					m_engine->getExecutorContext(),
					"test_table",
					schema, //tuple schema
					columnNames,
					pkey, //pkey index scheme
					indexes, //other(secondary) indices
					-1, //partitionColumn
					false, //exportEnabled
					false)); //exportOnly

    delete[] columnNames;

    //insert a bunch of tuples
    string col5_string;
    string col6_string;
    for (int16_t i = 1; i <= NUM_OF_TUPLES; ++i) {
      TableTuple &tuple = table->tempTuple();
      col5_string = rand_string();
      col6_string = rand_string();
      tuple.setNValue(0, ValueFactory::getSmallIntValue(i));
      tuple.setNValue(1, ValueFactory::getTinyIntValue((int8_t)(i % 100)));
      tuple.setNValue(2, ValueFactory::getIntegerValue((int32_t)(i % 3)));
      tuple.setNValue(3, ValueFactory::getBigIntValue((int64_t)(i * 11)));
      tuple.setNValue(4, ValueFactory::getBigIntValue((int64_t)(i + 20)));
      tuple.setNValue(5, ValueFactory::getStringValue(col5_string));
      tuple.setNValue(6, ValueFactory::getStringValue(col6_string));
      col5.push_back(col5_string);
      col6.push_back(col6_string);      
      assert(true == table->insertTuple(tuple));
      //delete[] tuple.address();
      /*
      if (i % 100 == 50) {
	std::cout << i << "\t" << col5_string << "\t" << col6_string << "\n";
      }
      */
    }
  }

protected:
  PersistentTable* table;
  char* m_exceptionBuffer;
  VoltDBEngine* m_engine;

  string rand_string() {
    static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    int len = rand() % 20;
    if (len == 0)
      len = rand() % 20;
    char* s = (char*)malloc(len+1);
    for (int i = 0; i < len; i++)
      s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    s[len] = 0;

    string rand_str = string(s, len+1);
    free(s);
    return rand_str;
  }

  ValueType type_lookup (int i) {
    if (i == 0)
      return VALUE_TYPE_SMALLINT;
    else if (i == 1)
      return VALUE_TYPE_TINYINT;
    else if (i == 2)
      return VALUE_TYPE_INTEGER;
    else if (i == 3)
      return VALUE_TYPE_BIGINT;
    else if (i == 4)
      return VALUE_TYPE_BIGINT;
    else if (i == 5)
      return VALUE_TYPE_VARCHAR;
    else if (i == 6)
      return VALUE_TYPE_VARCHAR;
    else
      return VALUE_TYPE_BIGINT;
  }

   int32_t len_lookup (int i) {
     if (i == 0)
       return NValue::getTupleStorageSize(VALUE_TYPE_SMALLINT);
     else if (i == 1)
       return NValue::getTupleStorageSize(VALUE_TYPE_TINYINT);
     else if (i == 2)
       return NValue::getTupleStorageSize(VALUE_TYPE_INTEGER);
     else if (i == 3)
       return NValue::getTupleStorageSize(VALUE_TYPE_BIGINT);
     else if (i == 4)
       return NValue::getTupleStorageSize(VALUE_TYPE_BIGINT);
     else if (i == 5)
       return STRING_MAX_LENGTH;
     else if (i == 6)
       return STRING_MAX_LENGTH;
     else
       return NValue::getTupleStorageSize(VALUE_TYPE_BIGINT);
   }

  inline void setColumnIndices (vector<int> &column_indices,
				vector<ValueType> &column_types,
				int key_col_num,
				int* key_col_orders) {
    for (int i = 0; i < key_col_num; i++) {
      int j = 0;
      while (key_col_orders[j] != i)
	j++;
      column_indices.push_back(j);
      column_types.push_back(type_lookup(j));
    }
      
  }

  inline void setKeyColumn(vector<ValueType> &keyColTypes,
			   vector<int32_t> &keyColLen,
			   int key_col_num,
			   int* key_col_orders) {
    for (int i = 0; i < key_col_num; i++) {
      int j = 0;
      while (key_col_orders[j] != i)
	j++;
      keyColTypes.push_back(type_lookup(j));
      keyColLen.push_back(len_lookup(j));
    }
  }

  inline void setSearchKey (TableTuple &sk, int* key_col_orders, int i) {
    sk.move(new char[sk.tupleLength()]);
    if (key_col_orders[0] >= 0)
      sk.setNValue(key_col_orders[0], ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    if (key_col_orders[1] >= 0)
      sk.setNValue(key_col_orders[1], ValueFactory::getTinyIntValue(static_cast<int8_t>(i % 100)));
    if (key_col_orders[2] >= 0)
      sk.setNValue(key_col_orders[2], ValueFactory::getIntegerValue(static_cast<int32_t>(i % 3)));
    if (key_col_orders[3] >= 0)
      sk.setNValue(key_col_orders[3], ValueFactory::getBigIntValue(static_cast<int64_t>(i * 11)));
    if (key_col_orders[4] >= 0)
      sk.setNValue(key_col_orders[4], ValueFactory::getBigIntValue(static_cast<int64_t>(i + 20)));
    if (key_col_orders[5] >= 0) {
      if (i <= NUM_OF_TUPLES)
	sk.setNValue(key_col_orders[5], ValueFactory::getStringValue(col5[i-1]));
      else
	sk.setNValue(key_col_orders[5], ValueFactory::getStringValue(rand_string()));
    }
    if (key_col_orders[6] >= 0) {
      if (i <= NUM_OF_TUPLES)
	sk.setNValue(key_col_orders[6], ValueFactory::getStringValue(col6[i-1]));
      else
	sk.setNValue(key_col_orders[6], ValueFactory::getStringValue(rand_string()));
    }
  }

  inline void setSearchKey (TableTuple &sk, int* key_col_orders,
			    int16_t i, int8_t i_100, int32_t i_3, int64_t i_11, int64_t i_20,
			    string str5, string str6) {
    sk.move(new char[sk.tupleLength()]);
    if (key_col_orders[0] >= 0)
      sk.setNValue(key_col_orders[0], ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    if (key_col_orders[1] >= 0)
      sk.setNValue(key_col_orders[1], ValueFactory::getTinyIntValue(static_cast<int8_t>(i_100)));
    if (key_col_orders[2] >= 0)
      sk.setNValue(key_col_orders[2], ValueFactory::getIntegerValue(static_cast<int32_t>(i_3)));
    if (key_col_orders[3] >= 0)
      sk.setNValue(key_col_orders[3], ValueFactory::getBigIntValue(static_cast<int64_t>(i_11)));
    if (key_col_orders[4] >= 0)
      sk.setNValue(key_col_orders[4], ValueFactory::getBigIntValue(static_cast<int64_t>(i_20)));
    if (key_col_orders[5] >= 0)
      sk.setNValue(key_col_orders[5], ValueFactory::getStringValue(str5));
    if (key_col_orders[6] >= 0)
      sk.setNValue(key_col_orders[6], ValueFactory::getStringValue(str6));
  }

  inline void setTupleKey (TableTuple &st, int i) {
    st.move(new char[st.tupleLength()]);
    st.setNValue(0, ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    st.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(i % 100)));
    st.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(i % 3)));
    st.setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(i * 11)));
    st.setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(i + 20)));
    st.setNValue(5, ValueFactory::getStringValue(col5[i-1]));
    st.setNValue(6, ValueFactory::getStringValue(col6[i-1]));
  }

  inline void setTupleKey_randString (TableTuple &st, int i) {
    st.move(new char[st.tupleLength()]);
    st.setNValue(0, ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    st.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(i % 100)));
    st.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(i % 3)));
    st.setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(i * 11)));
    st.setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(i + 20)));
    st.setNValue(5, ValueFactory::getStringValue(rand_string()));
    st.setNValue(6, ValueFactory::getStringValue(rand_string()));
  }

  inline void updateTuple (TableTuple &tuple, int i) {
    tuple.setNValue(0, ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    tuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(i % 100)));
    tuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(i % 3)));
    tuple.setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(i * 11)));
    tuple.setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(i + 20)));
    tuple.setNValue(5, ValueFactory::getStringValue(col5[i-1]));
    tuple.setNValue(6, ValueFactory::getStringValue(col6[i-1]));
  }

  inline void updateTuple_randString (TableTuple &tuple, int i) {
    tuple.setNValue(0, ValueFactory::getSmallIntValue(static_cast<int16_t>(i)));
    tuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(i % 100)));
    tuple.setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(i % 3)));
    tuple.setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(i * 11)));
    tuple.setNValue(4, ValueFactory::getBigIntValue(static_cast<int64_t>(i + 20)));
    tuple.setNValue(5, ValueFactory::getStringValue(rand_string()));
    tuple.setNValue(6, ValueFactory::getStringValue(rand_string()));
  }
  
  inline void tupleContentTest(TableTuple &tuple, int16_t i) {
    EXPECT_TRUE(ValueFactory::getSmallIntValue(i)
		.op_equals(tuple.getNValue(0)).isTrue());
    EXPECT_TRUE(ValueFactory::getTinyIntValue((int8_t)(i % 100))
		.op_equals(tuple.getNValue(1)).isTrue());
    EXPECT_TRUE(ValueFactory::getIntegerValue(i % 3)
		.op_equals(tuple.getNValue(2)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(i * 11).
		op_equals(tuple.getNValue(3)).isTrue());
    EXPECT_TRUE(ValueFactory::getBigIntValue(i + 20).
		op_equals(tuple.getNValue(4)).isTrue());
    EXPECT_TRUE(ValueFactory::getStringValue(col5[i-1]).
		compare(tuple.getNValue(5)) == 0);
    EXPECT_TRUE(ValueFactory::getStringValue(col6[i-1]).
		compare(tuple.getNValue(6)) == 0);  
  }

  inline void tupleContentTest_debug(TableTuple &tuple, int16_t i) {
    if (!ValueFactory::getSmallIntValue(i).op_equals(tuple.getNValue(0)).isTrue()) {
      std::cout << "ERROR: i = " << i << "\n";
      std::cout << "\t" << tuple.debugNoHeader() << "\n";
    }
  }

  inline int16_t nextKey_pk_intsUO(int16_t i) {
    int16_t pk;
    if (i > (NUM_OF_TUPLES - 100))
      pk = (int16_t)((i % 100) + 1);
    else
      pk = (int16_t)(i + 100);
    return pk;
  }

  inline int16_t nextKey_pk_compositeUO(int16_t i) {
    vector<string> col5_i;
    vector<string> col6_i;
    for (int j = (i % 100); j <= NUM_OF_TUPLES; j += 100)
      if (j > 0)
	col5_i.push_back(col5[j-1]);
    std::sort(col5_i.begin(), col5_i.end());

    int idx = 0;
    for (int k = 0; k < col5_i.size(); k++)
      if (col5_i[k].compare(col5[i-1]) == 0) {
	idx = k + 1;
	break;
      }
    EXPECT_TRUE(idx > 0);

    if (idx == col5_i.size()) {
      col5_i.clear();
      for (int j = (i % 100 + 1); j <= NUM_OF_TUPLES; j += 100)
	col5_i.push_back(col5[j-1]);
      std::sort(col5_i.begin(), col5_i.end());
      for (int j = (i % 100 + 1); j <= NUM_OF_TUPLES; j += 100)
	if (col5_i[0].compare(col5[j-1]) == 0)
	  return (int16_t)j;
    }

    if (col5_i[idx].compare(col5_i[idx-1]) != 0) {
      for (int j = (i % 100); j <= NUM_OF_TUPLES; j += 100)
	if (j > 0)
	  if (col5_i[idx].compare(col5[j-1]) == 0)
	    return (int16_t)j;
    }
    else {
      //TODO
      return (int16_t)(-1);
    }
    return (int16_t)(-1);
  }

  inline int16_t nextKey_pk_intsMO(int16_t i) {
    int16_t pk;
    if (i % 3 == 2) {
      pk = (int16_t)((i % 100) + 1);
      while (pk % 3 != 0)
	pk = (int16_t)(pk + 100);
    }
    else
      pk = (int16_t)((i + 100) - ((i + 100)/300 * 300));
    while (pk <= NUM_OF_TUPLES)
      pk = (int16_t)(pk + 300);
    pk = (int16_t)(pk - 300);
    return pk;
  }

  inline int16_t nextValue_pk_intsMO(int16_t i) {
    int16_t pk;
    if (i > 300)
      pk = (int16_t)(i - 300);
    else { 
      if (i % 3 == 2) {
	pk = (int16_t)((i % 100) + 1);
	while (pk % 3 != 0)
	  pk = (int16_t)(pk + 100);
      }
      else
	pk = (int16_t)((i + 100) - ((i + 100)/300 * 300));
      while (pk < NUM_OF_TUPLES)
	pk = (int16_t)(pk + 300);
      pk = (int16_t)(pk - 300);
    }
    return pk;
  }

  inline int16_t getRealNextKey_pk(int16_t i, TestCases tc) {
    int16_t pk;
    switch(tc) {
    case INTS_UO:
      pk = nextKey_pk_intsUO(i);
      break;
    case COMPOSITE_UO:
      pk = nextKey_pk_compositeUO(i);
      break;
    case INTS_MO:
      pk = nextKey_pk_intsMO(i);
      break;
    default:
      pk = (int16_t)(-1);
    }
    return pk;
  }

  inline int16_t getRealNextValue_pk(int16_t i, TestCases tc) {
    int16_t pk;
    switch(tc) {
    case INTS_UO:
      pk = nextKey_pk_intsUO(i);
      break;
    case COMPOSITE_UO:
      pk = nextKey_pk_compositeUO(i);
      break;
    case INTS_MO:
      pk = nextValue_pk_intsMO(i);
      break;
    default:
      pk = (int16_t)(-1);
    }
    return pk;
  }

  inline int16_t greaterOrEqual_pk_compositeUO(int8_t i_100, string str5, string str6) {
    vector<string> col5_i;
    vector<string> col6_i;
    for (int j = i_100; j <= NUM_OF_TUPLES; j += 100)
      if (j > 0)
	col5_i.push_back(col5[j-1]);
    std::sort(col5_i.begin(), col5_i.end());

    int idx = (int)col5_i.size();
    for (int k = 0; k < col5_i.size(); k++)
      if (col5_i[k].compare(str5) >= 0) {
	idx = k;
	break;
      }

    if (idx == col5_i.size()) {
      col5_i.clear();
      for (int j = i_100 + 1; j <= NUM_OF_TUPLES; j += 100)
	col5_i.push_back(col5[j-1]);
      std::sort(col5_i.begin(), col5_i.end());
      idx = 0;
      for (int j = i_100 + 1; j <= NUM_OF_TUPLES; j += 100)
	if (col5_i[0].compare(col5[j-1]) == 0)
	  return (int16_t)j;
    }

    //if (col5_i[idx].compare(col5_i[idx-1]) != 0) {
    for (int j = i_100; j <= NUM_OF_TUPLES; j += 100)
      if (j > 0)
	if (col5_i[idx].compare(col5[j-1]) == 0)
	  return (int16_t)j;
      //}
      //else {
      //TODO
      //return (int16_t)(-1);
      //}
    return (int16_t)(-1);
  }

  inline int16_t greater_pk_compositeUO(int8_t i_100, string str5, string str6) {
    vector<string> col5_i;
    vector<string> col6_i;
    for (int j = i_100; j <= NUM_OF_TUPLES; j += 100)
      if (j > 0)
	col5_i.push_back(col5[j-1]);
    std::sort(col5_i.begin(), col5_i.end());

    int idx = (int)col5_i.size();
    for (int k = 0; k < col5_i.size(); k++)
      if (col5_i[k].compare(str5) > 0) {
	idx = k;
	break;
      }

    if (idx == col5_i.size()) {
      col5_i.clear();
      for (int j = i_100 + 1; j <= NUM_OF_TUPLES; j += 100)
	col5_i.push_back(col5[j-1]);
      std::sort(col5_i.begin(), col5_i.end());
      idx = 0;
      for (int j = i_100 + 1; j <= NUM_OF_TUPLES; j += 100)
	if (col5_i[0].compare(col5[j-1]) == 0)
	  return (int16_t)j;
    }

    //if (col5_i[idx].compare(col5_i[idx-1]) != 0) {
    //std::cout << "idx = " << idx << "\n";
    for (int j = i_100; j <= NUM_OF_TUPLES; j += 100) {
      if (j > 0)
	if (col5_i[idx].compare(col5[j-1]) == 0)
	  return (int16_t)j;
      //}
      //else {
      //TODO
      //return (int16_t)(-1);
      //}
    }
    return (int16_t)(-1);
  }

  inline int16_t moveToKeyOrGreater_pk_intsUO(int16_t i, int8_t i_100) {
    int16_t pk;
    if (i < 0) {
      if (i_100 < 0)
	pk = 1;
      else if (i_100 == 0)
	pk = 100;
      else
	pk = i_100;
    }
    else {
      if (i % 100 > i_100)
	pk = (int16_t)(i - (i % 100) + 100 + i_100);
      else
	pk = (int16_t)(i - (i % 100) + i_100);
    }
    return pk;
  }

  inline int16_t moveToKeyOrGreater_pk_compositeUO(int8_t i_100, string str5, string str6) {
    return greaterOrEqual_pk_compositeUO(i_100, str5, str6);
  }

  inline int16_t moveToKeyOrGreater_pk_intsMO(int8_t i_100, int32_t i_3) {
    int16_t pk;
    if (i_100 <= 0)
      pk = 1;
    else {
      if (i_3 < 0) {
	i_3 = 0;
	pk = i_100;
      }
      else if (i_3 > 2) {
	i_3 = 0;
	pk = (int16_t)(i_100 + 1);
      }
      else {
	pk = i_100;
      }
      while ((pk % 3) != i_3)
	pk = (int16_t)(pk + 100);
      while (pk < NUM_OF_TUPLES)
	pk = (int16_t)(pk + 300);
      pk = (int16_t)(pk - 300);
    }
    return pk;
  }

  inline int16_t getRealMoveToKeyOrGreater_pk(int16_t i, int8_t i_100, int32_t i_3, 
					      int64_t i_11, int64_t i_20,
					      string str5, string str6, TestCases tc) {
    int16_t pk;
    switch(tc) {
    case INTS_UO:
      pk = moveToKeyOrGreater_pk_intsUO(i, i_100);
      break;
    case COMPOSITE_UO:
      pk = moveToKeyOrGreater_pk_compositeUO(i_100, str5, str6);
      break;
    case INTS_MO:
      pk = moveToKeyOrGreater_pk_intsMO(i_100, i_3);
      break;
    default:
      pk = (int16_t)(-1);
    }
    return pk;
  }

  inline int16_t moveToGreaterThanKey_pk_intsUO(int16_t i, int8_t i_100) {
    int16_t pk;
    if (i < 0) {
      if (i_100 < 0)
	pk = 1;
      else if (i_100 == 0)
	pk = 100;
      else
	pk = i_100;
    }
    else {
      if (i % 100 >= i_100) {
	pk = (int16_t)(i - (i % 100) + 100 + i_100);
	if (pk > NUM_OF_TUPLES)
	  pk = (int16_t)((pk + 1) % 100);
	if (pk == 0)
	  pk = 100;
      }
      else
	pk = (int16_t)(i - (i % 100) + i_100);
    }
    return pk;
  }

  inline int16_t moveToGreaterThanKey_pk_compositeUO(int8_t i_100, string str5, string str6) {
    return greater_pk_compositeUO(i_100, str5, str6);
  }

  inline int16_t moveToGreaterThanKey_pk_intsMO(int8_t i_100, int32_t i_3) {
    int16_t pk;
    if (i_100 < 0)
      pk = 1;
    else {
      if (i_3 < 0) {
	i_3 = 0;
	pk = i_100;
      }
      else if (i_3 >= 2) {
	i_3 = 0;
	pk = (int16_t)(i_100 + 1);
      }
      else {
	i_3 = (int32_t)(i_3 + 1);
	pk = i_100;
      }
      while ((pk % 3) != i_3)
	pk = (int16_t)(pk + 100);
      while (pk <= NUM_OF_TUPLES)
	pk = (int16_t)(pk + 300);
      pk = (int16_t)(pk - 300);
    }
    return pk;
  }

  inline int16_t getRealMoveToGreaterThanKey_pk(int16_t i, int8_t i_100, int32_t i_3, 
					      int64_t i_11, int64_t i_20,
					      string str5, string str6, TestCases tc) {
    int16_t pk;
    switch(tc) {
    case INTS_UO:
      pk = moveToGreaterThanKey_pk_intsUO(i, i_100);
      break;
    case COMPOSITE_UO:
      pk = moveToGreaterThanKey_pk_compositeUO(i_100, str5, str6);
      break;
    case INTS_MO:
      pk = moveToGreaterThanKey_pk_intsMO(i_100, i_3);
      break;
    default:
      pk = (int16_t)(-1);
    }
    return pk;
  }

  inline void moveToKeyTest(TableIndex* index, TupleSchema* keySchema, 
			    const TupleSchema* schema, 
			    int* key_col_orders, int16_t i, TestCases tc) {
    TableTuple searchkey(keySchema);
    TableTuple tuple(schema);
    //setSearchKey(searchkey, i);
    setSearchKey(searchkey, key_col_orders, i);
    //std::cout << searchkey.debugNoHeader() << "\n";
    EXPECT_TRUE(index->moveToKey(&searchkey));
    int count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest_debug(tuple, i);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES - (i % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest_debug(tuple, (int16_t)((i % 300) + 300 * (expect_num_items - count)));
      }
      if (i % 300 == 0)
	expect_num_items--;
      EXPECT_EQ(expect_num_items, count);
      if (expect_num_items != count) {
	std::cout << "ERROR: i = " << i << "\n";
	std::cout << "expect_num_items = " << expect_num_items << "\n";
	std::cout << "count = " << count << "\n";
      }
    }
    delete[] searchkey.address();
    delete[] tuple.address();
  }

  inline void moveToTuple_exist_advanceToNextKeyTest(TableIndex* index, const TupleSchema* schema, 
						     int16_t i, TestCases tc) {
    TableTuple searchtuple(schema);
    TableTuple tuple(schema);
    setTupleKey(searchtuple, i);
    EXPECT_TRUE(index->exists(&searchtuple));

    EXPECT_TRUE(index->moveToTuple(&searchtuple));
    int count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, i);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES - (i % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, (int16_t)((i % 300) + 300 * (expect_num_items - count)));
      }
      if (i % 300 == 0)
	expect_num_items--;
      EXPECT_EQ(expect_num_items, count);
    }

    if ((tc == INTS_UO) || (tc == COMPOSITE_UO) || (tc == INTS_MO) || (tc == COMPOSITE_MO)) {
      EXPECT_TRUE(index->advanceToNextKey());
      int16_t pk = getRealNextKey_pk(i, tc);

      count = 0;
      if ((tc == INTS_UO) || (tc == COMPOSITE_UO)) {
	while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	  count++;
	  tupleContentTest_debug(tuple, pk);
	}
	EXPECT_EQ(1, count);
      }
      else {
	int expect_num_items = pk/300 + 1;
	while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	  count++;
	  tupleContentTest_debug(tuple, (int16_t)(pk - 300 * (count - 1)));
	}
	if (pk % 300 == 0)
	  expect_num_items--;
	EXPECT_EQ(expect_num_items, count);
      }
    }

    delete[] searchtuple.address();
  }

  inline void moveToKeyOrGreaterTest(TableIndex* index, TupleSchema* keySchema, 
				     const TupleSchema* schema, int* key_col_orders,
				     int16_t i, int8_t i_100, int32_t i_3, int64_t i_11, int64_t i_20,
				     string str5, string str6, TestCases tc) {
    TableTuple searchkey(keySchema);
    TableTuple tuple(schema);
    setSearchKey(searchkey, key_col_orders,
		 i, i_100, i_3, i_11, i_20,
		 str5, str6);
    //std::cout << "searchkey = " << searchkey.debugNoHeader() << "\n";
    index->moveToKeyOrGreater(&searchkey);
    int16_t pk = getRealMoveToKeyOrGreater_pk(i, i_100, i_3, i_11, i_20,
					      str5, str6, tc);
    //std::cout << "pk = " << pk << "\n";
    int count = 0;
    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);

    //pk = getRealNextKey_pk(pk, tc);
    pk = getRealNextValue_pk(pk, tc);
    //std::cout << "pk = " << pk << "\n";
    count = 0;

    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);

    /*
    //pk = getRealNextKey_pk(pk, tc);
    pk = getRealNextValue_pk(pk, tc);
    //std::cout << "pk = " << pk << "\n";
    count = 0;

    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);

    //pk = getRealNextKey_pk(pk, tc);
    pk = getRealNextValue_pk(pk, tc);
    //std::cout << "pk = " << pk << "\n";
    count = 0;

    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);
    */
    delete[] searchkey.address();
  }


  inline void moveToGreaterThanKeyTest(TableIndex* index, TupleSchema* keySchema, 
				       const TupleSchema* schema, int* key_col_orders,
				       int16_t i, int8_t i_100, int32_t i_3, int64_t i_11, int64_t i_20,
				       string str5, string str6, TestCases tc) {
    TableTuple searchkey(keySchema);
    TableTuple tuple(schema);
    setSearchKey(searchkey, key_col_orders,
		 i, i_100, i_3, i_11, i_20,
		 str5, str6);
    index->moveToGreaterThanKey(&searchkey);
    int16_t pk = getRealMoveToGreaterThanKey_pk(i, i_100, i_3, i_11, i_20,
						str5, str6, tc);
    //std::cout << "pk = " << pk << "\n";
    int count = 0;
    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);
    /*    
    //pk = getRealNextKey_pk(pk, tc);
    pk = getRealNextValue_pk(pk, tc);
    //std::cout << "next pk = " << pk << "\n";
    count = 0;
    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      //std::cout << tuple.debugNoHeader() << "\n";
      tupleContentTest_debug(tuple, pk);
    }
    EXPECT_EQ(1, count);
    */
    delete[] searchkey.address();
  }

  inline void deleteEntry_addEntryTest(TableIndex* index, const TupleSchema* schema, 
				       int16_t i, TestCases tc) {
    TableTuple searchtuple(schema);
    TableTuple tuple(schema);
    TableTuple deletetuple(schema);
    setTupleKey(searchtuple, i);
    EXPECT_TRUE(index->exists(&searchtuple));
    
    EXPECT_TRUE(index->moveToTuple(&searchtuple));
    int count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, i);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES - (i % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, (int16_t)((i % 300) + 300 * (expect_num_items - count)));
      }
      if (i % 300 == 0)
	expect_num_items--;
      EXPECT_EQ(expect_num_items, count);
    }
    //std::cout << searchtuple.debugNoHeader() << "\n";

    EXPECT_TRUE(index->moveToTuple(&searchtuple));
    deletetuple = index->nextValueAtKey();
    EXPECT_TRUE(index->deleteEntry(&deletetuple));
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH))
      EXPECT_TRUE(!index->exists(&searchtuple));
    else {
      EXPECT_TRUE(index->moveToTuple(&searchtuple));
      int expect_num_items = (NUM_OF_TUPLES - (i % 300))/300;
      if (i % 300 == 0)
	expect_num_items--;
      count = 0;
      while (!(tuple = index->nextValueAtKey()).isNullTuple())
	count++;
      EXPECT_EQ(expect_num_items, count);
    }      

    //EXPECT_TRUE(index->addEntry(&searchtuple));
    EXPECT_TRUE(index->addEntry(&deletetuple));
    
    EXPECT_TRUE(index->moveToTuple(&searchtuple));
    count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      EXPECT_TRUE(index->exists(&searchtuple));
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	//std::cout << i << "\t" << tuple.debugNoHeader() << "\n";
	tupleContentTest(tuple, i);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES - (i % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	//tupleContentTest(tuple, (int16_t)(i + 300 * (expect_num_items - count)));
      }
      if (i % 300 == 0)
	expect_num_items--;
      EXPECT_EQ(expect_num_items, count);
    }

    delete[] searchtuple.address();
  }

  inline void replaceEntryTest(TableIndex* index, const TupleSchema* schema, 
			       int16_t i, int16_t j, TestCases tc) {
    TableTuple searchtuple(schema);
    TableTuple tuple(schema);
    setTupleKey(searchtuple, i);
    EXPECT_TRUE(index->exists(&searchtuple));
    
    string col5_str = rand_string();
    string col6_str = rand_string();
    col5.push_back(col5_str);
    col6.push_back(col6_str);
    TableTuple deletetuple(schema);
    TableTuple replacetuple(schema);
    setTupleKey(replacetuple, j);
    EXPECT_TRUE(j > NUM_OF_TUPLES);

    EXPECT_TRUE(index->moveToTuple(&searchtuple));
    deletetuple = index->nextValueAtKey();
    EXPECT_TRUE(index->replaceEntry(&deletetuple, &replacetuple));    
    EXPECT_TRUE(index->exists(&replacetuple));
    
    EXPECT_TRUE(index->moveToTuple(&replacetuple));
    int count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      EXPECT_TRUE(!index->exists(&deletetuple));
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, j);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES + 1 - (j % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, (int16_t)((j % 300) + 300 * (expect_num_items - count)));
      }
      //std::cout << expect_num_items << "\t" << count << "\n";
      EXPECT_EQ(expect_num_items, count);
    }

    //reverse the update
    if (!index->replaceEntry(&replacetuple, &deletetuple)) {
      std::cout << "i = " << i << "\n";
    }
    //EXPECT_TRUE(index->replaceEntry(&replacetuple, &deletetuple));
    EXPECT_TRUE(index->exists(&deletetuple));
    
    EXPECT_TRUE(index->moveToTuple(&deletetuple));
    count = 0;
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      EXPECT_TRUE(!index->exists(&replacetuple));
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, i);
      }
      EXPECT_EQ(1, count);
    }
    else {
      int expect_num_items = (NUM_OF_TUPLES + 1 - (i % 300))/300 + 1;
      while (!(tuple = index->nextValueAtKey()).isNullTuple()) {
	count++;
	tupleContentTest(tuple, (int16_t)((i % 300) + 300 * (expect_num_items - count)));
      }
      if (i % 300 == 0)
	expect_num_items--;
      //std::cout << expect_num_items << "\t" << count << "\n";
      EXPECT_EQ(expect_num_items, count);
    }
    delete[] searchtuple.address();
    delete[] replacetuple.address();
  }

  inline void insertFailureTest(TableIndex* index, const TupleSchema* schema, 
				int16_t i) {
    TableTuple inserttuple(schema);
    setTupleKey(inserttuple, i);
    EXPECT_TRUE(index->exists(&inserttuple));
    EXPECT_TRUE(!index->addEntry(&inserttuple));
    delete[] inserttuple.address();
  }

  inline void deleteFailureTest(TableIndex* index, const TupleSchema* schema, 
				int16_t i, TestCases tc) {
    TableTuple deletetuple(schema);
    setTupleKey_randString(deletetuple, i);
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH))
      EXPECT_TRUE(!index->exists(&deletetuple));
    EXPECT_TRUE(!index->deleteEntry(&deletetuple));
    delete[] deletetuple.address();
  }

  inline void replaceFailureTest_bothMissing(TableIndex* index, const TupleSchema* schema, 
					     int16_t i_missing, int16_t j_missing,
					     TestCases tc) {
    TableTuple oldtuple(schema);
    TableTuple newtuple(schema);
    setTupleKey_randString(oldtuple, i_missing);
    setTupleKey_randString(newtuple, j_missing);
    if ((tc == INTS_UO) || (tc == INTS_UH) || (tc == COMPOSITE_UO) || (tc == COMPOSITE_UH)) {
      EXPECT_TRUE(!index->exists(&oldtuple));
      EXPECT_TRUE(!index->exists(&newtuple));
    }
    EXPECT_TRUE(!index->replaceEntry(&oldtuple, &newtuple));    
    delete[] oldtuple.address();
    delete[] newtuple.address();
  }

  inline void replaceFailureTest_bothExist(TableIndex* index, const TupleSchema* schema, 
					   int16_t i_exist, int16_t j_exist) {
    TableTuple oldtuple(schema);
    TableTuple newtuple(schema);
    setTupleKey(oldtuple, i_exist);
    EXPECT_TRUE(index->exists(&oldtuple));
    setTupleKey(newtuple, j_exist);
    EXPECT_TRUE(index->exists(&newtuple));
    EXPECT_TRUE(!index->replaceEntry(&oldtuple, &newtuple));
    delete[] oldtuple.address();
    delete[] newtuple.address();
  }

  inline void replaceFailureTest(TableIndex* index, const TupleSchema* schema, 
				 int16_t i_missing, int16_t j_missing,
				 int16_t i_exist, int16_t j_exist) {
    TableTuple oldtuple(schema);
    setTupleKey_randString(oldtuple, i_missing);
    EXPECT_TRUE(!index->exists(&oldtuple));
    TableTuple newtuple(schema);
    setTupleKey_randString(newtuple, j_missing);
    EXPECT_TRUE(!index->exists(&newtuple));
    EXPECT_TRUE(!index->replaceEntry(&oldtuple, &newtuple));
    
    setTupleKey(oldtuple, i_exist);
    EXPECT_TRUE(index->exists(&oldtuple));
    setTupleKey(newtuple, j_exist);
    EXPECT_TRUE(index->exists(&newtuple));
    EXPECT_TRUE(!index->replaceEntry(&oldtuple, &newtuple));
    delete[] oldtuple.address();
    delete[] newtuple.address();
  }

  inline void moveToKeyFailure(TableIndex* index, TupleSchema* keySchema,
			       int* key_col_orders, int i) {
    TableTuple searchkey(keySchema);
    setSearchKey(searchkey, key_col_orders, i);
    EXPECT_TRUE(!index->moveToKey(&searchkey));
    delete[] searchkey.address();
  }

  inline void moveToTupleFailure(TableIndex* index, const TupleSchema* schema,
				 int i) {
    TableTuple searchtuple(schema);
    setTupleKey_randString(searchtuple, i);
    EXPECT_TRUE(!index->moveToTuple(&searchtuple));
    delete[] searchtuple.address();
  }

  inline void nextValueOutOfBoundTest(TableIndex* index, 
				      TupleSchema* keySchema,
				      const TupleSchema* schema,
				      int* key_col_orders) {
    TableTuple searchkey(keySchema);
    TableTuple tuple(schema);
    setSearchKey(searchkey, key_col_orders, NUM_OF_TUPLES-1);
    index->moveToKeyOrGreater(&searchkey);
    int count = 0;
    if (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
      tupleContentTest(tuple, NUM_OF_TUPLES-1);
    }
    EXPECT_EQ(1, count);
    EXPECT_TRUE((tuple = index->nextValue()).isNullTuple());
    
    setSearchKey(searchkey, key_col_orders, NUM_OF_TUPLES-1);
    index->moveToGreaterThanKey(&searchkey);
    EXPECT_TRUE((tuple = index->nextValue()).isNullTuple());
    
    setSearchKey(searchkey, key_col_orders, NUM_OF_TUPLES, 99, NUM_OF_TUPLES % 3, NUM_OF_TUPLES * 11,
		 NUM_OF_TUPLES + 20, rand_string(), rand_string());
    index->moveToKeyOrGreater(&searchkey);
    EXPECT_TRUE((tuple = index->nextValue()).isNullTuple());
    
    index->moveToGreaterThanKey(&searchkey);
    EXPECT_TRUE((tuple = index->nextValue()).isNullTuple());
    delete[] searchkey.address();
  }

  inline void selectStarTest(TableIndex* index, 
			     TupleSchema* keySchema,
			     const TupleSchema* schema,
			     int* key_col_orders) {
    //TableTuple searchkey(keySchema);
    TableTuple tuple(schema);
    //setSearchKey(searchkey, key_col_orders, 100);
    //index->moveToKeyOrGreater(&searchkey);
    index->moveToEnd(true);
    int count = 0;
    while (!(tuple = index->nextValue()).isNullTuple()) {
      count++;
    }
    if (NUM_OF_TUPLES != count)
      std::cout << NUM_OF_TUPLES << "\t" << count << "\n";
    EXPECT_EQ(NUM_OF_TUPLES, count);
    //delete[] searchkey.address();
  }
};


TEST_F(IndexMoreTest, intsUO) {
  //create the index under test
  vector<int> intsUO_column_indices;
  vector<ValueType> intsUO_column_types;
  int key_col_num = 4;
  //tiny,small,big,int
  int key_col_orders[NUM_OF_TUPLES] = {1, 0, 3, 2, -1, -1, -1};
  setColumnIndices(intsUO_column_indices, intsUO_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("intsUO",
			BALANCED_TREE_INDEX,
			intsUO_column_indices,
			intsUO_column_types,
			true, //unique
			true, //intOnly
			NULL));
  TableIndex* index = table->index("intsUO");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);

  //NORMAL TESTS==========================================================================
  //point query tests
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, INTS_UO);

  //range query tests
  //for (int8_t i = 0; i < (NUM_OF_TUPLES % 100); i++)
  for (int8_t i = 0; i < 100; i++)
    moveToKeyOrGreaterTest(index, keySchema, table->schema(), key_col_orders, 
			   -1, i, -1, -1, -1, rand_string(), rand_string(), INTS_UO);
  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if (i != 899)
      moveToGreaterThanKeyTest(index, keySchema, table->schema(), key_col_orders,
			       i, (int8_t)(i%100), i%3, i*11, i+20, rand_string(), rand_string(), INTS_UO);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, INTS_UO);
  //for (int16_t i = 1; i <= 10; i++)
  //deleteEntry_addEntryTest(index, table->schema(), i, INTS_UO);

  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  //update test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    replaceEntryTest(index, table->schema(), i, 1001, INTS_UO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);


  //FAILURE TESTS=========================================================================
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    insertFailureTest(index, table->schema(), i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    deleteFailureTest(index, table->schema(), i, INTS_UO);

  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), INTS_UO);
  //for (int16_t i = 1; i < NUM_OF_TUPLES; i++)
  //replaceFailureTest_bothExist(index, table->schema(), i, (int16_t)(i+1));
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToKeyFailure(index, keySchema, key_col_orders, i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToTupleFailure(index, table->schema(), i);

  //CORNER CASES=========================================================================
  //nextValue reaches the end
  nextValueOutOfBoundTest(index, keySchema, table->schema(), key_col_orders);

  selectStarTest(index, keySchema, table->schema(), key_col_orders);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}
/*
TEST_F(IndexMoreTest, intsUH) {
  //create the index under test
  vector<int> intsUH_column_indices;
  vector<ValueType> intsUH_column_types;
  int key_col_num = 4;
  //tiny,small,big,int
  int key_col_orders[NUM_OF_TUPLES] = {1, 0, 3, 2, -1, -1, -1};
  setColumnIndices(intsUH_column_indices, intsUH_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("intsUH",
			HASH_TABLE_INDEX,
			intsUH_column_indices,
			intsUH_column_types,
			true, //unique
			true, //intOnly
			NULL));
  TableIndex* index = table->index("intsUH");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);
  //NORMAL TESTS==========================================================================
  //point query tests
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UH);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, INTS_UH);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, INTS_UH);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UH);

  //update test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //for (int16_t i = 1; i <= 20; i++)
    replaceEntryTest(index, table->schema(), i, 1001, INTS_UH);

  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UH);
  
  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);


  //FAILURE TESTS=========================================================================
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    insertFailureTest(index, table->schema(), i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    deleteFailureTest(index, table->schema(), i, INTS_UH);

  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), INTS_UH);
  //for (int16_t i = 1; i < NUM_OF_TUPLES; i++)
  //replaceFailureTest_bothExist(index, table->schema(), i, (int16_t)(i+1));
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UH);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToKeyFailure(index, keySchema, key_col_orders, i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToTupleFailure(index, table->schema(), i);

  //CORNER CASES=========================================================================
  //nextValue reaches the end
  //nextValueOutOfBoundTest(index, keySchema, table->schema(), key_col_orders);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}
*/
/*
TEST_F(IndexMoreTest, compositeUO) {
  //rand_string();
  //create the index under test
  vector<int> compositeUO_column_indices;
  vector<ValueType> compositeUO_column_types;
  int key_col_num = 3;
  //tiny,varchar,varchar
  int key_col_orders[NUM_OF_TUPLES] = {-1, 0, -1, -1, -1, 1, 2};
  setColumnIndices(compositeUO_column_indices, compositeUO_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("compositeUO",
			BALANCED_TREE_INDEX,
			compositeUO_column_indices,
			compositeUO_column_types,
			true, //unique
			false, //intOnly
			NULL));
  TableIndex* index = table->index("compositeUO");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);

  //NORMAL TESTS==========================================================================
  //point query tests
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UO);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if ((i % 100) != 99)
      moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, COMPOSITE_UO);

  //range query tests
  for (int8_t i = 0; i < (NUM_OF_TUPLES % 100); i++)
    if ((i % 100) != 99)
      moveToKeyOrGreaterTest(index, keySchema, table->schema(), key_col_orders, 
			     -1, i, -1, -1, -1, rand_string(), rand_string(), COMPOSITE_UO);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if ((i % 100) != 99)
      moveToGreaterThanKeyTest(index, keySchema, table->schema(), key_col_orders,
			       i, (int8_t)(i%100), i%3, i*11, i+20, rand_string(), rand_string(), COMPOSITE_UO);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, COMPOSITE_UO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UO);

  //update test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    replaceEntryTest(index, table->schema(), i, 1001, INTS_UO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);


  //FAILURE TESTS=========================================================================
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    insertFailureTest(index, table->schema(), i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    deleteFailureTest(index, table->schema(), i, COMPOSITE_UO);

  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), COMPOSITE_UO);
  //for (int16_t i = 1; i < NUM_OF_TUPLES; i++)
  //replaceFailureTest_bothExist(index, table->schema(), i, (int16_t)(i+1));
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UO);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToKeyFailure(index, keySchema, key_col_orders, i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToTupleFailure(index, table->schema(), i);

  //CORNER CASES=========================================================================
  //nextValue reaches the end
  //nextValueOutOfBoundTest(index, keySchema, table->schema(), key_col_orders);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}
*/
/*
TEST_F(IndexMoreTest, compositeUH) {
  //create the index under test
  vector<int> compositeUH_column_indices;
  vector<ValueType> compositeUH_column_types;
  int key_col_num = 3;
  //tiny,varchar,varchar
  int key_col_orders[NUM_OF_TUPLES] = {-1, 0, -1, -1, -1, 1, 2};
  setColumnIndices(compositeUH_column_indices, compositeUH_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("compositeUH",
			HASH_TABLE_INDEX,
			compositeUH_column_indices,
			compositeUH_column_types,
			true, //unique
			false, //intOnly
			NULL));
  TableIndex* index = table->index("compositeUH");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);

  //NORMAL TESTS==========================================================================
  //point query tests
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UH);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if ((i % 100) != 99)
      moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, COMPOSITE_UH);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, COMPOSITE_UO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UO);

  //update test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    replaceEntryTest(index, table->schema(), i, 1001, INTS_UO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_UO);

  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);


  //FAILURE TESTS=========================================================================
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    insertFailureTest(index, table->schema(), i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    deleteFailureTest(index, table->schema(), i, COMPOSITE_UH);

  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), COMPOSITE_UH);
  //for (int16_t i = 1; i < NUM_OF_TUPLES; i++)
  //replaceFailureTest_bothExist(index, table->schema(), i, (int16_t)(i+1));
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, COMPOSITE_UH);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToKeyFailure(index, keySchema, key_col_orders, i);

  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    moveToTupleFailure(index, table->schema(), i);

  //CORNER CASES=========================================================================
  //nextValue reaches the end
  //nextValueOutOfBoundTest(index, keySchema, table->schema(), key_col_orders);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}
*/

TEST_F(IndexMoreTest, intsMO) {
  //create the index under test
  vector<int> intsMO_column_indices;
  vector<ValueType> intsMO_column_types;
  int key_col_num = 2;
  //tiny,int
  int key_col_orders[NUM_OF_TUPLES] = {-1, 0, 1, -1, -1, -1, -1};
  setColumnIndices(intsMO_column_indices, intsMO_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("intsMO",
			BALANCED_TREE_INDEX,
			intsMO_column_indices,
			intsMO_column_types,
			false, //unique
			true, //intOnly
			NULL));
  TableIndex* index = table->index("intsMO");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);

  //NORMAL TESTS==========================================================================
  //point query tests
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  for (int16_t i = 1; i <= 2; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MO);
  /*
  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if (i % 300 != 299)
      moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, INTS_MO);

  //range query tests
  for (int8_t i = 0; i < (NUM_OF_TUPLES % 100); i++)
    moveToKeyOrGreaterTest(index, keySchema, table->schema(), key_col_orders, 
			   -1, i, -1, -1, -1, rand_string(), rand_string(), INTS_MO);
  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    if (i % 300 != 299)
      moveToGreaterThanKeyTest(index, keySchema, table->schema(), key_col_orders,
			       i, (int8_t)(i%100), i%3, i*11, i+20, rand_string(), rand_string(), INTS_MO);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, INTS_MO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MO);

  //update test
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //if (i % 300 != 101)
  //replaceEntryTest(index, table->schema(), i, 1001, INTS_MO);
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MO);

  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);


  //FAILURE TESTS=========================================================================
  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  deleteFailureTest(index, table->schema(), i, INTS_MO);
  */
  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), INTS_MO);
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MO);

  selectStarTest(index, keySchema, table->schema(), key_col_orders);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}


TEST_F(IndexMoreTest, intsMH) {
  //create the index under test
  vector<int> intsMH_column_indices;
  vector<ValueType> intsMH_column_types;
  int key_col_num = 2;
  //tiny,int
  int key_col_orders[NUM_OF_TUPLES] = {-1, 0, 1, -1, -1, -1, -1};
  setColumnIndices(intsMH_column_indices, intsMH_column_types,
		   key_col_num, key_col_orders);

  init(TableIndexScheme("intsMH",
			HASH_TABLE_INDEX,
			intsMH_column_indices,
			intsMH_column_types,
			false, //unique
			true, //intOnly
			NULL));
  TableIndex* index = table->index("intsMH");
  EXPECT_EQ(true, index != NULL);

  //create the key schema
  vector<ValueType> keyColumnTypes;
  vector<int32_t> keyColumnLengths;
  setKeyColumn(keyColumnTypes, keyColumnLengths, 
	       key_col_num, key_col_orders);
  vector<bool> keyColumnAllowNull(2, true);
  TupleSchema* keySchema =
    TupleSchema::createTupleSchema(keyColumnTypes,
				   keyColumnLengths,
				   keyColumnAllowNull,
				   true);

  //NORMAL TESTS==========================================================================
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MH);

  for (int16_t i = 1; i < (NUM_OF_TUPLES - 1); i++)
    moveToTuple_exist_advanceToNextKeyTest(index, table->schema(), i, INTS_MH);

  //delete, insert back test
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    deleteEntry_addEntryTest(index, table->schema(), i, INTS_MO);
  for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
    moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MO);

  //update test
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //if (i % 300 != 101)
  //replaceEntryTest(index, table->schema(), i, 1001, INTS_MH);
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MH);

  //setEntryToNewAddress test

  //checkForIndexChange test
  //exact same implementation, no need to test for now

  //moveToEnd test

  //getSize test
  EXPECT_TRUE(index->getSize() == NUM_OF_TUPLES);

  //getMemoryEstimate test
  EXPECT_TRUE(index->getMemoryEstimate() > 0);

  //FAILURE TESTS=========================================================================
  for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
    deleteFailureTest(index, table->schema(), i, INTS_MH);

  //for (int16_t i = (int16_t)(NUM_OF_TUPLES + 1); i <= (NUM_OF_TUPLES + 100); i++)
  //replaceFailureTest_bothMissing(index, table->schema(), i, (int16_t)(i+1), INTS_MH);
  //for (int16_t i = 1; i <= NUM_OF_TUPLES; i++)
  //moveToKeyTest(index, keySchema, table->schema(), key_col_orders, i, INTS_MH);

  TupleSchema::freeTupleSchema(keySchema);
  col5.clear();
  col6.clear();
}

int main() {
  return TestSuite::globalInstance()->runAll();
}
