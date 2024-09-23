from collections import List
from db_package.set import Set
from collections.dict import Dict, _DictKeyIter, _DictValueIter, _DictEntryIter
from time import now




@value
struct Lookup[Orig: KeyElement]:
    var columnName: String
    var origToIndex: Dict[Orig, Int]
    var indexToOrig: Dict[Int, List[Int]]
    var rawVals: List[Orig]
    var nextIndex: Int

    fn __init__(inout self,columnName: String):
        self.columnName = columnName
        self.origToIndex = Dict[Orig,Int]() #key is the index to a list of original vals, value is the id to that value 
        self.indexToOrig = Dict[Int,List[Int]]() #key is the distinct id, value are the row indexes to that id
        self.rawVals = List[Orig]() #raw data u=in the original type
        self.nextIndex = 0 
    
    fn __init__(inout self,columnName: String, other: Self):
        self.columnName = columnName
        self.origToIndex = other.origToIndex
        self.indexToOrig = other.indexToOrig
        self.nextIndex = other.nextIndex
        self.rawVals = other.rawVals

    fn encode(inout self, origs: List[Orig]) raises:
        var val: Orig
        for ind in range(len(origs)):
            val = origs[ind]
            if self.origToIndex.find(val):
                var indexKey = self.origToIndex[val]
                self.indexToOrig[indexKey].append(ind)
            else:
                self.origToIndex[val] = self.nextIndex
                self.indexToOrig[self.nextIndex] = List(ind)
                self.nextIndex += 1

    fn decode(self, ints: List[Int]) raises -> List[Orig]:
        var retOrigs = List[Orig]()
        for i in ints:
            retOrigs.extend(self.decode(i[]))
        return retOrigs
    
    fn decode(self, ints: Int) raises -> List[Orig]:
        if ints not in self.indexToOrig:
            raise('int not in lookup')
        var retOrigs = List[Orig]()
        var orgListInds =  self.indexToOrig[ints]
        for o in orgListInds:
            retOrigs.append(self.rawVals[o[]])
        return retOrigs

    fn update(inout self, valSet: Set[Int]) raises:
        var offset = 0
        for rawInd in range(len(self.rawVals)):
            var ind = self.origToIndex[self.rawVals[rawInd+offset]]
            if ind not in valSet:
                var poppedOrig = self.rawVals.pop(rawInd+offset)
                offset += -1
                var popIndToOrg = self.origToIndex.pop(poppedOrig)
                _ = self.indexToOrig.pop(popIndToOrg)

    
@value 
struct DBTable:
    var table: List[Int]
    var columnNames: List[String]
    var numOfColumns: Int
    var numOfRows: Int

    # ===-------------------------------------------------------------------===#
    # Life cycle methods
    # ===-------------------------------------------------------------------===#

    fn __init__(inout self, numOfColumns: Int, numOfRows: Int, owned columnNames: List[String] = List[String]()):
        self.table = List[Int](capacity = numOfColumns*numOfRows)
        self.numOfColumns = numOfColumns
        self.numOfRows = numOfRows
        self.columnNames = columnNames
        if len(columnNames) == 0:
            var baseStr = 'column' 
            for n in range(numOfColumns):
                self.columnNames.append(baseStr+str(n))
        #TODO find a neet way to handel colum mismatch without raising (cant immplement __copyinit__ when __init__ raises)
        # elif len(columnNames) != numOfColumns:
        #     raise('DBTable __init_ Error: cannot initialize a DBTable where length of columnNames list does not match numOfColumns')
    
    fn __moveinit__(inout self, owned other: Self):
        self.table = other.table^
        self.columnNames = other.columnNames^
        self.numOfColumns = other.numOfColumns
        self.numOfRows = other.numOfRows
    
    fn __copyinit__(inout self, other: Self):
        self = Self(other.numOfColumns, other.numOfRows, other.columnNames)
        for i in other.table:
            self.table.append(i[])


    # ===-------------------------------------------------------------------===#
    # Dunders
    # ===-------------------------------------------------------------------===#

    fn __getitem__(self, index: Int) raises -> Int:
        if index >= self.numOfRows * self.numOfColumns:
            raise 'index out of bounds'
        
        return self.table[index]

    fn __getitem__(self, col: Int, row: Int) raises -> Int:
        if col >= self.numOfColumns:
            raise 'column index out of bounds'
        if row >= self.numOfRows:
            raise 'row index out of bounds'
        
        return self.table[col*self.numOfRows + row]

    fn __setitem__(inout self, index: Int, val: Int) raises:
        if index >= self.numOfRows * self.numOfColumns:
            raise 'index out of bounds'
        
        self.table[index] = val

    fn __setitem__(inout self, col: Int, row: Int, val: Int) raises:
        if col >= self.numOfColumns:
            raise 'column index out of bounds'
        if row >= self.numOfRows:
            raise 'row index out of bounds'
        
        self.table[col*self.numOfRows + row] = val
        
    fn __str__(self) -> String:
        try:
            var printStr = str('---####------####---: \n')
            for col in range(self.numOfColumns):
                printStr += self.columnNames[col]+': ['
                for val in range(self.numOfRows):
                    printStr += str(self.__getitem__(col,val))+ ', '
                printStr += ']\n'
            return printStr + '---############---\n'
        except:
            return 'issue with __str__, probably out of bounds issue'
    
    fn __eq__(self, rhs: Self) -> Bool:
        if self.numOfColumns != rhs.numOfColumns or self.numOfRows != rhs.numOfRows:
            return False
        
        for name in range(len(self.columnNames)):
            if self.columnNames[name] != rhs.columnNames[name]:
                return False
        
        for ind in range(self.numOfColumns*self.numOfRows):
            if self.table[ind] != rhs.table[ind]:
                return False
        
        return True

    fn __ne__(self, rhs: Self) -> Bool:
        if self.numOfColumns != rhs.numOfColumns or self.numOfRows != rhs.numOfRows:
            return True
        
        for name in range(len(self.columnNames)):
            if self.columnNames[name] != rhs.columnNames[name]:
                return True

        for ind in range(self.numOfColumns*self.numOfRows):
            if self.table[ind] != rhs.table[ind]:
                return True
        
        return False
    
    # ===-------------------------------------------------------------------===#
    # Methods: update/write
    # ===-------------------------------------------------------------------===#

    fn add_empty_col(inout self, columnNames: List[String]) raises:
        for col in range(len(columnNames)):
            self.table.append(-1)

    fn add_col(inout self, owned *columns: List[Int], columnNames: List[String]) raises:
        if len(columnNames) != len(columns):
            raise('DBTable - addCol Error: number of columns added must match number of column names')
        for col in columns:
            if len(col[]) >= self.numOfRows:
                raise 'DBTable - addCol Error: When adding columns the number of elements must match the number of rows in the table'
            for val in col[]:
                self.table.append(val[])

    fn add_col(inout self, owned columns: List[List[Int]], columnNames: List[String]) raises:
        if len(columnNames) != len(columns):
            raise('DBTable - addCol Error: number of columns added must match number of column names')
        for col in columns:
            if len(col[]) >= self.numOfRows:
                raise 'DBTable - addCol Error: When adding columns the number of elements must match the number of rows in the table'
            for val in col[]:
                self.table.append(val[])
    
    fn type(self):
        print('DBTable')      

    # ===-------------------------------------------------------------------===#
    # Methods: read
    # ===-------------------------------------------------------------------===#
    
    fn filter(self, columns: List[Int], columnValues: List[List[Int]]) raises -> Set[Int]:
        print('Filtering....')
        print('-----------------------------')
        var filter_column_len = len(columns)
        var resSet = Set[Int]()

        var total = now()
        
        #rows for the first column
        var start = now()
        var valSet = Set[Int](columnValues[0])
        
        for row in range(self.numOfRows):
            if self[row] in valSet:
                resSet.add(row)
        print('get rowlist from first column')    
        print(str((now()-start)/1_000_000_000)+'sec')

        #remove rows from resSet that are not true for other filter columns
        start = now()
        for col in range(1, filter_column_len): 
            valSet = Set(columnValues[col])
            for row in resSet:
                if self[col,row[]] not in valSet:
                    resSet.remove(row[])
        print('combine OR vals & find common rows')    
        print(str((now()-start)/1_000_000_000)+'sec')

        
        print('-----------------------------')
        print('TOTAL FILTER TIME: '+str((now()-total)/1_000_000_000)+'sec')
        return resSet^
    
    fn filter(self, columns: Int, columnValues: List[Int]) raises -> Set[Int]:
        print('Filtering....')
        print('-----------------------------')
        var resSet = Set[Int]()
        var total = now()
        
        var start = now()
        var valSet = Set[Int](columnValues)
        for row in range(self.numOfRows):
            if self[row] in valSet:
                resSet.add(row)
        print('get rowlist from first column')    
        print(str((now()-start)/1_000_000_000)+'sec')
        
        print('-----------------------------')
        print('TOTAL FILTER TIME: '+str((now()-total)/1_000_000_000)+'sec')
        return resSet^

    fn filter(self, columns: List[Int], columnValues: List[Set[Int]]) raises -> Set[Int]:
        print('Filtering....')
        print('-----------------------------')
        var filter_column_len = len(columns)
        var resSet = Set[Int]()

        var total = now()
        
        #rows for the first column
        var start = now()
        for row in range(self.numOfRows):
            if self[row] in columnValues[0]:
                resSet.add(row)
        print('get rowlist from first column')    
        print(str((now()-start)/1_000_000_000)+'sec')

        #remove rows from resSet that are not true for other filter columns
        start = now()
        for col in range(1, filter_column_len): 
            for row in resSet:
                if self[col,row[]] not in columnValues[col]:
                    resSet.remove(row[])
        print('combine OR vals & find common rows')    
        print(str((now()-start)/1_000_000_000)+'sec')

        
        print('-----------------------------')
        print('TOTAL FILTER TIME: '+str((now()-total)/1_000_000_000)+'sec')
        return resSet^

    #TODO currently no way to preserve column index to translate back to lookup,
    #     need some way to identify each column later
    #     1. could have index 0 of each column contain the identifier
    #     2. could pass lookup to return table (requires lookup to be apart of table?)
    #     3. could have a list of column names (Trying THIS)
    fn apply_settings(self, rowSet: Set[Int], colList: List[Int]) raises -> Self:
        var numOfRows = len(rowSet)
        var numOfColumns= len(colList)
        var retColNames = List[String]()
        
        for ind in colList:
            retColNames.append(self.columnNames[ind[]])

        var retTab = DBTable(numOfColumns = numOfColumns, numOfRows = numOfRows, columnNames = retColNames)
        for col in colList:
            for row in rowSet:
                retTab.table.append(self[col[],row[]])
        
        return retTab^
    
    fn apply_settings(self, rowSet: Set[Int]) raises -> Self:
        var numOfRows = len(rowSet)

        var retTab = DBTable(numOfColumns = self.numOfColumns, numOfRows = self.numOfRows, columnNames = self.columnNames)
        for col in range(self.numOfColumns):
            for row in rowSet:
                retTab.table.append(self[col,row[]])
        
        return retTab^
    
    #TODO Order by 
    # fn order_by(self, order_cols: List[Int]) raises -> List[Int]:


    #TODO Group by
    # fn groupBy(self, groupCols: List[Int]) raises -> Self:

    #TODO countDistinct testing
    fn count_distinct(self, cols: List[Int], rowSet: Set[Int]) raises -> List[Int]:
        var ret_list = List[Int]()
        var dist_set: Set[Int]
        
        for col in cols:
            dist_set = Set[Int]()
            for row in rowSet:
                dist_set.add(self[col[],row[]])
            ret_list.append(len(dist_set))
        
        return ret_list^

    fn count_distinct(self, cols: Int, rowSet: Set[Int]) raises -> Int:
        var dist_set = Set[Int]()

        for row in rowSet:
            dist_set.add(self[cols,row[]])
        
        return len(dist_set)

    fn count_distinct(self, cols: List[Int]) raises -> List[Int]:
        var ret_list = List[Int]()
        var dist_set: Set[Int]
        
        for col in cols:
            dist_set = Set[Int]()
            for row in self.rows:
                dist_set.add(self[col[],row[]])
            ret_list.append(len(dist_set))
        
        return ret_list^
    
    fn count_distinct(self, cols: Int) raises -> Int:
        var dist_set = Set[Int]()

        for row in self.rows:
            dist_set.add(self[cols,row[]])
        
        return len(dist_set)



    #TODO joins
    fn inner_join(self, other: Self, self_join_cols: List[Int], other_join_cols: List[Int], self_ret_cols: List[Int], other_ret_cols: List[Int]) raises -> Self:
        var ret_row = List[Int]()
        var new_row_count = 0
        var new_col_count = len(self_ret_cols) + len(other_ret_cols)

        var match_all: Bool
        for s_row in range(self.numOfRows):
            for o_row in range(other.numOfRows):
                match_all = True
                for j in range(len(self_join_cols)):
                    if self[self_join_cols[j],s_row] != other[other_join_cols[j],o_row]:
                        match_all = False
                        break
                if match_all:
                    for i in self_ret_cols:
                        ret_row.append(self[i[],s_row])
                    for i in other_ret_cols:
                        ret_row.append(other[i[],o_row])
                    new_row_count += 1

        if new_row_count == 0:
            return DBTable(0,0)

        var ret_col_names = List[String]()
        for c in self_ret_cols:
            ret_col_names.append(self.columnNames[c[]])
        for c in other_ret_cols:
            ret_col_names.append(other.columnNames[c[]])
        
        var ret_tab = DBTable(new_col_count,new_row_count,ret_col_names)
        for col in range(new_col_count):
            for row in range(new_row_count):
                ret_tab.table.append(ret_row[row*new_col_count+col])
        
        return ret_tab^

    # fn inner_join(self, other: Self, self_join_cols: List[Int], other_join_cols: List[Int], self_set: Set[Int] = Set[Int](), other_set: Set[Int] = Set[Int]()) raises -> (Set[Int],Set[Int]):
    #     var new_self_set = Set[Int]()
    #     var new_other_set = Set[Int]()


