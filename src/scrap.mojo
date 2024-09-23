from db_package.table import DBTable
from random import random_ui64, rand
from time import now
from algorithm import vectorize, parallelize
from sys import simdwidthof
from memory.memory import memset_zero, memset
from collections.dict import Dict, _DictKeyIter, _DictValueIter, _DictEntryIter
from db_package.set import Set
from python import Python
from memory.reference import Reference

alias col_count = 100 
alias row_count = 100_000
alias val_range = 1_000
alias filter_col_count = 5
alias filter_val_range = 5

fn createTable() raises -> DBTable:
    var tab = DBTable(numOfColumns = col_count, numOfRows = row_count)
    @parameter
    for col in range(col_count):
        for row in range(row_count):
            tab.table.append(row%val_range)
    return tab^

fn createRandomTable() raises -> DBTable:
    var tab = DBTable(numOfColumns = col_count, numOfRows = row_count)

    for col in range(col_count):
        for row in range(row_count):
            tab[col*row_count + row] = int(random_ui64(1,val_range))
    return tab^

fn createFilterCols() -> List[Int]:
    var lst = List[Int]()
    for x in range(filter_col_count):
        lst.append(x)
    return lst^

fn createFilterVals() -> List[List[Int]]:
    var outer = List[List[Int]]()
    var inner: List[Int]
    for x in range(filter_col_count):
        inner = List[Int]()
        for y in range(filter_val_range):
            inner.append(y)
        outer.append(inner)
    return outer^

fn createFilterSets() -> List[Set[Int]]:
    var outer = List[Set[Int]]()
    var inner: Set[Int]
    for x in range(filter_col_count):
        inner = Set[Int]()
        for y in range(filter_val_range):
            inner.add(y)
        outer.append(inner)
    return outer^

alias type = DType.uint32
alias nelts = simdwidthof[type]() * 2

struct Matrix[cols: Int, rows: Int]:
    var data: UnsafePointer[Scalar[type]]

    # Initialize zeroeing all values
    fn __init__(inout self):
        self.data = UnsafePointer[Scalar[type]].alloc(rows * cols)
        memset_zero(self.data, rows * cols)

    # Initialize taking a pointer, don't set any elements
    fn __init__(inout self, data: UnsafePointer[Scalar[type]]):
        self.data = data

    fn __del__(owned self):
        self.data.free()

    # Initialize with random values
    @staticmethod
    fn rand() -> Self:
        var data = UnsafePointer[Scalar[type]].alloc(rows * cols)
        rand(data, rows * cols)
        return Self(data)

    fn __getitem__(self, y: Int, x: Int) -> Scalar[type]:
        return self.load[1](y, x)

    fn __setitem__(self, y: Int, x: Int, val: Scalar[type]):
        self.store[1](y, x, val)

    fn load[nelts: Int](self, y: Int, x: Int) -> SIMD[type, nelts]:
        return self.data.load[width=nelts](y * self.rows + x)

    fn store[nelts: Int](self, y: Int, x: Int, val: SIMD[type, nelts]):
        return self.data.store[width=nelts](y * self.rows + x, val)

    #TODO wait for #3229 to be fixed...
    # fn filterCol(self, col: Int, filters: List[Int], base: UnsafePointer[Scalar[DType.bool]]):
    #     for f in filters:
    #         print('baseOutter',base.load[width = self.rows](0))
    #         @parameter
    #         fn comp[nelts: Int](n: Int):
    #             print('nelts',nelts, 'n', n)
    #             var stugg = ( self.load[nelts](col, n).__eq__(SIMD[type, nelts](f[])) )
    #             print('self',self.load[nelts](col, n))
    #             print('f',SIMD[type, nelts](f[]))
    #             print('stugg',stugg)
    #             var stugg2 = base.load[width = nelts](n) | stugg
    #             print('stugg2',stugg2)
    #             base.store[width = nelts](n, stugg2)  
    #             print('baseN',n,base.load[width = nelts](0))
    #             print('base',base.load[width = self.rows](0))
    #             # print(nelts)
    #             # print(base.load[width = nelts](0))
    #         print('nelts',nelts)
    #         vectorize[comp, nelts](size = self.rows)

    #TODO reconfig when  #3229 is fixed
    fn filterCol(self, col: Int, filters: List[Int]) ->  UnsafePointer[Scalar[DType.uint8]]:
        var base = UnsafePointer[Scalar[DType.uint8]].alloc(self.rows)
        for f in filters:
            @parameter
            fn comp[nelts: Int](n: Int):
                var stugg = ( self.load[nelts](col, n).__eq__(SIMD[type, nelts](f[])) )
                var stugg2 = base.load[width = nelts](n) | stugg.cast[DType.uint8]()
                base.store[width = nelts](n, stugg2)  
            vectorize[comp, nelts](size = self.rows)  
        return base    
    
    #TODO finish the function
    # fn countDistinct(self, cols: List[Int]) -> List[type]:
    #     var res_list = List[type]()
    #     var dist_set: Set[type]

    #     var base = UnsafePointer[Scalar[DType.uint8]].alloc(self.rows)

    #     for col in cols:
    #         dist_set = Set[type]()
    #         @parameter
    #         fn[nelts: Int](n:Int):
    #             var stugg = ( self.load[nelts](col[], n)


    
    # fn select(self, *, filter_cols, filter_col_vals, group_cols, )

def main():
    print('col_count: ' +str(col_count))
    print('row_count: ' +str(row_count))
    print('val_range: ' +str(val_range))
    print('filter_col_count: ' +str(filter_col_count))
    print('filter_val_range: ' +str(filter_val_range))

    # print('creating Matrix...')
    # var start = now()
    # var m = Matrix[col_count, row_count]()
    # print(str((now()-start)/1_000_000_000)+'sec')

    print('filling Matrix...')
    start = now()
    #pop table
    # for y in range(col_count):
    #     for x in range(row_count):
    #         m[y,x] = x%val_range
            # m[y,x] = int(random_ui64(SIMD[DType.uint64, 1](0),SIMD[DType.uint64, 1](val_range)) )
    print(str((now()-start)/1_000_000_000)+'sec')
    
    print('creating Filters...')
    start = now()
    var filCols = createFilterCols()
    var filVals = createFilterVals()
    # var filVals = createFilterSets()
    print(str((now()-start)/1_000_000_000)+'sec')
    
    
    # print('Testing filterCols:')
    # var base =  m.filterCol(filCols[0], filVals[0])
    # var out = UnsafePointer[Scalar[DType.uint8]].alloc(m.rows)
    # memset(out,SIMD[DType.uint8, 1](0),m.rows)

    # start = now()
    # for x in range(len(filCols)):
    #     base = m.filterCol(filCols[x], filVals[x])
    #     @parameter
    #     fn and_cols[nelts: Int](n: Int):
    #         var stugg = base.load[width = nelts](n) | out.load[width = nelts](n)
    #         out.store[width = nelts](n, stugg)
    #     vectorize[and_cols, simdwidthof[DType.uint8]() * 2](size = m.rows)
    #     # base = m.filterCol(filCols[x], filVals[x])
    # print(str((now()-start)/1_000_000_000)+'sec')
    # var count = 0
    # for x in range(row_count):
    #     count += int(base[x])
    # base.free()
    # print(count)
    # print('\n\n')
    
    
    var t = createTable()
    start = now()
    print('Testing DBTable Filter...')
    print('Testing join...')
    # var s = t.filter(filCols, filVals)
    print('t',len(t.table))
    var t2 = t

    # print(str(t))
    # print('---------')
    # print(str(t2))

    # print(t == t2)
    var t3 = t.inner_join(other = t2, self_join_cols = List(2), other_join_cols = List(2), self_ret_cols = List(0,1), other_ret_cols = List(1,2))
    # print(str(t3))

    print(str((now()-start)/1_000_000_000)+'sec')
    # count = 0
    # print(len(s))
    # var my_set = Set(int(Scalar[DType.uint8](1)))
    # var j = List(Scalar[DType.uint8](1),Scalar[DType.uint8](2),Scalar[DType.uint8](3))
    # print(j.data.load[width = 3](0))
    # var k = j.unsafe_ptr()
    # # k.load[DType.int64](0)
    # var base = UnsafePointer[Scalar[DType.uint8]].alloc(8)
    # memset_zero(base,8)
    # print(base.load[width = 2](1))
    # # print(j.unsafe_ptr())

