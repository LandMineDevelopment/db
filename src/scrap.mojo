from db_package.table import DBTable
from random import random_ui64, rand
from time import now
from algorithm import vectorize

alias tab_row_count = 1_000_000
alias tab_columns = 1_000
alias val_range = 2    
alias filter_cols = 5
alias filter_vals = 8

def createTable() -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = row%val_range
    return tab^

def createRandomTable() -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = int(random_ui64(1,val_range))
    return tab^

def createFilterCols() -> List[Int]:
    var lst = List[Int]()
    for x in range(filter_cols):
        lst.append(x)
    return lst^

def createFilterVals() -> List[List[Int]]:
    var outer = List[List[Int]]()
    var inner: List[Int]
    for x in range(filter_cols):
        inner = List[Int]()
        for y in range(filter_vals):
            inner.append(y)
        outer.append(inner)
    return outer^


alias type = DType.uint32
alias nelts = simdwidthof[type]() * 2

struct Matrix[cols: Int, rows: Int]:
    var data: DTypePointer[type]

    # Initialize zeroeing all values
    fn __init__(inout self):
        self.data = DTypePointer[type].alloc(rows * cols)
        memset_zero(self.data, rows * cols)

    # Initialize taking a pointer, don't set any elements
    fn __init__(inout self, data: DTypePointer[type]):
        self.data = data

    # Initialize with random values
    @staticmethod
    fn rand() -> Self:
        var data = DTypePointer[type].alloc(rows * cols)
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

    fn filterCol(self, col: Int, filters: List[Int], base: DTypePointer[DType.bool]):
        for f in filters:
            print('baseOutter',base.load[width = self.rows](0))
            @parameter
            fn comp[nelts: Int](n: Int):
                # print('nelts',nelts)
                # var stugg = ( self.load[16](col, 0).__eq__(SIMD[type, 16](f[])) )
                # print('self',self.load[16](col, 0))
                # print('f',SIMD[type, 16](f[]))
                # print('stugg',stugg)
                # var stugg2 = base.load[width = 16](n) | stugg
                # print('stugg2',stugg2)
                # base.store[width = 16](n, stugg2)  
                # print(nelts)
                # print(base.load[width = nelts](0))

                # fn comp[nelts: Int](n: Int):
                print('nelts',nelts, 'n', n)
                var stugg = ( self.load[nelts](col, n).__eq__(SIMD[type, nelts](f[])) )
                print('self',self.load[nelts](col, n))
                print('f',SIMD[type, nelts](f[]))
                print('stugg',stugg)
                var stugg2 = base.load[width = nelts](n) | stugg
                print('stugg2',stugg2)
                base.store[width = nelts](n, stugg2)  
                print('baseN',n,base.load[width = nelts](0))
                print('base',base.load[width = self.rows](0))
                # print(nelts)
                # print(base.load[width = nelts](0))
            print('nelts',nelts)
            vectorize[comp, nelts](size = self.rows)
            
    
    


def main():
    alias col_count = 2 
    alias row_count = 3
    alias val_range = 3
    alias filter_col_count = 2
    alias filter_val_range = 2

    print('col_count: ' +str(col_count))
    print('row_count: ' +str(row_count))
    print('val_range: ' +str(val_range))
    print('filter_col_count: ' +str(filter_col_count))
    print('filter_val_range: ' +str(filter_val_range))

    var m = Matrix[col_count, row_count]()

    #pop table
    for y in range(col_count):
        for x in range(row_count):
            m[y,x] = x%val_range
    
    #create filters
    var filCols = List[Int]()
    var filVals = List[List[Int]]()
    for col in range(filter_col_count):
        filCols.append(col)
        var tmp = List[Int]()
        for val in range(filter_val_range):
            tmp.append(val)
        filVals.append(tmp)


    print('Testing filterCols:')
    var start = now()
    # _ = m.filterCols(filCols, filVals)
    var base = DTypePointer[DType.bool].alloc(row_count)
    memset_zero(base, row_count)

    # for x in range(len(filCols)):
    m.filterCol(filCols[0], filVals[0], base)
    print(str((now()-start)/1_000_000_000)+'sec')

    var count = 0
    for x in range(row_count):
        print('count', base.load(x))

    print('m   ',m.load[col_count*row_count](0,0))
    print('filVals[0]','[',filVals[0][0],',',filVals[0][1],',',filVals[0][2],']')
    print('base',base.load[width = 3]())
    print(count)
    base.free()

    var t = DTypePointer[DType.bool].alloc(16)
    memset_zero(t, 16)
    t.store[width = 1](0,True)
    t.store[width = 4](1,-1)
    t.store[width = 1](2,False)
    print('t',t.load[width = 16](0))

    # for x in range(len(filCols)):
    #     base.__iand__(m.filterCol(filCols[x],filVals[x]))
    

    # print(base.slice[16]())
    # for x in range(filter_val_range):
    #     print(filVals[0][x]==filVals[1][x])
    
    # var c = 0
    # for x in range(row_count):
    #     if base[x] == True:
    #         c += 1
    # print(c)
    # alias test = SIMD[DType.int8,8](1)
    # print(test.reduce_add())

    # var tab = createTable()
    # var filters =  createFilterCols() 
    # var filVals = createFilterVals()
    # _ = tab.filter(filters, filVals)
    '''
    alias n = 16
    var m = Matrix[1,n]()
    for i in range(m.rows):
        m[0,i] = i
    # print(m.load[m.rows](0, 0))
    var f = List[Int](1,2,3,4)
    var f2 = List[Int](1,3)
    var filters = List(f,f2)
    var cols = List(0,0)
    
    print(m.filterCols(cols, filters))
    '''


    '''
    var v1 = SIMD[DType.uint32,16](0,1,2,3,4,5,6,7,8)
    var v2 = SIMD[DType.uint32,4](2,4,5,7)
    var v3 = SIMD[DType.uint32,16](2)

    v3 = SIMD[DType.uint32,16](v2[0])
    var b = v1.__eq__(v3)
    for i in range(1, len(v2)):
        v3 = SIMD[DType.uint32,16](v2[i])
        b.__ior__(v1.__eq__(v3))
        print(b)
    var retList = List[Int](capacity = len(v1))
    for i in range(len(b)):
        if b[i]:
            retList.append(int(v1[i]))
            print(v1[i])
'''
#test 
'''
fiter vector

v1 = raw int64 column vals (len = ~1milion)
v2 = 1st filter  int64 vals (len = ~1milion)
v3 = return vector bool

v1 defined already
v2.splat(1st filter val)
v3 = v1.__eq__(v2)

update v2 to next filter
v2.splat(next filter val)

v3.__ior__(v1.__eq__(v2))

repeat for all other filters...

v3 now has all vals indexes that are one of the filter vals

return v3

how do I get it back?
multiply v3.cast(typeOf(v1)) and v1
'''