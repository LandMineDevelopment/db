from db_package.table import DBTable
from testing import assert_equal, assert_not_equal, assert_false, assert_raises, assert_true


alias tab_row_count = 5 
alias tab_columns = 5
alias val_range = 5    


# ===-------------------------------------------------------------------===#
# Setup functions
# ===-------------------------------------------------------------------===#

def createTable() -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = row%val_range
    return tab^

def createTable(columnNames: List[String]) -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count, columnNames = columnNames)

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

def createRandomTable(columnNames: List[String]) -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count, columnNames = columnNames)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = int(random_ui64(1,val_range))
    return tab^

def createFilterCols(filter_cols: Int) -> List[Int]:
    var lst = List[Int]()
    for x in range(filter_cols):
        lst.append(x)
    return lst^

def createFilterVals(filter_cols: Int, filter_vals: Int) -> List[List[Int]]:
    var outer = List[List[Int]]()
    var inner: List[Int]
    for x in range(filter_cols):
        inner = List[Int]()
        for y in range(filter_vals):
            inner.append(y)
        outer.append(inner)
    return outer^

# ===-------------------------------------------------------------------===#
# Life cycle methods
# ===-------------------------------------------------------------------===#

def test_init():
    var colNameList = List[String]()
    for name in range(tab_columns+1):
        colNameList.append('t'+str(name))
    
    #test too many columnNames
    with assert_raises():
        _ = createTable(colNameList)
    
    _ = colNameList.pop()
    _ = colNameList.pop()
    
    #test too few columnNames
    with assert_raises():
        _ = createTable(colNameList)


# ===-------------------------------------------------------------------===#
# dunder: __eq__
# ===-------------------------------------------------------------------===#

def test_eq():
    rhs = createTable()
    assert_equal(createTable(),rhs, msg = '__eq__ error: DBtables should match but do not')

# ===-------------------------------------------------------------------===#
# dunder: __ne__
# ===-------------------------------------------------------------------===#

def test_ne_index():
    rhs = createTable()
    rhs[0] = -1
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: failed differing value at a given index')

def test_ne_cols():
    lhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)
    rhs = DBTable(numOfColumns = tab_columns+1, numOfRows = tab_row_count)
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: column sizes do not match')

def test_ne_rows():
    lhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)
    rhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count+1)
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: row count do not match')

# ===-------------------------------------------------------------------===#
# method: filter
# ===-------------------------------------------------------------------===#

def test_filter():
    alias filter_cols = 2
    alias filter_vals = 2
    
    var tab = createTable()
    var filCols = createFilterCols(filter_cols)
    var filVals = createFilterVals(filter_cols, filter_vals)

    var resSet = tab.filter(filCols,filVals)
    
    var valSet: Set[Int]
    for col in filCols:
        valSet = filVals[col[]]
        for row in resSet:
            assert_true(tab[col[],row[]] in valSet, 'filter error: returns a value not in the filter set')

# ===-------------------------------------------------------------------===#
# method: applySettings
# ===-------------------------------------------------------------------===#
#TODO add testing on columnNames
def test_applySettings():
    alias filter_cols = 2
    alias filter_vals = 2
    
    var tab = createTable()
    var filCols = createFilterCols(filter_cols)
    var filVals = createFilterVals(filter_cols, filter_vals)

    var resSet = tab.filter(filCols,filVals)
    var newTab = tab.applySettings(resSet)
    
    var valSet: Set[Int]
    for col in filCols:
        valSet = filVals[col[]]
        for row in resSet:
            assert_true(newTab[col[],row[]] in valSet, 'applySettings error: returns a value not in the filter set')