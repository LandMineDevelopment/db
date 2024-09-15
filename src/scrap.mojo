from db_package.table import DBTable
from random import random_ui64
from time import now

alias tab_row_count = 10
alias tab_columns = 5
alias val_range = 20    
alias filter_cols = 5
alias filter_vals = 10

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


def main():
    var tab = createTable()
    print(tab)


    var filter_cols = createFilterCols()
    var filter_vals = createFilterVals()

    var filtered_set = tab.filter(filter_cols,filter_vals)
    var start = now()
    var restTab = tab.applySettings(filtered_set)
    print('\napplySettings time:', (now()-start)/1_000_000_000, 'sec')

    print('\nSet for Table')
    print('Row Count:',len(filtered_set))

    print('\nresTab')
    print('Column Count:', restTab.numOfColumns, 'Row Count:', restTab.numOfRows)

    print('\n########################################')
    print('########################################\n')

    var tabRand = createRandomTable()
    filtered_set = tabRand.filter(filter_cols,filter_vals)
    start = now()
    restTab = tabRand.applySettings(filtered_set)
    print('\napplySettings time:', (now()-start)/1_000_000_000, 'sec')

    print('\nSet for Random Table')
    print('Row Count:',len(filtered_set))

    print('\nresTab')
    print('Column Count:', restTab.numOfColumns, 'Row Count:', restTab.numOfRows)
    var s = Set(5)

###currently working on lookups   