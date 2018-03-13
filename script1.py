from sqlplus import *


setdir("data")



with connect('db.db') as c:
    c.load('mdata1.csv')





