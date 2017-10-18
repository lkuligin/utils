from emr_connect import load_hive
sql="SELECT * FROM default.tpixel LIMIT 10"
import pandas as pd
t=load_hive(sql)
print type(t)
print t.head()
