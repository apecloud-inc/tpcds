import os
import re

filetosave = "/cqueries"

if not os.path.exists(os.getcwd() + filetosave):
    os.mkdir(os.getcwd() + filetosave)

splitted_text = ""
query_count = 0

qdays_list = [5, 12, 16, 20, 21, 32, 37, 40, 77, 80, 82, 92, 94, 95, 98]
loch_list = [36, 70, 86]
roolup_list = [5, 14, 18, 22, 27, 36, 67, 70, 77, 80, 86]
int_list = [54]

with open('query_0.sql', 'r') as q0:
    text_split = q0.read().split("\n\n\n")

text_split = text_split[:-1]

def replace_rollup(sql):
    return re.sub(r'\bROLLUP\s*\((.*?)\)', r'\1 WITH ROLLUP', sql, flags=re.IGNORECASE)

def replace_full_outer_join(sql):
    # find full outer join field
    return re.sub(r'\bFULL OUTER JOIN\s*(.*?)\s*ON\s*\((.*?)\)', r'(left join \1 on (\2) union right join \1 on (\2))', sql, flags=re.IGNORECASE)

def replace_days(sql):
    return re.sub(r"[+-]\s*(\d+)\s*days", r"+ INTERVAL \1 DAY", sql)

for each_text in text_split:
    query_count += 1
    if query_count == 30:
        each_text = each_text.replace('c_last_review_date_sk', 'c_last_review_date')
    elif query_count in qdays_list:
        each_text = replace_days(each_text)
    elif query_count in loch_list:
        each_text = each_text.replace('select', 'select * from (select ', 1)
        each_text = ') as sub\n order by'.join(each_text.rsplit('order by', 1))
    elif query_count in int_list:
        each_text = each_text.replace('int', 'signed')
    
    if query_count in roolup_list:
        each_text = replace_rollup(each_text)
        
    each_file = open(os.getcwd() + filetosave + "/query" + query_count.__str__() + ".sql", "w")
    each_file.write(each_text.lstrip() + "\n\n")
            
    each_file.close()
