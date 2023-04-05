import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

value = 1


def compare_data(new,old):
    old_obj = ["address","city","district","state","bank_name"]
    new_obj = ["ADDRESS","CITY","DISTRICT","STATE","BANK"]
    for i in range(0,len(old_obj)):
        if(old[old_obj[i]] != new[new_obj[i]]):
                wrong_ifsc.append(old["ifsc"])
                break

def verify_data(data):
    global value
    print(value)
    value += 1
    response = requests.get(url+data["IFSC"])
    if(response.status_code != 200):
        untracked_ifsc.append(data["IFSC"])
    else:
        compare_data(data,response.json())

def create_csv(file_name,headers_list):
    df = pd.DataFrame(list(),columns=headers_list)
    df.to_csv(file_name)

url = "http://10.24.2.60/ifsc/"
file = "IFSC.csv"
df = pd.read_csv(file,low_memory=False)
untracked_ifsc = []
wrong_ifsc = []
untracked_ifsc_file = "untracked_ifsc.csv"
wrong_ifsc_file = "wrong_ifsc.csv"
create_csv(untracked_ifsc_file,["IFSC"])
create_csv(wrong_ifsc_file,["IFSC"])
data = []
for i in range(0,df.index.size):
    data.append(df.iloc[i])

print("data extracted")
with ThreadPoolExecutor(max_workers=500) as pool:
    pool.map(verify_data,data)

print(untracked_ifsc)
print(wrong_ifsc)

if(len(untracked_ifsc) > 0):
    df = pd.read_csv(untracked_ifsc_file,index_col=0)
    df["IFSC"] = untracked_ifsc
    df.to_csv(untracked_ifsc_file)

if(len(wrong_ifsc) > 0):
    df = pd.read_csv(wrong_ifsc_file,index_col=0)
    df["IFSC"] = wrong_ifsc
    df.to_csv(wrong_ifsc_file)



