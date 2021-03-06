import requests as req
import json
import pandas as pd
import datetime
import re
import numpy as np

Cilent_ID = YOUR ID
Client_Secret = YOUR SECRET
Access_Token = YOUR ACCESS TOKEN
band_header = {"Authorization":f"Bearer {Access_Token}"}

def get_band_id():
#get band id
    band_id_url = 'https://openapi.band.us/v2.1/bands'
    band_id_response = req.get(band_id_url,headers=band_header)
    id_result = json.loads(band_id_response.text) 


    temp = id_result['result_data']['bands']
    id_length=len(temp)
    band_key_list = []
    for idx in range(id_length):
        band_key_list.append((temp[idx]['name'],temp[idx]['band_key']))
    return band_key_list

# ### GET CONTENTS
# 밴드 이름|글쓴이|시간|글 번호|글 내용|댓글 수|감정 수

def first_page(band_key):
    #get first page
    band_content_url = 'https://openapi.band.us/v2/band/posts?band_key='
    band_content_response = req.get(band_content_url+band_key,headers = band_header)
    band_content_json = json.loads(band_content_response.text)
    after = band_content_json['result_data']['paging']['next_params']['after']

    return band_content_json, after

def after_page(band_key, after):
    #get after page
    band_content_url = 'https://openapi.band.us/v2/band/posts?after='+after+'&limit=20&band_key='
    band_content_response = req.get(band_content_url+band_key,headers = band_header)
    band_content_json = json.loads(band_content_response.text)
    try:
        after = band_content_json['result_data']['paging']['next_params']['after']
    except:
        after = 0

    return band_content_json, after

#check time
def check_time(epochtime,enddate):
    return epochtime >= enddate


#milliesconds to datetype
def make_time(epochtime):
    return str(datetime.datetime.fromtimestamp(epochtime//1000))

#데이터 가공
def make_data(content):
    global flag
    row = []
    temp = content['result_data']['items']
    for idx in range(len(temp)):
        epochtime=make_time(temp[idx]['created_at'])
        content = temp[idx]['content']
        hashtag =  re.findall(r"#(\w+)", content)
        delimeter = ","
        a=delimeter.join(hashtag)
        if not check_time(epochtime,enddate):
            flag = 1
            break
        else:
            row.append((temp[idx]['author']['name'],epochtime,temp[idx]['post_key'],content,a,temp[idx]['comment_count'],temp[idx]['emotion_count']))
    return row

# ### GET Comments
# 밴드 이름|글 번호|댓글쓴이|댓글내용

def get_comment(band_key,post_key):
    #get first page
    band_comment_url = 'https://openapi.band.us/v2/band/post/comments?band_key='+band_key+'&post_key='+post_key
    band_comment_response = req.get(band_comment_url,headers = band_header)
    band_comment_json = json.loads(band_comment_response.text)
    
    return band_comment_json
#댓글이 20개 이상 넘어가는 경우가 있을까?

def get_each_comment(band_comment_json):
    temp=band_comment_json['result_data']['items']
    row = []
    for idx in range(len(temp)):
        row.append((temp[idx]['post_key'],make_time(temp[idx]['created_at']),temp[idx]['author']['name'],temp[idx]['content']))
    return row

# ### GET HASHTAG FOR TREND ANALYTICS
# 날씨/시간|해시태그

def get_hashtag_db(Contents):
    Contents['date_hour']=Contents['createdate'].str[:13]+':00:00'
    temp2 = Contents[['bandname','date_hour','hashtag']]

    temp2['hashtag']=temp2['hashtag'].apply(lambda x : x.split(','))

    r = pd.DataFrame({
        col:np.repeat(temp2['date_hour'].values, temp2['hashtag'].str.len())
        for col in temp2.columns.drop('hashtag')}
        ).assign(**{'hashtag':np.concatenate(temp2['hashtag'].values)}) 

    band_name=np.repeat(temp2['bandname'].values, temp2['hashtag'].str.len())
    band_name=pd.DataFrame(band_name,columns={'bandname'})
    r=pd.concat([band_name,r],axis = 1)

    return r


band_key_list = get_band_id()

enddate = (datetime.datetime.today().date()-datetime.timedelta(7)).strftime('%Y-%m-%d')
Contents = pd.DataFrame()
Comments = pd.DataFrame()

for b in band_key_list:
    flag = 0
    band_key=b[1]
    band_name = b[0]
    content, after=first_page(band_key)
    row=make_data(content)
    while flag == 0 and after != 0:
        content, after = after_page(band_key,after)
        # print(after)
        row+=make_data(content)

    df= pd.DataFrame(row)
    df=df.rename(columns= {0:'author',1:'createdate',2:'postkey',3:'content',4:'hashtag',5:'commentcount',6:'emotioncount'})
    df['bandname'] = band_name

    postkeylist=df.postkey.tolist()
    row=[]
    for p in postkeylist:
        comment=get_comment(band_key,p)
        row+=get_each_comment(comment)

    df2=pd.DataFrame(row)
    df2=df2.rename(columns={0:'postkey',1:'createddate',2:'author',3:'comment'})
    df2['bandname'] = band_name    

    Contents=pd.concat([Contents,df],axis=0)
    Comments=pd.concat([Comments,df2],axis=0)

target='|'.join(['\n'])
Contents['content']=Contents['content'].str.replace(target,'')
Comments['comment']=Comments['comment'].str.replace(target,'')
Contents.reset_index(drop=True,inplace=True)
Comments.reset_index(drop=True,inplace=True)

Hashtags=get_hashtag_db(Contents)
Hashtags['hashtag'].replace('', np.nan, inplace=True)
Hashtags.dropna(subset=['hashtag'], inplace=True)
Hashtags.reset_index(drop=True,inplace=True)

#To_csv file
# Contents.to_csv("./csvfilename.csv",index= False,encoding = 'utf-8')
# Comments.to_csv("./csvfilename.csv",index=False,encoding = 'utf-8')
# Hashtags.to_csv("./csvfilename.csv",index=False)
