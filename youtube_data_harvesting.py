##importing the google client data
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

### Modifying the API key into the Youtube variable and collecting the information
def Api_connect():
  Api_Id="AIzaSyBRuGiiMtUSUsBzR1mjjCCTw_qeJKIszXo"#API key have a quota limit try to get information form small channels
  api_service_name="youtube"
  api_version="v3"
  youtube=build(api_service_name,api_version,developerKey=Api_Id)

  return youtube
youtube=Api_connect()

### Pulling channel information and dictionary file making
def get_Channel_info(Channel_Id):
  request=youtube.channels().list(
              part="snippet,ContentDetails,statistics",
              id=Channel_Id
  )
  response=request.execute()

  #dictionary file making

  for i in response['items']:
    data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subcribers=i["statistics"]["subscriberCount"],
              View=i["statistics"]["viewCount"],
              Total_Videos=i["statistics"]["videoCount"],
              Channel_description=i["snippet"]["description"],
              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
  
### Pulling channel Video ID's and information
# get video Id's and paly list Id's
def get_video_Ids(Channel_Id):
  video_ids=[]
  response=youtube.channels().list(id=Channel_Id,
                                  part='contentDetails').execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  #page token used for loop the results get all video' data from channels
  next_page_token=None

  # max result 50 default is 5 while loop gets all results
  while True:
    response_of_playlist=youtube.playlistItems().list(
                                                  part='snippet',
                                                  playlistId=Playlist_Id,
                                                  maxResults=50,
                                                  pageToken=next_page_token).execute()
    for i in range(len(response_of_playlist['items'])):
                              video_ids.append(response_of_playlist['items'][i]['snippet']['resourceId']['videoId'])
    #getting nextPageToken Id's
    next_page_token = response_of_playlist.get('nextPageToken')

    if next_page_token is None:#break the loop if all data will received
      break
  return video_ids

### Pulling Channel video information using with Video ID's
def get_video_info(video_ids):#use the result of the print value in video ID variable not channel ID
        Video_data =[]
        for video_id in video_ids:# for loop is used for get all datas of videos and its loops the function
                request= youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id = video_id
                )
                response = request.execute()
        # For loop the separate specific details of each video
                for item in response['items']:
                        data=dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Descriptions=item['snippet'].get('description'),
                        Publish_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics']['viewCount'],
                        likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
                Video_data.append(data)
        return Video_data # return the video data function is get_video_Info

### Pulling Channel Comment information using with Video ID's
def get_comment_Info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:# for loop is used for get all datas of videos and its loops the function
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            # For loop the separate specific details of each video
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                          Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_data.append(data)
    except:
        pass
    return Comment_data # return the video data function is get_comment_Info

### Pulling Channel Play List iformation Using with Video ID's
def get_playlist_Info(channel_Id):
  #page token used for loop the results get all playlist' data from channels
  next_page_token = None
  Play_list_data = []
  # max result 50 default is 5 while loop gets all results
  while True:
      request = youtube.playlists().list(
          part='snippet,contentDetails',
          channelId=channel_Id,
          maxResults=50
      )
      response = request.execute()
      for item in response['items']:#slicing the data's
          data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_name=item['snippet']['channelTitle'],
                        Published_At=item['snippet']['publishedAt'],
                        Video_count=item['contentDetails']['itemCount'])
          Play_list_data.append(data)
      #getting nextPageToken Id's
      next_page_token = response.get('nextPageToken')
      if next_page_token is None:
          break  #break the loop if all data will received
  return Play_list_data # return the video data function is get_video_Info

# Connecting MongoDB and create client data
client = pymongo.MongoClient("mongodb+srv://sarathkumar:kumarsarath@cluster0.rivob1w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db=client["youtube_data"]

#define mongoDB cannel information and upload the data to MongoDB
def channel_details(channel_Id):
    channel_info=get_Channel_info(channel_Id)
    playlist_info=get_playlist_Info(channel_Id)
    vd_ids=get_video_Ids(channel_Id)
    video_info=get_video_info(vd_ids)
    comment_info=get_comment_Info(vd_ids)

    collection_1=db["channel_details"]
    collection_1.insert_one({"channel_information":channel_info,"playlist_information":playlist_info,"video_ID's":vd_ids,"video_information":video_info,"comment_information":comment_info})

    return "Details fetch successfully"

#PostgresSQL table creation using with channel_details
def channel_tables(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kumarsarath",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    # creating a headings of table creations in postgres_sql
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subcribers bigint,
                                                            View bigint,
                                                            Total_Videos int,
                                                            Channel_description text,
                                                            Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()
    

    #using pandas to change the data of youtube entire data into a datafram method and migrate to postgres_sql
    list_of_unique_ch=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]
    for ch_data in collection_1.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
        list_of_unique_ch.append(ch_data["channel_information"])

    df_list_of_unique_ch= pd.DataFrame(list_of_unique_ch)

    # addin information of channel details as row to postgres_sql
    for index,row in df_list_of_unique_ch.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subcribers,
                                            View,
                                            Total_Videos,
                                            Channel_description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subcribers'],
                row['View'],
                row['Total_Videos'],
                row['Channel_description'],
                row['Playlist_Id'])
        
        # duplicate error handleing 
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
             news=f"Your provided channel name {channel_name_s} is already exist"
        return news
         
            
        

#PostgresSQL table creation using with playlist_details
def playlist_table(channels_name_s):
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="kumarsarath",
                                database="youtube_data",
                                port="5432")
        cursor=mydb.cursor()

        # creating a headings of table creations in postgres_sql
        create_query='''create table if not exists playlist(Playlist_Id varchar(100)primary key,
                                                                Title varchar(80) ,
                                                                Channel_Id varchar(100),
                                                                Channel_name varchar(100),
                                                                Published_At timestamp,
                                                                Video_count int)'''
        cursor.execute(create_query)
        mydb.commit()
        
        #using pandas to change the data of youtube entire data into a datafram method and migrate to postgres_sql
        list_of_unique_plst=[]
        db=client["youtube_data"]
        collection_1=db["channel_details"]
        for ch_data in collection_1.find({"channel_information.Channel_Name": channels_name_s},{"_id":0}):
                list_of_unique_plst.append(ch_data["playlist_information"])
        df_list_of_unique_plst=pd.DataFrame(list_of_unique_plst[0])

        # addin information of playlist details as row to postgres_sql
        for index,row in df_list_of_unique_plst.iterrows():
                insert_query='''insert into playlist(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_name,
                                                Published_At,
                                                Video_count)

                                                values(%s,%s,%s,%s,%s,%s)'''
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_name'],
                        row['Published_At'],
                        row['Video_count'])
                
                cursor.execute(insert_query,values)
                mydb.commit()

#PostgresSQL table creation using with video_details
def video_table(channels_name_s):
    mydb=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="kumarsarath",
                                    database="youtube_data",
                                    port="5432")
    cursor=mydb.cursor()

    # creating a headings of table creations in postgres_sql
    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                                    Channel_Id varchar(100),
                                                                    Video_Id varchar(30) primary key,
                                                                    Title varchar(150),
                                                                    Tags text,
                                                                    Thumbnail varchar(200),
                                                                    Descriptions text,
                                                                    Publish_Date timestamp,
                                                                    Duration interval,
                                                                    Views bigint,
                                                                    likes bigint,
                                                                    Comments int,
                                                                    Favorite_Count int,
                                                                    Definition varchar(10),
                                                                    Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()

    #using pandas to change the data of youtube entire data into a datafram method and migrate to postgres_sql
    list_of_unique_vi=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]
    for ch_data in collection_1.find({"channel_information.Channel_Name": channels_name_s},{"_id":0}):
            list_of_unique_vi.append(ch_data["video_information"])
    df_list_of_unique_vi= pd.DataFrame(list_of_unique_vi[0])


    # addin information of playlist details as row to postgres_sql
    for index,row in df_list_of_unique_vi.iterrows():
                    insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Descriptions,
                                                        Publish_Date,
                                                        Duration,
                                                        Views,
                                                        likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status)

                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values=(row['Channel_Name'],
                            row['Channel_Id'],
                            row['Video_Id'],
                            row['Title'],
                            row['Tags'],
                            row['Thumbnail'],
                            row['Descriptions'],
                            row['Publish_Date'],
                            row['Duration'],
                            row['Views'],
                            row['likes'],
                            row['Comments'],
                            row['Favorite_Count'],
                            row['Definition'],
                            row['Caption_Status'])
                    
                    cursor.execute(insert_query,values)
                    mydb.commit()

#PostgresSQL table creation using with comment_details.
def comment_table(channels_name_s):
    mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="kumarsarath",
                                database="youtube_data",
                                port="5432")
    cursor=mydb.cursor()

    # creating a headings of table creations in postgres_sql
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                    Video_Id varchar(50),
                                                                    Comment_Text text,
                                                                    Comment_Author varchar(150),
                                                                    Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

        #using pandas to change the data of youtube entire data into a datafram method and migrate to postgres_sql

    list_of_unique_comd=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]
    for ch_data in collection_1.find({"channel_information.Channel_Name": channels_name_s},{"_id":0}):
                list_of_unique_comd.append(ch_data["comment_information"])

    df_list_of_unique_comd= pd.DataFrame(list_of_unique_comd[0])


        # addin information of playlist details as row to postgres_sql
    for index,row in df_list_of_unique_comd.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                     Video_Id,
                                                     Comment_Text,
                                                     Comment_Author,
                                                     Comment_Published)

                                                values(%s,%s,%s,%s,%s)'''
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'],
                        )
                
                cursor.execute(insert_query,values)
                mydb.commit()

# combine all functions into one function
def tables(unique_channels_values):
    news=channel_tables(unique_channels_values)
    if news:
         return news
    else:
        playlist_table(unique_channels_values)
        video_table(unique_channels_values)
        comment_table(unique_channels_values)

        return "Tables Created Successfuly"

#sending information to Stermlit web app
def show_channel_tables():
    channel_list=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]

    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])

    df=st.dataframe(channel_list)

    return df

#sending information to Stermlit web app
def show_playlist_tables():
    playlist_list=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]

    for playlist_data in collection_1.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(playlist_data["playlist_information"])):
                    playlist_list.append(playlist_data["playlist_information"][i])

    df1 =st.dataframe(playlist_list)

    return df1

#sending information to Stermlit web app
def show_videos_tables():
    video_list=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]

    for video_data in collection_1.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])

    df2 =st.dataframe(video_list)

    return df2

#sending information to Stermlit web app
def show_comments_tables():
        comment_list=[]
        db=client["youtube_data"]
        collection_1=db["channel_details"]

        for comment_data in collection_1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(comment_data["comment_information"])):
                        comment_list.append(comment_data["comment_information"][i])

        df3 =st.dataframe(comment_list)

        return df3

#preparing the stermlit web app
with st.sidebar:
    st.title(":green[YouTube Data Harvesting and Warehousing]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Intergration")
    st.caption("Data Management using MongoDB and SQL")

channel_id =st.text_input("Enter the Channel ID")

if st.button("Collect data in MongoDB"):
    ch_id=[]
    db=client["youtube_data"]
    collection_1=db["channel_details"]
    for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
        ch_id.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_id:
        st.success("Channel details are already provided please try new channel ID")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channels=[]
db=client["youtube_data"]
collection_1=db["channel_details"]

for ch_data in collection_1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])

unique_channel=st.selectbox("Select specific channels",all_channels)

if st.button("Switch data to SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_tables()

elif show_table=="PLAYLISTS":
    show_playlist_tables()

elif show_table=="VIDEOS":
    show_videos_tables()

elif show_table=="COMMENTS":
    show_comments_tables()

#SQL query check and connections
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="kumarsarath",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

questions=st.selectbox("Select your questions",("1. What are the names of all the videos and their corresponding channels",
                                                "2. Which channels have the most number of videos, and how many videos do they have",
                                                "3. What are the top 10 most viewed videos and their respective channels",
                                                "4. How many comments were made on each video, and what are their corresponding video names",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names",
                                                "8. What are the names of all the channels that have published videos in the year 2022",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names"))

if questions=="1. What are the names of all the videos and their corresponding channels":
    questions_1 = '''select title as video,channel_name as channelname from videos'''
    cursor.execute(questions_1)
    mydb.commit()
    tabel_1=cursor.fetchall()

    df=pd.DataFrame(tabel_1,columns=["video title","channel name"])
    st.write(df)

elif questions=="2. Which channels have the most number of videos, and how many videos do they have":
    questions_2 = '''select channel_name as channelname,total_videos as no_videos from channels
                    order by total_videos desc'''
    cursor.execute(questions_2)
    mydb.commit()
    tabel_2=cursor.fetchall()

    df2=pd.DataFrame(tabel_2,columns=["channel name","NO of videos"])
    st.write(df2)

elif questions=="3. What are the top 10 most viewed videos and their respective channels":
    questions_3 = '''select views as views, channel_name as channelname,title as videotile from videos
                        where views is not null order by views desc limit 10 '''
    cursor.execute(questions_3)
    mydb.commit()
    tabel_3=cursor.fetchall()

    df3=pd.DataFrame(tabel_3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif questions=="4. How many comments were made on each video, and what are their corresponding video names":
    questions_4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(questions_4)
    mydb.commit()
    tabel_4=cursor.fetchall()

    df4=pd.DataFrame(tabel_4,columns=["no_comments","videotile"])
    st.write(df4)

elif questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names":
    questions_5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                        from videos where likes is not null order by likes desc'''
    cursor.execute(questions_5)
    mydb.commit()
    tabel_5=cursor.fetchall()

    df5=pd.DataFrame(tabel_5,columns=["videotitle","channelname","likecount"])
    st.write(df5)
# the Question is get number likes and dislikes in each videos but youtube is hided the dislikes data for community purpose
elif questions=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names":
    questions_6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(questions_6)
    mydb.commit()
    tabel_6=cursor.fetchall()

    df6=pd.DataFrame(tabel_6,columns=["likecount","videotitle"])
    st.write(df6)
elif questions=="7. What is the total number of views for each channel, and what are their corresponding channel names":
    questions_7 = '''select channel_name as channelname,view as totalviews from channels'''
    cursor.execute(questions_7)
    mydb.commit()
    tabel_7=cursor.fetchall()

    df7=pd.DataFrame(tabel_7,columns=["channelname","totalviews"])
    st.write(df7) 
elif questions=="8. What are the names of all the channels that have published videos in the year 2022":
    questions_8 = '''select title as video_title, Publish_Date as videopublished, channel_name as channelname from videos
                        where extract(year from Publish_Date)=2022'''
    cursor.execute(questions_8)
    mydb.commit()
    tabel_8=cursor.fetchall()

    df8=pd.DataFrame(tabel_8,columns=["video_title","videopublished","channelname"])
    st.write(df8)
elif questions=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names":
    questions_9 = '''select channel_name as channelname, AVG(Duration) as avgdurationofvideo from videos group by channel_name'''
    cursor.execute(questions_9)
    mydb.commit()
    tabel_9=cursor.fetchall()

    df9=pd.DataFrame(tabel_9,columns=["channelname","avgdurationofvideo"])
    # change the avg duration value time format into str format to uploade the data streamlit 
    Time9=[]
    for index,row in df9.iterrows():
        channel_titel=row["channelname"]
        average_duration=row["avgdurationofvideo"]
        average_duration_str=str(average_duration)
        Time9.append(dict(channeltitle=channel_titel,avgduration=average_duration_str))
    df9_1=pd.DataFrame(Time9)
    st.write(df9_1)
elif questions=="10. Which videos have the highest number of comments, and what are their corresponding channel names":
    questions_10 = '''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(questions_10)
    mydb.commit()
    tabel_10=cursor.fetchall()

    df10=pd.DataFrame(tabel_10,columns=["videotitle","channelname","comments"])
    st.write(df10)