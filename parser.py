# -- coding: UTF-8 -- 
import sys
import os
import glob
import re
import fnmatch
import chardet

import langid
import datetime
import time
from datetime import datetime

import pysrt
from pysrt import SubRipTime
import pdb

import pymongo
from pymongo import MongoClient

import opencc

from multiprocessing import Pool


def checkEncoding(content):
    encoding=chardet.detect(content[:700])["encoding"]
    return encoding


def checkLanguage(content):
    lang="unknown"
    try:
        lang=langid.classify(content[600:1600])#.decode("utf-8") error
        return lang[0]
    except Exception as e:
        print e
        return "unknown"

def srttime2totaltime(srttime):
    return srttime.milliseconds / 1000.0 + srttime.seconds + srttime.minutes * 60 + srttime.hours * 3600


def compare_subtitles(en_str_parsed, zh_str_parsed):
     
    #we will return extraction
    extraction=[]

    effective_lines_zh_srt = len(zh_str_parsed)
    effective_lines_en_srt = len(en_str_parsed)

    #check from the 2nd line
    i, j = 0, 0
    while i < effective_lines_en_srt and j < effective_lines_zh_srt:

        #pdb.set_trace()
        if j>=80 or i>=80 :
            if (len(extraction)<30) :
                return None
        
        start_delta_t = srttime2totaltime(zh_str_parsed[j].start - en_str_parsed[i].start)

        end_delta_t = srttime2totaltime(zh_str_parsed[j].end - en_str_parsed[i].end)

        if start_delta_t < -1 :
            j += 1
        elif start_delta_t >= -1 and start_delta_t <= 1 :
            if abs(end_delta_t)<=1 :
                
                extraction.append([en_str_parsed[i].start, en_str_parsed[i].end,en_str_parsed[i].text.replace("\n", " ").encode('utf8'), zh_str_parsed[j].text.replace("\n", " ").encode('utf8')])
            i += 1
            j += 1
        else :
            i += 1
 
    return extraction

def checkBilingualzhSubtitles(parsed_content):
    
    zh_lines = 0
    en_lines = 0
    
    for line in parsed_content[3:13]:
        splitted_line=line.text.encode('utf-8').split('\n')
        
#        if(langid.classify(splitted_line[0])[0]=="zh") : zh_lines += 1
#        elif(langid.classify(splitted_line[0])[0]=="en") : en_lines += 1
        if(langid.classify(splitted_line[0])[0]=="en") : en_lines += 1
            
        try:
#            if(langid.classify(splitted_line[1])[0]=="zh") : zh_lines += 1
#            elif(langid.classify(splitted_line[1])[0]=="en") : en_lines += 1 
            if(langid.classify(splitted_line[1])[0]=="en") : en_lines += 1
        except:
            pass     
        
    for line in parsed_content[-13:-3]:
        splitted_line=line.text.encode('utf-8').split('\n')
        
#        if(langid.classify(splitted_line[0])[0]=="zh") : zh_lines += 1
#        elif(langid.classify(splitted_line[0])[0]=="en") : en_lines += 1
        if(langid.classify(splitted_line[0])[0]=="en") : en_lines += 1
            
        try:
#            if(langid.classify(splitted_line[1])[0]=="zh") : zh_lines += 1
#            elif(langid.classify(splitted_line[1])[0]=="en") : en_lines += 1   
            if(langid.classify(splitted_line[1])[0]=="en") : en_lines += 1
        except:
            pass
    
    if(en_lines) > 7 : 
        return True
    else: return False
    
#=================================================
#code start here to execute
#=================================================


def processFolder(foldercursor):
    filelist = glob.glob(foldercursor["address"]+srt_extension)
    folder_name=foldercursor["_id"]
    if len(filelist) == 0 : 
        movies_info_collection.update_one({'_id':folder_name},{"$set": {'finished':True}})
        movies_info_collection.update_one({'_id':folder_name},{"$set": {'srt_count':0}})
        return None #if no .srt file, skip

    movies_info_collection.update_one({'_id':folder_name},{"$set": {'srt_count':len(filelist)}})
    #=================================================
    #hint: reduce the read and write (io) to disk
    #=================================================
    movies_content_in_a_folder = []
    
    #create a list to store matched subtitles
    matched_content_in_a_folder = []
    for filename in filelist :
        try:
            
            with open(filename) as f : 
                
	        content = f.read()
                
                movie_content = {}
                
	        #print "check_encoding..."
                content_encoding = checkEncoding(content)
                if not content_encoding :
                    continue
                    
                #print "decoding..."
                movie_content["content"] = content.decode(content_encoding, 'ignore').encode("utf-8")
                
                #remove html tags
                pattern = re.compile('<[^>]*>|{[^}]*}')
                movie_content["content"]=pattern.sub('',movie_content["content"])
	        
                #print "check language..."
                movie_content["language"] = checkLanguage(movie_content["content"])
            
                #convert t-chinese to s-chinese
                #if movie_content["language"] == "zh" :
                #    except:
                #        pass
                #movie_content["content"] = opencc.convert(movie_content["content"]).encode("utf-8")
                
                #print "parsing srt..."
                movie_content["parsed_content"] = pysrt.from_string(movie_content["content"].decode("utf-8", 'ignore')) #pysrt.from_string(movie_content["content"],xencoding='utf_8') #pysrt.from_string(movie_content["content"].decode("utf-8", 'ignore'))
                
                movie_content["total_lines"] = len(movie_content["parsed_content"])
                
                
                movie_content["filename"] = filename
                
                movies_content_in_a_folder.append(movie_content)
        except Exception as e:
            print e

    #walk through all english subtitles
    
    #check if one srt file folder's srt file is zh and bilingual
    if len(movies_content_in_a_folder) == 1 :
        if movies_content_in_a_folder[0]['language'] == 'zh':
            if (checkBilingualzhSubtitles(movies_content_in_a_folder[0]['parsed_content'])):
                movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
    

    #TODO: change to set
    finished_en_subtitles = set()
    finished_zh_subtitles = set()
    
    #=================================================
    #start the iteration for every file in the folder
    #=================================================
    #check eng subtitles counts
    en_subtitle_counts = 0
    for en_movie_content in movies_content_in_a_folder:
        if en_movie_content["language"]!="en":
           continue
        else: en_subtitle_counts += 1
       
        if en_movie_content["total_lines"] in finished_en_subtitles:
           continue

        #print en_movie_content['filename']
         
        #print finished_en_subtitles
        #print finished_zh_subtitles

        #compare begins
        
        failed_zh_attempts=set() #record the failed zh srt file(s) for a en srt file
        # 1st, check the zh srt which has the same number of lines with this eng subtitle
        for zh_movie_content in movies_content_in_a_folder:
            if zh_movie_content["language"]!="zh" :
                continue           
            
            if zh_movie_content["total_lines"] != en_movie_content["total_lines"] : 
                continue           
            
            #this line is optional
            if zh_movie_content["total_lines"] in failed_zh_attempts :  
                continue   
                
            result = compare_subtitles(en_movie_content['parsed_content'], zh_movie_content['parsed_content'])
            
            if result :
                
                #if this is a bilingual subtitle, delete eng content in the zh srt file
                if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
                    for line in result:
                        line[3] = line[3].replace(line[2],"")
                    movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
                
                matched_content_in_a_folder = matched_content_in_a_folder + result
                finished_en_subtitles.add(en_movie_content["total_lines"])
                finished_zh_subtitles.add(zh_movie_content["total_lines"])
                movies_info_collection.update_one({'_id':folder_name},{"$set": {'matchedd':True}})
                break
            else : 
                failed_zh_attempts.add(zh_movie_content["total_lines"])

        # if this eng sub has been successfully processed, skip to next eng subtitle
        if en_movie_content["total_lines"] in finished_en_subtitles : 
            continue

        # if this eng sub has not been paired, check zh srt with lines other than the same lines         
        for zh_movie_content in movies_content_in_a_folder:
            if zh_movie_content["language"]!="zh":continue
            if zh_movie_content["total_lines"] in finished_zh_subtitles:continue#saves time, but optional
            if zh_movie_content["total_lines"] in failed_zh_attempts:continue
            
            #print "begin to compare:",en_movie_content['filename'], zh_movie_content['filename']
            


            result = compare_subtitles(en_movie_content['parsed_content'], zh_movie_content['parsed_content'])

            if result :
                
                #if this is a bilingual subtitle, delete eng content in the zh srt file
                if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
                    for line in result:
                        line[3] = line[3].replace(line[2],"")
                    movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
                
                
                matched_content_in_a_folder = matched_content_in_a_folder + result
                finished_en_subtitles.add(en_movie_content["total_lines"])
                finished_zh_subtitles.add(zh_movie_content["total_lines"])
                movies_info_collection.update_one({'_id':folder_name},{"$set": {'matched':True}})
                break
            else :
                failed_zh_attempts.add(zh_movie_content["total_lines"])

    #if there is no eng subtitles
    if en_subtitle_counts == 0:
        movies_info_collection.update_one({'_id':folder_name},{"$set": {'no_en_subtitle':True}})
        #check if there is bilingual zh-en subtitle
        for zh_movie_content in movies_content_in_a_folder:
            if zh_movie_content["language"]!="zh":
                continue
            if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
                movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
                break
    #TODO: output
    
    if matched_content_in_a_folder:
        for line in matched_content_in_a_folder:
            print "%s%f%f%s%s" % (folder_name.encode("utf8"), srttime2totaltime(line[0]), srttime2totaltime(line[1]), line[2], line[3])
    #mark this folder as processed
    movies_info_collection.update_one({'_id':folder_name},{"$set": {'finished':True}})


if len(sys.argv)>1 :
    path=sys.argv[1]
else :
    print "usage: parse.py dirname"
    sys.exit(1)

print "listing dirs ..."
folderlist=glob.glob(path+"/*")
print "init language detector ..."

srt_extension="/*.[Ss][Rr][Tt]"

# connect to the mongo database and open the corresponding collection
client = MongoClient()
db = client.moviedb
movies_info_collection = db.moviesinfocollection

# if folder name/(movie info) is not in the database, then add it into the DB
print "refershing database"
for folder in folderlist:
    folderid = folder.split('/')[-1]
    if not movies_info_collection.find_one({"_id": folderid}):
        movies_info_collection.insert_one({"_id": folderid, "finished": False,"address": folder, "no_en_subtitle": False})
        


#query the unfinished folder list
find_res_list = []
for cursor in movies_info_collection.find({"finished":False}):
    find_res_list.append(cursor)

p = Pool(4)
print "i am here!!before mapping"
p.map(processFolder,find_res_list)
#for cursor in find_res_list:
#    filelist = glob.glob(cursor["address"]+srt_extension)
#    folder_name=cursor["_id"]
#    if len(filelist) == 0 : 
#        movies_info_collection.update_one({'_id':folder_name},{"$set": {'finished':True}})
#        movies_info_collection.update_one({'_id':folder_name},{"$set": {'srt_count':0}})
#        continue #if no .srt file, skip
#
#    movies_info_collection.update_one({'_id':folder_name},{"$set": {'srt_count':len(filelist)}})
#    #=================================================
#    #hint: reduce the read and write (io) to disk
#    #=================================================
#    movies_content_in_a_folder = []
#    
#    #create a list to store matched subtitles
#    matched_content_in_a_folder = []
#    for filename in filelist :
#        try:
#            
#            with open(filename) as f : 
#                
#	        content = f.read()
#                
#                movie_content = {}
#                
#	        #print "check_encoding..."
#                content_encoding = checkEncoding(content)
#                if not content_encoding :
#                    continue
#                    
#                #print "decoding..."
#                movie_content["content"] = content.decode(content_encoding, 'ignore').encode("utf-8")
#                
#                #remove html tags
#                pattern = re.compile(<[^>]*>|{[^}]*})
#                movie_content["content"]=pattern.sub('',movie_content["content"])
#	        
#                #print "check language..."
#                movie_content["language"] = checkLanguage(movie_content["content"])
#            
#                #convert t-chinese to s-chinese
#                #if movie_content["language"] == "zh" :
#                #movie_content["content"] = opencc.convert(movie_content["content"]).encode("utf-8")
#                
#                #print "parsing srt..."
#                movie_content["parsed_content"] = pysrt.from_string(movie_content["content"].decode("utf-8", 'ignore')) #pysrt.from_string(movie_content["content"],xencoding='utf_8') #pysrt.from_string(movie_content["content"].decode("utf-8", 'ignore'))
#                
#                movie_content["total_lines"] = len(movie_content["parsed_content"])
#                
#                
#                movie_content["filename"] = filename
#                
#                movies_content_in_a_folder.append(movie_content)
#        except Exception as e:
#            print e
#
#    #walk through all english subtitles
#    
#    #check if one srt file folder's srt file is zh and bilingual
#    if len(movies_content_in_a_folder) == 1 :
#        if movies_content_in_a_folder[0]['language'] == 'zh':
#            if (checkBilingualzhSubtitles(movies_content_in_a_folder[0]['parsed_content'])):
#                movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
#    
#
#    #TODO: change to set
#    finished_en_subtitles = set()
#    finished_zh_subtitles = set()
#    
#    #=================================================
#    #start the iteration for every file in the folder
#    #=================================================
#    #check eng subtitles counts
#    en_subtitle_counts = 0
#    for en_movie_content in movies_content_in_a_folder:
#        if en_movie_content["language"]!="en":
#           continue
#        else: en_subtitle_counts += 1
#       
#        if en_movie_content["total_lines"] in finished_en_subtitles:
#           continue
#
#        #print en_movie_content['filename']
#         
#        #print finished_en_subtitles
#        #print finished_zh_subtitles
#
#        #compare begins
#        
#        failed_zh_attempts=set() #record the failed zh srt file(s) for a en srt file
#        # 1st, check the zh srt which has the same number of lines with this eng subtitle
#        for zh_movie_content in movies_content_in_a_folder:
#            if zh_movie_content["language"]!="zh" :
#                continue           
#            
#            if zh_movie_content["total_lines"] != en_movie_content["total_lines"] : 
#                continue           
#            
#            #this line is optional
#            if zh_movie_content["total_lines"] in failed_zh_attempts :  
#                continue   
#                
#            result = compare_subtitles(en_movie_content['parsed_content'], zh_movie_content['parsed_content'])
#            
#            if result :
#                
#                #if this is a bilingual subtitle, delete eng content in the zh srt file
#                if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
#                    for line in result:
#                        line[3] = line[3].replace(line[2],"")
#                    movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
#                
#                matched_content_in_a_folder = matched_content_in_a_folder + result
#                finished_en_subtitles.add(en_movie_content["total_lines"])
#                finished_zh_subtitles.add(zh_movie_content["total_lines"])
#                movies_info_collection.update_one({'_id':folder_name},{"$set": {'matchedd':True}})
#                break
#            else : 
#                failed_zh_attempts.add(zh_movie_content["total_lines"])
#
#        # if this eng sub has been successfully processed, skip to next eng subtitle
#        if en_movie_content["total_lines"] in finished_en_subtitles : 
#            continue
#
#        # if this eng sub has not been paired, check zh srt with lines other than the same lines         
#        for zh_movie_content in movies_content_in_a_folder:
#            if zh_movie_content["language"]!="zh":continue
#            if zh_movie_content["total_lines"] in finished_zh_subtitles:continue#saves time, but optional
#            if zh_movie_content["total_lines"] in failed_zh_attempts:continue
#            
#            #print "begin to compare:",en_movie_content['filename'], zh_movie_content['filename']
#            
#
#
#            result = compare_subtitles(en_movie_content['parsed_content'], zh_movie_content['parsed_content'])
#
#            if result :
#                
#                #if this is a bilingual subtitle, delete eng content in the zh srt file
#                if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
#                    for line in result:
#                        line[3] = line[3].replace(line[2],"")
#                    movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
#                
#                
#                matched_content_in_a_folder = matched_content_in_a_folder + result
#                finished_en_subtitles.add(en_movie_content["total_lines"])
#                finished_zh_subtitles.add(zh_movie_content["total_lines"])
#                movies_info_collection.update_one({'_id':folder_name},{"$set": {'matched':True}})
#                break
#            else :
#                failed_zh_attempts.add(zh_movie_content["total_lines"])
#
#    #if there is no eng subtitles
#    if en_subtitle_counts == 0:
#        movies_info_collection.update_one({'_id':folder_name},{"$set": {'no_en_subtitle':True}})
#        #check if there is bilingual zh-en subtitle
#        for zh_movie_content in movies_content_in_a_folder:
#            if zh_movie_content["language"]!="zh":
#                continue
#            if (checkBilingualzhSubtitles(zh_movie_content['parsed_content'])):
#                movies_info_collection.update_one({'_id':folder_name},{"$set": {'bilingual_zh_en':True}})
#                break
#    #TODO: output
#    
#    if matched_content_in_a_folder:
#        for line in matched_content_in_a_folder:
#            print "%s%f%f%s%s" % (folder_name.encode("utf8"), srttime2totaltime(line[0]), srttime2totaltime(line[1]), line[2], line[3])
#    #mark this folder as processed
#    movies_info_collection.update_one({'_id':folder_name},{"$set": {'finished':True}})
