#!/usr/bin/python3
import time
import os
import datetime
import threading
import sqlite3
import json
import sys


#---------THREAD-----------------
class ParrallelWorker (threading.Thread):
    def __init__(self, threadID, datas, pgnsToWork, dictionary, name, ts):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.datas = datas
        self.pgnsToWork = pgnsToWork
        self.dictionary = dictionary
        self.name = name
        self.ts = ts

    def run(self):
        forRedis = main_thread(self.datas, self.pgnsToWork, self.name)
        export_on_csv(forRedis, self.dictionary, self.name, self.ts)

        

#-----------FUNCTION-------------
def create_connection_sqlite():
    conn = None
    try:
        conn = sqlite3.connect('storico.sqlite3')
    except Exception as e:
        print(e)
    return conn


def create_connection_redis():
    redisClient = ""
    try:
        redisClient = redis.Redis(host='10.70.0.2', port=6379, db=3)
    except redis.exceptions.DataError as e:
        print(e)
    return redisClient

def get_important_pgns():
    pgns = ""
    try:
        conn = create_connection_sqlite()
        sql = """SELECT PGN, SRC, Instance, LookValue, `Variable`, RedisVarLink, `Key`, LookupPreReverse, LookupVar, RedisVariable
        FROM ToProcess WHERE isNumeric = 'si' AND ToShow = 'si'"""
        cur = conn.cursor()
        cur.execute(sql)
        pgns = cur.fetchall()
        conn.close()
    except Exception as er:
        conn.close()
        errors("get_important_pgns", str(er))
    return pgns


def get_lookup_list():
    try:
        conn = create_connection_sqlite()
        sql = "SELECT * FROM LookupList"
        cur = conn.cursor()
        cur.execute(sql)
        looklist = cur.fetchall()
        conn.close()
    except Exception as er:
        conn.close()
        errors("get_important_pgns", str(er))
    return looklist


def get_lookup_translation():
    try:
        conn = create_connection_sqlite()
        sql = "SELECT * FROM LookupTranslation"
        cur = conn.cursor()
        cur.execute(sql)
        lookt = cur.fetchall()
        conn.close()
    except Exception as er:
        conn.close()
        errors("get_important_pgns", str(er))
    return lookt


def clean_string(data):
    tmp = data
    tmp = tmp.replace("!", "")
    tmp = tmp.replace("\'", "")
    tmp = tmp.replace("\"", "")
    tmp = tmp.replace("/", "")
    tmp = tmp.replace("&", "")
    tmp = tmp.replace("(", "")
    tmp = tmp.replace(")", "")
    tmp = tmp.replace("^", "")
    tmp = tmp.replace("%", "")
    tmp = tmp.replace(":", "")
    tmp = tmp.replace(";", "")
    tmp = tmp.replace("@", "")
    tmp = tmp.replace("#", "")
    tmp = tmp.replace("$", "")
    tmp = tmp.replace("*", "")
    tmp = tmp.replace("~", "")
    tmp = tmp.replace(" ", "")
    return tmp


def main_thread(datas, impPGNS, nome):
    try:
        toBeMeaned = []
        worked = []
        for data in datas:
            for pgn in impPGNS:

                pgnn = pgn[0]
                src = pgn[1]
                instance = pgn[2]
                lookVal = pgn[3]
                variable = pgn[4]
                rlink = pgn[5]
                key = pgn[6]
                lPreReverse = pgn[7]
                lookVariable = pgn[8]
                rVariable = pgn[9]

                if str(data["pgn"]) == str(pgnn):
                    if str(data["src"]) == str(src):
                        if key not in worked:
                            #here we know if there are Instaces / lookup
                            #by counting element we value the path to follow
                            # if len = 3 ==> no instance no lookup
                            # if len = 4 ==> or instance or lookup
                            # if len = 5 ==> booth
                            counter = len(key.split("_"))
                            ts = str(data["timestamp"])
                            ts = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
                            ts = datetime.datetime.timestamp(ts)

                            if counter == 3:
                                try:
                                    value = data["fields"][variable]
                                    preparedData = (key, value, rlink, rVariable, ts)
                                    toBeMeaned.append(preparedData)
                                    worked.append(key)
                                except Exception as ex:
                                    errors("main_thread","Nome: " + nome + " Key: "+key + " - Error(c3): "+str(ex))

                            if counter == 4:
                                if "no lookupvalue" in str(lookVal):
                                    # instance
                                    if str(instance) == str(data["fields"]["Instance"]):
                                        try:
                                            value = data["fields"][variable]
                                            preparedData = (key, value, rlink, rVariable, ts)
                                            toBeMeaned.append(preparedData)
                                            worked.append(key)
                                        except Exception as ex:
                                            errors("main_thread","Nome: " + nome + " Key: "+key + " - Error(c4): "+str(ex))

                                else:
                                    # lookup
                                    if str(lookVal) == str(data["fields"][lookVariable]) or str(lPreReverse) == str(data["fields"][lookVariable]):
                                        try:
                                            value = data["fields"][variable]
                                            preparedData = (key, value, rlink, rVariable, ts)
                                            toBeMeaned.append(preparedData)
                                            worked.append(key)
                                        except Exception as ex:
                                            errors("main_thread","Nome: " + nome + " Key: "+key + " - Error(c4): "+str(ex))
                                        # print("Debug: Key: "+ str(key) +" "+str(data["fields"][lookVariable])+str(lookVal)+str(lPreReverse))


                            if counter == 5:
                                if str(instance) == str(data["fields"]["Instance"]) and (str(lookVal) == str(data["fields"][lookVariable]) or str(lPreReverse) == data["fields"][lookVariable]):
                                        worked.append(key)
                                        try:
                                            value = data["fields"][variable]
                                            preparedData = (key, value, rlink, rVariable, ts)
                                            toBeMeaned.append(preparedData)
                                        except Exception as ex:
                                            errors("main_thread","Nome: " + nome + " Key: "+key + " - Error(c5): "+str(ex))

        return toBeMeaned
    except Exception as ex:
        errors("main_Thread",str(ex))


def export_on_redis(datas):
    # logs("export_on_redis", "Scrittura dati su redis: ")
    try:
        redisClient = create_connection_redis()
        for d in datas:

            key = d[2]
            variable = d[3]
            value = d[1]
            ts = d[4]

            redisClient.hset("sailadv/data/" + key, variable, value)
            redisClient.hset("sailadv/device/" + key, "data_ts", ts)
            redisClient.hset("sailadv/device/" + key, "connected", "true")
            redisClient.close()

            logs("export_on_redis", str(d[0]) + " " + str(key) + " " +str(variable) + " " + str(value))
    except Exception as ex:
        errors("export_on_redis", "error in redis function - Error code: "+str(ex))


def export_on_csv(datas, dictionary, nome, ts):
    try:
        outPath = "out/"
        f = open(outPath+nome+".csv", "a")

        for d in datas:
            dictionary[d[0]] = d[1]

        ts = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        toWrite = str(ts)+";"

        for k in dictionary.keys():
            toWrite += str(dictionary[k])+";"
        
        toWrite = toWrite[:-1] + "\n"
        f.write(toWrite)
        f.close()


    except Exception as ex:
        errors("export_on_csv", "Error code: "+str(ex))


def generare_csv(dictionary,nome):
    try:
        outPath = "out/"
        f = open(outPath+nome+".csv", "a")
        toWrite = "timestamp;"

        for k in dictionary.keys():
            toWrite += k + ";"

        toWrite = toWrite[:-1] + "\n"
        f.write(toWrite)
        f.close()
    except Exception as ex:
        error_flag_file(("%"+nome+"%",))
        errors("generatecsv", "Error code: "+str(ex))



def insert_data_in_fw(dat):
    conn = create_connection_sqlite()
    try:
        sql = ''' INSERT OR IGNORE INTO FolderW(FileName,Status)
                VALUES(?,?) '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
    except Exception as er:
        conn.rollback()
        if not "Duplicate" in str(er):
            print(er)  # print error only if is different to UNIQUE constraint failed
    finally:
        conn.close()
    return cur.lastrowid


def lock_file(dat):
    try:
        conn = create_connection_sqlite()
        sql = ''' UPDATE FolderW SET Status = 'working' WHERE FileName = ? '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
        conn.close()
    except Exception as er:
        conn.rollback()
        print(er)
    finally:
        conn.close()
    return cur.lastrowid


def error_flag_file(dat):
    try:
        conn = create_connection_sqlite()
        sql = ''' UPDATE FolderW SET ErrorFlag = '1' WHERE FileName LIKE ? '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
        conn.close()
    except Exception as er:
        conn.rollback()
        print(er)
    finally:
        conn.close()
    return cur.lastrowid


def file_worked(dat):
    print("worked: " + str(dat[0]))
    try:
        conn = create_connection_sqlite()
        sql = ''' UPDATE FolderW SET Status = 'worked' WHERE FileName = ? '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
        conn.close()
    except Exception as er:
        conn.rollback()
        print(er)
    finally:
        conn.close()
    return cur.lastrowid


def scan_folder(pathname):
    for log in os.listdir(pathname):
        if "output" in log and ".log" in log:  # open every file named output... in directory
            insert_data_in_fw((pathname + "/" + log, "to be worked"))


def get_files_ordered():
    out = ""
    try:
        conn = create_connection_sqlite()
        sql = """SELECT * FROM FolderW WHERE Status = 'to be worked' ORDER BY (FileName) """

        cur = conn.cursor()
        cur.execute(sql)
        out = cur.fetchall()
        conn.close()
    except Exception as er:
        conn.close()
        errors("get_files_ordered", str(er))
    return out


def initialize_dictionary(datas):
    dictionary = {}
    for d in datas:
        dictionary[d[6]] = ""
    return dictionary


def timestamp_in_unix(timest):
    # Convert to unix time
    ts = datetime.datetime.strptime(str(timest), "%Y-%m-%dT%H:%M:%S.%fZ")
    ts = datetime.datetime.timestamp(ts)
    ts = int(float(ts))
    return ts


def logs(func, text):
    try:
        timest = str(time.asctime(time.localtime(time.time())))
        with open("log.log", "a") as f:
            f.write("time: "+timest+" # "+func+" # "+str(text)+"\n")
    except Exception:
        errors("logs", "error in log function")


def errors(func, text):
    try:
        timest = str(time.asctime(time.localtime(time.time())))
        with open("error.log", "a") as f:
            f.write("time: "+timest+" # "+func+" # "+str(text)+"\n")
    except Exception:
        print("ma che sfiga!")


def debugs(text):
    try:
        with open("debug.log", "a") as f:
            f.write(text)
    except Exception:
        errors("debugs", "error in log function")


#---------MAIN----------------
lock = threading.Lock()
pgnsToWork = get_important_pgns()
lookupList = get_lookup_list()
lookupPGNList = []
dictionary = initialize_dictionary(pgnsToWork)
for l in lookupList:
    lookupPGNList.append(l[0])
lookTranslation = get_lookup_translation()
# print(lookupList) testata
samplingTime = 5  # campione
threadId = 0
dataDir = "Data"
# dataDir = str(sys.argv[1])

scan_folder(dataDir)
filestw = get_files_ordered()
for filetw in filestw:
    lock_file((filetw[0],))
    print("start working: " + filetw[0])
    try:
        generare_csv(dictionary,filetw[0].replace(dataDir+"/", ""))
        jData = open(filetw[0], 'r')
        lines = jData.readlines()
        tmp = json.loads(lines[0])
        bottomTs = timestamp_in_unix(tmp["timestamp"])
    except Exception as ex:
        print(str(ex))
        # error_flag_file((filetw[0],))
    
    while (bottomTs % 5) != 0:
        bottomTs += 1

    topTs = bottomTs + samplingTime

    dataToWork = []

    for l in lines:
        try:
            if "fields" in l:
                data = json.loads(l)
                curUnixTs = timestamp_in_unix(data["timestamp"])
                if curUnixTs >= bottomTs and curUnixTs < topTs:
                    dataToWork.append(data)

                if curUnixTs >= topTs:
                    workerThread = ParrallelWorker(str(threadId), dataToWork, pgnsToWork, dictionary, filetw[0].replace(dataDir+"/", ""), bottomTs)
                    workerThread.start()
                    workerThread.join()
                    dataToWork = []
                    threadId += 1
                    bottomTs = topTs
                    topTs += samplingTime
                # read first line and check timestamp

        except Exception as ex:
            print(ex)
    try:
        file_worked((filetw[0],))
    except Exception as ex:
        print(ex)
