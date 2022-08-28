from mysql.connector import connect, Error
from getpass import getpass
import string
from datetime import datetime
#10529 | DESKTOP-8M98O0K 




def fd():
    try:
        with connect(
            host = "localhost",
            user = input("Log :"),
            password = getpass('pass :'),
            database = "zabbix",
            ) as connection:
            Temp_RX570 = """
                    SELECT *
                    FROM
                        (SELECT a2.Data_time
                            , sum(a2.CPU_RX570_temp) CPU_RX570_temp
                            , sum(a2.Speed_Fan1) Speed_Fan1
                            , sum(a2.Speed_Fan2) Speed_Fan2
                        FROM
                            (SELECT from_unixtime(hu.clock,'%Y-%c-%d, %H-%i ') Data_time
                                , CASE WHEN a1.name = 'CPU_RX Temperature' THEN hu.value ELSE 0 END CPU_RX570_temp
                                , CASE WHEN a1.name = 'Speed_Fan1' THEN hu.value ELSE 0 END Speed_Fan1
                                , CASE WHEN a1.name = 'Speed_Fan2' THEN hu.value ELSE 0 END Speed_Fan2  
                            FROM history_uint hu 
                            JOIN 
                                (SELECT i.name
                                    , i.itemid
                                FROM items i
                                JOIN 
                                    (SELECT hostid
                                    FROM hosts 
                                    WHERE 1=1
                                        AND host = 'DESKTOP-8M98O0K') a ON a.hostid = i.hostid 
                                    AND name IN('Speed_Fan2', 'Speed_Fan1', 'CPU_RX Temperature')
                                    ) a1 ON hu.itemid = a1.itemid) a2
                        WHERE 1=1
                        GROUP BY a2.Data_time) a3
            """
                #print(connection)
            #Temp_RX570_execute = "SELECT from_unixtime(clock) Data_time, value FROM history_uint  WHERE 1=1 AND itemid = 43856;"
            with connection.cursor() as cursor:
                cursor.execute(Temp_RX570)

                #result = cursor.fetchal()
                with open('full_data.txt', 'w') as f_d:
                    for i in cursor:
                        f_d.write(f'{i[0]} Video card RX570: temp - {i[1]}; speed fan 1 - {i[1]}; speed fan 2 - {i[1]}' + '\n')
       

    except Error as e:
        print(e)
    
    return 't'




print(fd())



def getting_the_data(sign = 0, data = '2022-8-27'):
    try:
        with connect(
            host = "localhost",
            user = input("Log :"),
            password = getpass('pass :'),
            database = "zabbix",
            ) as connection:
            if sign:
                Temp_RX570 = f"""
                SELECT a3.Data_time
                    , a3.CPU_RX570_temp
                    , a3.Speed_Fan1
                    , a3.Speed_Fan2
                FROM
                    (SELECT a2.Data_time
                        , sum(a2.CPU_RX570_temp) CPU_RX570_temp
                        , sum(a2.Speed_Fan1) Speed_Fan1
                        , sum(a2.Speed_Fan2) Speed_Fan2
                    FROM
                        (SELECT from_unixtime(hu.clock,'%Y-%c-%d, %H-%i ') Data_time
                            , CASE WHEN a1.name = 'CPU_RX Temperature' THEN hu.value ELSE 0 END CPU_RX570_temp
                            , CASE WHEN a1.name = 'Speed_Fan1' THEN hu.value ELSE 0 END Speed_Fan1
                            , CASE WHEN a1.name = 'Speed_Fan2' THEN hu.value ELSE 0 END Speed_Fan2  
                        FROM history_uint hu 
                        JOIN 
                            (SELECT i.name
                                , i.itemid
                            FROM items i
                            JOIN 
                                (SELECT hostid
                                FROM hosts 
                                WHERE 1=1
                                    AND host = 'DESKTOP-8M98O0K') a ON a.hostid = i.hostid 
                                AND name IN('Speed_Fan2', 'Speed_Fan1', 'CPU_RX Temperature')
                                ) a1 ON hu.itemid = a1.itemid) a2
                    WHERE 1=1
                        AND a2.Data_time like '%{data}%'
                    GROUP BY a2.Data_time) a3
                WHERE 1=1
                    AND a3.CPU_RX570_temp > 77 
                    AND a3.Speed_Fan1 < 333
                    AND a3.Speed_Fan2 < 333
                    
                """
            else:
                Temp_RX570 = f"""
                SELECT a3.Data_time
                    , a3.CPU_RX570_temp
                    , a3.Speed_Fan1
                    , a3.Speed_Fan2
                FROM
                    (SELECT a2.Data_time
                        , sum(a2.CPU_RX570_temp) CPU_RX570_temp
                        , sum(a2.Speed_Fan1) Speed_Fan1
                        , sum(a2.Speed_Fan2) Speed_Fan2
                    FROM
                        (SELECT from_unixtime(hu.clock,'%Y-%c-%d, %H-%i ') Data_time
                            , CASE WHEN a1.name = 'CPU_RX Temperature' THEN hu.value ELSE 0 END CPU_RX570_temp
                            , CASE WHEN a1.name = 'Speed_Fan1' THEN hu.value ELSE 0 END Speed_Fan1
                            , CASE WHEN a1.name = 'Speed_Fan2' THEN hu.value ELSE 0 END Speed_Fan2  
                        FROM history_uint hu 
                        JOIN 
                            (SELECT i.name
                                , i.itemid
                            FROM items i
                            JOIN 
                                (SELECT hostid
                                FROM hosts 
                                WHERE 1=1
                                    AND host = 'DESKTOP-8M98O0K') a ON a.hostid = i.hostid 
                                AND name IN('Speed_Fan2', 'Speed_Fan1', 'CPU_RX Temperature')
                                ) a1 ON hu.itemid = a1.itemid) a2
                    WHERE 1=1
                    GROUP BY a2.Data_time) a3
                WHERE 1=1
                    AND a3.CPU_RX570_temp > 77 
                    AND a3.Speed_Fan1 < 333
                    AND a3.Speed_Fan2 < 333
                """
                #print(connection)
            #Temp_RX570_execute = "SELECT from_unixtime(clock) Data_time, value FROM history_uint  WHERE 1=1 AND itemid = 43856;"
            with connection.cursor() as cursor:
                cursor.execute(Temp_RX570)
                #print(type(cursor))
                #result = cursor.fetchal()
                rows = transform_start_data(1, cursor)    

    except Error as e:
        print(e)

    return rows


def transform_start_data(sign, rows):
    if sign:
        dic_data = dict()
        dic_data = {i[0]: [i[1],i[2],i[3]] for i in rows}
        
        if len(dic_data) > 0:
            with open('triggers.txt', 'w') as f_o:
                for k, v in dic_data.items():
                    f_o.write(f'{k} Video card RX570: temp - {v[0]}; speed fan 1 - {v[1]}; speed fan 2 - {v[2]}' + '\n')
        else:
            with open('triggers.txt', 'w') as f_o:
                f_o.write('No triggers detected')
    #110 critical_temp 
    #70 +- norm
    return 'Well play by'


def start():
    print("Hi, let's look at the data on the video card")
    sign = input('day or all the time? Y/N ' )
    
    if sign == 'Y':
        data = input('format: Y-W-D - XXXX-X-X ' )
        rows = getting_the_data(1, data)  
    else:
        rows = getting_the_data()  

    return rows





print(start())