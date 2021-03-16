import pandas as pd
from openpyxl.reader.excel import load_workbook
from builtins import int
import datetime
import pymysql
def importExcelToMysql(cur, path,test=0):      
    sheet = pd.read_excel(path,keep_default_na=False,sheet_name=[0])
    for sqlstr in sheet[0].values:
        #status = 0 break
        status = str(sqlstr[8])
        if status == '0':
                continue        
        teachernumber=str(sqlstr[2]).strip()
        # 校验有没有这个编号的 音频AA
#         print(teachernumber)
        cur.execute("select id,number from product where number = (%s)",teachernumber)
        #拿ok_teacher_id
        data = cur.fetchone(); #(1, '男声1号')

        check_teachernumber = ''
        if data:
            check_teachernumber = str(data[1]).strip()
        if  check_teachernumber == teachernumber:
            print('有重复的音频')
            continue
        
        else:
            time='{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            #拿teacher_id
            #ok
            if test == 1:
                ok_teacher_id = str(sqlstr[10])
                cur.execute("select * from temp_table where ok_teacher_id = (%s)",ok_teacher_id)
                temp_data = cur.fetchone();
                if temp_data is  None:
                    print(ok_teacher_id+'ccccccccccccccccccccccccccc')
            if test == 2:
                #go
                go_teacher_id = str(sqlstr[10])
                cur.execute("select * from temp_table where go_teacher_id = (%s)",go_teacher_id)
                temp_data = cur.fetchone();
                if temp_data is  None:
                    print(go_teacher_id+'ccccccccccccccccccccccccccc')
            

            teacher_id = str(temp_data[1])
#             print(ok_teacher_id+'------'+teacher_id)
#                     id    sort    number  type    lang    sex age mood    status  audio_url   teacher_id   
            valuestr = [str(sqlstr[2]).strip(),str(sqlstr[1]), str(sqlstr[8]), str(sqlstr[9]),teacher_id,time]
            cur.execute("insert into product(number,sort,status,audio_url,teacher_id,created_at) values(%s,%s,%s,%s,%s,%s)", valuestr)
            product_id = getLastId(cur)
#             print([product_id])
            #五个属性↓ 4 5 6 7 8
            t = str(sqlstr[3]).strip()
            cur.execute("select id from product_type where name = (%s) and category_id = 2",t)
            t_data  = cur.fetchone()
            t_productstr = [product_id,t_data[0]]
            t_res = cur.execute("insert into product_type_intermedial(product_id,product_type_id) values(%s,%s)",t_productstr)
            
            l = str(sqlstr[4]).strip()
            cur.execute("select id from product_type where name = (%s) and category_id = 1",l)
            l_data  = cur.fetchone()
            l_productstr = [product_id,l_data[0]]
            l_res = cur.execute("insert into product_type_intermedial(product_id,product_type_id) values(%s,%s)",l_productstr)

            s = str(sqlstr[5]).strip()
            cur.execute("select id from product_type where name = (%s) and category_id = 3",s)
            s_data  = cur.fetchone()
            s_productstr = [product_id,s_data[0]]
            s_res = cur.execute("insert into product_type_intermedial(product_id,product_type_id) values(%s,%s)",s_productstr)
            
            a = str(sqlstr[6]).strip()
            cur.execute("select id from product_type where name = (%s) and category_id = 4",a)
            a_data  = cur.fetchone()
            a_productstr = [product_id,l_data[0]]
            a_res = cur.execute("insert into product_type_intermedial(product_id,product_type_id) values(%s,%s)",a_productstr)
            
            m = str(sqlstr[7]).strip()
            cur.execute("select id from product_type where name = (%s) and category_id = 5",m)
            m_data  = cur.fetchone()
            m_productstr = [product_id,m_data[0]]
            m_res = cur.execute("insert into product_type_intermedial(product_id,product_type_id) values(%s,%s)",m_productstr)
            
            print('insert success'+'----'+str(product_id)+'---属性：'+str(t_res)
                 +str(l_res)+str(s_res)+str(a_res)+str(m_res))
def getLastId(cur):
    cur.execute("select last_insert_id();")
    data = cur.fetchone();
    return data[0]


if __name__ == '__main__':
    starttime = datetime.datetime.now()
#     conn = pymysql.connect(host = '14.18.80.193', user ='root', password ='KsmwE6tK3s5c5nmC', database ='test', charset='utf8')
    conn = pymysql.connect(host = 'localhost', user ='root', password ='123456', database ='test', charset='utf8')
    cur = conn.cursor()
#     input_time=input("输入：输入1 或 2")
#     importExcelToMysql(cur, "./ok_audio_list.xlsx",1)
    importExcelToMysql(cur, "./go_audio_list.xlsx",2)
    cur.close()
    conn.commit()
    # 关闭数据库服务器连接，释放内存
    conn.close()
    endtime = datetime.datetime.now()
    x = endtime - starttime

    print(x)