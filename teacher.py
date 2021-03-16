
import pandas as pd
from openpyxl.reader.excel import load_workbook
from builtins import int
import datetime
import pymysql
def importExcelToMysql(cur, path,test=0):      
    if test==1:
        sheet = pd.read_excel(path,keep_default_na=False,sheet_name=[0])
    #             sheet['okvoices_teacher'].values
        for sqlstr in sheet[0].values:
            time='{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            #         status	avatar	number	nickname	good_at	location	sort	created_at     
    #             id	头像	善长(描述)	编号	地址	昵称	状态	排序
            valuestr = [str(sqlstr[6]), str(sqlstr[1]), str(sqlstr[3]).strip(), str(sqlstr[5]), str(sqlstr[2]), str(sqlstr[4]), str(sqlstr[7]),time]
            cur.execute("insert into teacher(status,avatar,number,nickname,good_at,location,sort,created_at) values(%s,%s,%s,%s,%s,%s,%s,%s)", valuestr)
            teacher_id = getLastId(cur)
            #temp_table
            ok_teacher_id = str(sqlstr[0])
            teacherstr = [teacher_id,ok_teacher_id]
            test = cur.execute("insert into temp_table(teacher_id,ok_teacher_id) values(%s,%s)",teacherstr)
            print('insert success'+'--------'+str(teacher_id))
    elif test==2:
        sheet = pd.read_excel(path,keep_default_na=False,sheet_name=[1])
    #             sheet['goodvoice_teacher'].values
        for sqlstr in sheet[1].values:
            #temp_table
            teachernumber=str(sqlstr[3]).strip()
            #查询名字是否相同 相同就插入到同一条 teacher_id 的临时表/ 不相同就直接插入 go_teacher_id 与 teacher_id
            cur.execute("select id,number from teacher where number = (%s)",teachernumber)
            #拿ok_teacher_id
            data = cur.fetchone(); #(1, '男声1号')
            if data:
                check_teachernumber = str(data[1]).strip()
            if  check_teachernumber == teachernumber:
                go_teacher_id = int(sqlstr[0])
                teacher_id = int(data[0])
                upteachertemp = [go_teacher_id,teacher_id]
                cur.execute("update temp_table set go_teacher_id=(%s) where teacher_id = (%s)",upteachertemp)
                print('update_success'+check_teachernumber+'----'+teachernumber+'----'+str(go_teacher_id)+'--'+str(teacher_id))
            else:
                time='{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
                #         status	avatar	number	nickname	good_at	location	sort	created_at     
                valuestr = [str(sqlstr[6]), str(sqlstr[1]), str(sqlstr[3]).strip(), str(sqlstr[5]), str(sqlstr[2]), str(sqlstr[4]), str(sqlstr[7]),time]
                cur.execute("insert into teacher(status,avatar,number,nickname,good_at,location,sort,created_at) values(%s,%s,%s,%s,%s,%s,%s,%s)", valuestr)
                teacher_id = getLastId(cur)
                go_teacher_id = int(sqlstr[0])
                teacherstr = [teacher_id,go_teacher_id]
                cur.execute("insert into temp_table(teacher_id,go_teacher_id) values(%s,%s)",teacherstr)
                if data:
                    print('insert_success'+data[1]+'----'+teachernumber)
                else:
                    print('insert_success'+'-----'+str(teacher_id)+'----'+teachernumber)
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
    importExcelToMysql(cur, "./ok+gv-teacher.xlsx",2)#先执行 1 再执行 2
    cur.close()
    conn.commit()
    # 关闭数据库服务器连接，释放内存
    conn.close()
    endtime = datetime.datetime.now()
    x = endtime - starttime

    print(x)