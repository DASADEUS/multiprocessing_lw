# -*- coding: utf-8 -*-
"""
# Параллельные вычисления

## Лабораторная работа

1. Разбейте файл `recipes_full.csv` на несколько (например, 8) примерно одинаковых по объему файлов c названиями `id_tag_nsteps_*.csv`. Каждый файл содержит 3 столбца: `id`, `tag` и `n_steps`, разделенных символом `;`. Для разбора строк используйте `csv.reader`.

__Важно__: вы не можете загружать в память весь файл сразу. Посмотреть на первые несколько строк файла вы можете, написав код, который считывает эти строки.

Подсказка: примерное кол-во строк в файле - 2.3 млн.

```
id;tag;n_steps
137739;60-minutes-or-less;11
137739;time-to-make;11
137739;course;11
```
"""

import numpy as np
import pandas as pd
from google.colab import files
import csv
import re
import multiprocessing
import time
import collections

from google.colab import drive
drive.mount('/content/drive')

dataarray=[]
with open('/content/drive/MyDrive/recipes_full.csv', encoding='utf-8') as file:  
    csv_reader = csv.reader(file)
    for row in csv_reader:
      if len(row)==6:
        dataarray.append([row[1],row[5],'0'])
      else:
        dataarray.append([row[1],row[5],row[6]])

dataarray.pop(0)
for i in dataarray:
    i[1]=i[1][1:-1]

for i in dataarray:
    i[1]=re.sub(r"[''/""]+", "", str(i[1])).split(', ')
dataarray

dataarray1=dataarray[:446327]
dataarray2=dataarray[446327:892654]
dataarray3=dataarray[892654:1338981]
dataarray4=dataarray[1338981:1785308]
dataarray5=dataarray[1785308:2231637]

# def splittinп(data):
#     outarray=[]
#     for i in data:
#         for z in i[1]:
#             outarray.append([i[0],z,i[2]])
#     return outarray
# def save(name,data):
#     with open(name,"w") as f:
#         writer =csv.writer(f)
#         writer.writerows(data)
#     files.download(name)
#     return
# pool=multiprocessing.Pool(processes=2)
# results=pool.map(splittinп,[dataarray5])
# save('id_tag_nsteps_555.csv',results)

def savesplittinп(data,name):
     outarray=[]
     for i in data:
         for z in i[1]:
             outarray.append([i[0],z,i[2]])
     with open(name,"w") as f:
         writer =csv.writer(f)
         writer.writerows(outarray)
     files.download(name)
     return outarray

p1=multiprocessing.Process(target=savesplittinп, args=(dataarray1,'id_tag_nsteps_1.csv'))
p1.start()
p2=multiprocessing.Process(target=savesplittinп, args=(dataarray2,'id_tag_nsteps_2.csv'))
p2.start()
p3=multiprocessing.Process(target=savesplittinп, args=(dataarray3,'id_tag_nsteps_3.csv'))
p3.start()
p4=multiprocessing.Process(target=savesplittinп, args=(dataarray4,'id_tag_nsteps_4.csv'))
p4.start()
p5=multiprocessing.Process(target=savesplittinп, args=(dataarray5,'id_tag_nsteps_5.csv'))
p5.start()
p1.join()
p2.join()
p3.join()
p4.join()
p5.join()

"""2. Напишите функцию, которая принимает на вход название файла, созданного в результате решения задачи 1, считает среднее значение количества шагов для каждого тэга и возвращает результат в виде словаря."""

def average20(name, queue):
  d1=[]
  with open(name, encoding='utf-8') as file:  
    csv_reader = csv.reader(file)
    for row in csv_reader:
      d1.append(row)
  d1pd=pd.DataFrame(d1)
  d1pd.set_axis(['id','tags','nsteps'], axis=1, inplace=True)
  d1pd['nsteps']=d1pd['nsteps'].astype('int')
  dic=d1pd.groupby(['tags']).mean().to_dict()
  queue.put(dic)

queue = multiprocessing.SimpleQueue()
multiprocessing.Process(target=average20, args=('id_tag_nsteps_1.csv',queue,)).start()
exit1=queue.get()
multiprocessing.Process(target=average20, args=('id_tag_nsteps_2.csv',queue,)).start()
exit2=queue.get()
multiprocessing.Process(target=average20, args=('id_tag_nsteps_3.csv',queue,)).start()
exit3=queue.get()
multiprocessing.Process(target=average20, args=('id_tag_nsteps_4.csv',queue,)).start()
exit4=queue.get()
multiprocessing.Process(target=average20, args=('id_tag_nsteps_5.csv',queue,)).start()
exit5=queue.get()
exit5

"""3. Напишите функцию, которая считает среднее значение количества шагов для каждого тэга по всем файлам, полученным в задаче 1, и возвращает результат в виде словаря. Не используйте параллельных вычислений. При реализации выделите функцию, которая объединяет результаты обработки отдельных файлов. Модифицируйте код из задачи 2 таким образом, чтобы иметь возможность получить результат, имея результаты обработки отдельных файлов. Определите, за какое время задача решается для всех файлов.

"""

def average3(name):
  d1=[]
  with open(name, encoding='utf-8') as file:  
    csv_reader = csv.reader(file)
    for row in csv_reader:
      d1.append(row)
  d1pd=pd.DataFrame(d1)
  d1pd.set_axis(['id','tags','nsteps'], axis=1, inplace=True)
  d1pd['nsteps']=d1pd['nsteps'].astype('int')
  dic=d1pd.groupby(['tags']).mean().to_dict()
  return dic
def mtopd(arr):
  x=pd.DataFrame(arr)
  x.set_axis(['nsteps'], axis=1, inplace=True)
  x['index']=x.index
  x = x[['index', 'nsteps']]
  x.reset_index(drop=True, inplace=True)
  return x
def Union(names):
  dicts=[average3(x) for x in names]
  first=mtopd(dicts[0])
  for i in dicts[1:]:
    first=first.merge(mtopd(i), left_on='index', right_on='index', how='outer')
  first.set_axis(['index']+listnames, axis=1, inplace=True)
  return first

start_time = time.time()
listnames=['id_tag_nsteps_1.csv','id_tag_nsteps_2.csv','id_tag_nsteps_3.csv','id_tag_nsteps_4.csv','id_tag_nsteps_5.csv']
result=Union(listnames)
print("--- %s seconds ---" % (time.time() - start_time))
print(result)

"""4. Решите задачу 3, распараллелив вычисления с помощью модуля `multiprocessing`. Для обработки каждого файла создайте свой собственный процесс. Определите, за какое время задача решается для всех файлов."""

def mtopd4(arr,queue):
  x=pd.DataFrame(arr)
  x.set_axis(['nsteps'], axis=1, inplace=True)
  x['index']=x.index
  x = x[['index', 'nsteps']]
  x.reset_index(drop=True, inplace=True)
  queue.put(x)

start_time = time.time()
listnames=['id_tag_nsteps_1.csv','id_tag_nsteps_2.csv','id_tag_nsteps_3.csv','id_tag_nsteps_4.csv','id_tag_nsteps_5.csv']
dicts=[average3(x) for x in listnames]

queue = multiprocessing.SimpleQueue()
multiprocessing.Process(target=mtopd4, args=(dicts[0],queue,)).start()
multiprocessing.Process(target=mtopd4, args=(dicts[1],queue,)).start()
multiprocessing.Process(target=mtopd4, args=(dicts[2],queue,)).start()
multiprocessing.Process(target=mtopd4, args=(dicts[3],queue,)).start()
multiprocessing.Process(target=mtopd4, args=(dicts[4],queue,)).start()

resultex4=queue.get()
for i in range(4):
  resultex4=resultex4.merge(queue.get(), left_on='index', right_on='index', how='outer')
resultex4.set_axis(['index']+listnames, axis=1, inplace=True)
print("--- %s seconds ---" % (time.time() - start_time))
print(resultex4)

"""5. (*) Решите задачу 3, распараллелив вычисления с помощью модуля `multiprocessing`. Создайте фиксированное количество процессов (равное половине количества ядер на компьютере). При помощи очереди передайте названия файлов для обработки процессам и при помощи другой очереди заберите от них ответы. """

def mtopd5(arr,queue):
  x=pd.DataFrame(arr)
  x.set_axis(['nsteps'], axis=1, inplace=True)
  x['index']=x.index
  x = x[['index', 'nsteps']]
  x.reset_index(drop=True, inplace=True)
  queue2.put(x)

# listnames=['id_tag_nsteps_1.csv','id_tag_nsteps_2.csv','id_tag_nsteps_3.csv','id_tag_nsteps_4.csv','id_tag_nsteps_5.csv']
# dicts=[average3(x) for x in listnames]
# queue1 = multiprocessing.SimpleQueue()
# for i in dicts:
#   queue1.put(i)
#   print(queue1.get())

start_time = time.time()
listnames=['id_tag_nsteps_1.csv','id_tag_nsteps_2.csv','id_tag_nsteps_3.csv','id_tag_nsteps_4.csv','id_tag_nsteps_5.csv']
dicts=[average3(x) for x in listnames]

queue1 = multiprocessing.SimpleQueue()
queue2 = multiprocessing.SimpleQueue()
for i in dicts:
  queue1.put(i)
  multiprocessing.Process(target=mtopd5, args=(queue1.get(),queue2,)).start()

resultex5=queue2.get()
for i in range(4):
  resultex5=resultex5.merge(queue2.get(), left_on='index', right_on='index', how='outer')
resultex5.set_axis(['index']+listnames, axis=1, inplace=True)
print("--- %s seconds ---" % (time.time() - start_time))
print(resultex5)