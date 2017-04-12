import multiprocessing.dummy
import re
from collections import OrderedDict

num_of_column = 10
num_of_column = num_of_column - 2
p = multiprocessing.dummy.Pool(6)
filename = 'data_final'
f = open(filename+".txt",'r')
s = f.read()
#x = re.sub('[^A-Za-z0-9?!$\n ]','',s)
x = re.sub('[^A-Za-z?!$\n ]','',s)
x = re.sub('\n+'
           ,'\n',x)
x = re.sub(' +',' ',x)
x = re.sub('\n +','\n',x)
x = re.sub('\n+','\n',x).lower()

x=x.split('\n')
temp_line = []
temp_class = []
no_of_comment =0;
for temp in x:
    temp2 = temp.split('$')
    temp_line.append(temp2[0])
    no_of_comment = no_of_comment + 1
    print(temp)
    temp_class.append(temp2[1])

each_line = temp_line;
x = each_line

def words (line):
    return line.split(" ")

x = p.map(words,x)

def mapper(s): # string -> [(key value)]
	pairs = []
	for c in s:
		pairs.append((c,1))
	return pairs

def combiner(pairs):
	index = {}
	for (key,value) in pairs:
		if not index.has_key(key):
			index[key] = value
		else:
			index[key] = index[key] + value
	pairs = []
	for key in index:
		pairs.append((key,index[key]))
	return pairs

def reducer(data):
	index = {}
	for pairs in data:
		for (key,value) in pairs:
			if not index.has_key(key):
				index[key] = value
			else:
				index[key] = index[key] + value
	pairs = []
	for key in index:
		pairs.append((key,index[key]))
	return pairs

data = p.map(mapper , x)
data = p.map(combiner,data)
data = reducer(data)
#print(data)

dict = {}

for (key,value) in data:
    if(key != '' and key != 'class'):
        dict[key]= value

print(dict)
dict_temp = sorted(dict.iteritems(), key=lambda  x:x[1],reverse=True)
print(dict_temp)

dict_sort = OrderedDict((key, value) for (key,value) in dict_temp)

print('__________________________\n')
print(dict_sort)

#num_of_column = len(dict_sort)#taking all the words as attributes
print(num_of_column)
dict_modified = {}
list_modified = []
cnt = 0;
for (key,value) in dict_temp:
    list_modified.append((key,value))
    cnt = cnt + 1
    if cnt >num_of_column:
        break;


ff = open(filename+".arff",'w')
ff.write('@relation mooc ')
ff.write('\n\n')

attr = 0;
for m in dict_sort.keys():
    ff.write('@attribute ')
    ff.write('\'')
    ff.write(m)
    ff.write('\'')
    ff.write(' real ')
    ff.write('\n')
    attr = attr +1
    if(attr>num_of_column):
        break;


ff.write('@attribute ')
ff.write('class ')
ff.write('{yes,no}')
ff.write('\n\n')
ff.write('@data')
ff.write('\n')

foo ="0"
var1 = ""
var2 = ""
var3 = ""
line_count = 0

dict_modified = OrderedDict((key, value) for (key,value) in list_modified)
print(dict_modified)#with less attributes

for var1 in each_line:
    a = [foo for i in range(len(dict_modified))]
    var2 = var1.split(" ")
    for var3 in var2:
        if dict_modified.has_key(var3):
            i = dict_modified.keys().index(var3)
            if var3 == "help":
                a[i] = "500"
            elif var3 == "need":
                a[i] = "500"
            elif var3 == "problem":
                a[i] = "500"
            elif var3 == "please":
                a[i] = "500"
            else:
                a[i] = "1"
    #print(a)
    for k in a:
        ff.write(k)
        ff.write(',')
    ff.write(temp_class[line_count])
    line_count = line_count + 1
    ff.write('\n')
ff.close()
print(no_of_comment)