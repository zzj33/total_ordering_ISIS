
#f1=open('1.txt',"r")
#f2=open('2.txt',"r")
#f3=open('3.txt',"r")
f4=open('4.txt',"r")
f5=open('5.txt',"r")
f6=open('6.txt',"r")
f7=open('7.txt',"r")
f8=open('8.txt',"r")

while True:
    #l1=f1.readline()
    #l2=f2.readline()
    #l3=f3.readline()
    l1=f4.readline()
    l5=f5.readline()
    l6=f6.readline()
    l7=f7.readline()
    l8=f8.readline()




    n=min(len(l1),len(l5),len(l6),len(l7),len(l8))
    if l1:
        if l1[:n]==l6[:n] and l1[:n]==l7[:n]:
            if l1[:n]==l5[:n] and l1[:n]==l8[:n]:
                print(True)
    else:
        break

