def inspect():
    standard = open('full_list.txt')
    output = open('output.txt')
    slist = []
    outlist = []
    while 1:
        line = standard.readline()
        if not line:
            break
        slist.append(line[:8])
    while 1:
        line = output.readline()
        if not line:
            break
        outlist.append(line[:8])
    for each in slist:
        if each not in outlist:
            print each

inspect()