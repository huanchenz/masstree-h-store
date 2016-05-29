import sys
usage = "parse.py <input file>"

print usage
inFile = sys.argv[1]
outFile = "inputMasstree/inMT_" + inFile[11:]

fin = open(inFile, 'r')
fout = open(outFile, 'w')
lines = fin.readlines()

scanCounter = 0
#isScan = 0
scanValCounter = 0
#isScanVal = 0

for line in lines:
    tag = line[0:3]
    if tag == "CMD":
        cmd = line[4:-1]
        #if (cmd != "nextValue") and isScan:
            #isScan = 0
            #fout.write("\t")
            #fout.write(str(scanCounter))
        #if (cmd != "nextValueAtKey") and isScanVal:
            #isScanVal = 0
            #fout.write("\t")
            #fout.write(str(scanValCounter))
        if cmd == "addEntry":
            fout.write("\nPUT\t")
        elif cmd == "deleteEntry":
            fout.write("\nDEL\t")
        elif cmd == "moveToKey":
            scanValCounter = 0
            #isScanVal = 1
            fout.write("\nGET\t")
        elif cmd == "moveToKeyOrGreater":
            scanCounter = 0
            #isScan = 1
            fout.write("\nSCAN\t")
        elif cmd == "moveToGreaterThanKey":
            scanCounter = 0
            #isScan = 1
            fout.write("\nSCAN\t")
        elif cmd == "nextValueAtKey":
            scanValCounter = scanValCounter + 1
            if scanValCounter > 1:
                fout.write("\nNVAL\t")
        elif cmd == "nextValue":
            scanCounter = scanCounter + 1
            if scanCounter > 1:
                fout.write("\nNEXT\t")
        else:
            fout.write("ERROR: UNHANDLED CMD: ")
            fout.write(cmd)
    elif tag == "KEY":
        keyType = line[4:7]
        if keyType == "tin":
            key = int(line[13:-1])
            key_char = chr(key)
            fout.write(key_char.encode("hex"))
        elif keyType == "sma":
            key = int(line[14:-1])
            mask = 0xff
            key0 = key & mask
            key_char0 = chr(int(key0))
            key1 = (key >> 8) & mask
            key_char1 = chr(int(key1))
            fout.write(key_char1.encode("hex"))
            fout.write(key_char0.encode("hex"))
        elif keyType == "int":
            key = int(line[13:-1])
            mask = 0xff
            key0 = key & mask
            key_char0 = chr(int(key0))
            key1 = (key >> 8) & mask
            key_char1 = chr(int(key1))
            key2 = (key >> 16) & mask
            key_char2 = chr(int(key2))
            key3 = (key >> 24) & mask
            key_char3 = chr(int(key3))
            fout.write(key_char3.encode("hex"))
            fout.write(key_char2.encode("hex"))
            fout.write(key_char1.encode("hex"))
            fout.write(key_char0.encode("hex"))
        elif keyType == "big":
            fout.write("\nERROR: UNKNOWN TYPE bigint\n")
        elif keyType == "dou":
            fout.write("\nERROR: UNKNOWN TYPE double\n")
        elif keyType == "var":
            if line[7] == "c":
                if line[15] == "]":
                    keylen = int(line[14])
                    key = line[17:(17+keylen)]
                    fout.write(key.encode("hex"))
                elif line[16] == "]":
                    keylen = int(line[14:16])
                    key = line[18:(18+keylen)]
                    fout.write(key.encode("hex"))
                else:
                    fout.write("\nERROR: varchar TOO LONG\n")
            elif line[7] == "b":
                fout.write("\nERROR: UNKNOWN TYPE varbinary\n")
        elif keyType == "tim":
            fout.write("\nERROR: UNKNOWN TYPE timestamp\n")
        elif keyType == "dec":
            fout.write("\nERROR: UNKNOWN TYPE decimal\n")
        elif keyType == "INV":
            fout.write("\nNOTE: TYPE INVALID\n")
        elif keyType == "NUL":
            fout.write("\nERROR: UNKNOWN TYPE NULL\n")
        elif keyType == "boo":
            fout.write("\nERROR: UNKNOWN TYPE boolean\n")
        elif keyType == "add":
            fout.write("\nERROR: UNKNOWN TYPE address\n")
    elif tag == "VAL":
        fout.write("\t")
        value = line[8:-1]
        fout.write(value)
    elif tag == "ARG":
        fout.write("\nERROR: UNKNOWN TAG ARG\n")
    elif tag == "Tab":
        fout.write("BEGIN\tBEGIN\tBEGIN")
    else:
        fout.write("\nERROR: UNKNOWN TAG\n")


