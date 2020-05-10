import schlib as sch
import schlib_AWS as schaws

def Refresh_AWSFiles(lottoType):
        try:
                s3 = schaws.s3_resource
                # lottoType = ("Lotto649", "LottoMax", "DailyGrand", "Lottario", "On49", "OnKeno")

                keyName = lottoType + "Cur.csv"
                obj = s3.Object("lotto-number", keyName)
                body = obj.get()['Body'].read().decode('utf-8')
                curResultList = str(body).split("\r\n")
                # print(curResultList)
                # print(len(curResultList))

                # curResultList.pop(len(curResultList))  # Remove the last one as it's empty string
                for row in curResultList:
                        if row == "":
                                curResultList.remove(row)

                # print(len(curResultList))
                # print(curResultList)

                if len(curResultList)>1:
                        curResultRows=[]

                        LottoNum=0
                        if lottoType in ["Lotto649", "Lottario", "On49"]:
                                LottoNum=6
                        if lottoType == "LottoMax":
                                LottoNum=7
                        if lottoType == "DailyGrand":
                                LottoNum=5
                        if lottoType == "OnKeno":
                                LottoNum=20
                        # print("LottoNum:" + str(LottoNum))
                        # print(curResultList[1:-1])

                        for row in curResultList[1:] :
                                if row != "":
                                        # create new row as a list
                                        newResult = []
                                        Items = row.split(",")
                                        # print(Items)
                                        # print(len(Items))
                                        newResult.append(Items[0])  # PlayDate
                                        # print("PlayDate: " + Items[0])

                                        i = 1
                                        if lottoType == "OnKeno":
                                                newResult.append(Items[i])  # AP
                                                i = i + 1

                                        numBegin = i
                                        while i < numBegin + LottoNum:
                                                newResult.append(int(Items[i]))
                                                # print("Nums: " + Items[i])
                                                i = i + 1

                                        if lottoType in ["Lotto649", "LottoMax", "DailyGrand", "Lottario", "On49"]: # Bonus
                                                newResult.append(int(Items[i]))
                                                print("i=" + str(i) +" Bonus: " + Items[i])
                                                i = i + 1

                                        if lottoType in ["Lotto649", "LottoMax", "Lottario"]: # Jackpot
                                                newResult.append(Items[i])
                                                i = i + 1

                                        if lottoType =="LottoMax":  # Million Draws
                                                newResult.append(int(Items[i]))
                                                i = i + 1
                                        # print(newResult)
                                        curResultRows.insert(0, newResult)

                        # write to database
                        if lottoType in ["Lotto649", "Lottario"]:
                                stmtHeader = "INSERT INTO Result (PlayDate, No1, No2, No3, No4, No5, No6, Bonus, Jackpot) "
                        if lottoType == "On49":
                                stmtHeader = "INSERT INTO Result (PlayDate, No1, No2, No3, No4, No5, No6, Bonus) "
                        if lottoType == "LottoMax":
                                stmtHeader = "INSERT INTO Result (PlayDate, No1, No2, No3, No4, No5, No6, No7, Bonus, Jackpot, MD) "
                        if lottoType == "DailyGrand":
                                stmtHeader = "INSERT INTO Result (PlayDate, No1, No2, No3, No4, No5, Bonus) "
                        if lottoType == "OnKeno":
                                stmtHeader = "INSERT INTO Result (PlayDate, AP, N01, N02, N03, N04, N05, N06, N07, N08, N09, N10, N11, N12, N13, N14, N15, N16, N17, N18, N19, N20) "

                        for row in curResultRows:
                                stmt = stmtHeader
                                stmt = stmt + " Select '" + row[0] + "'" # PlayDate

                                i = 1
                                if lottoType == "OnKeno":
                                        stmt = stmt + ", '" + row[i]+ "' "  # AP
                                        i = i + 1

                                numBegin = i
                                while i < numBegin + LottoNum:  # Main numbers
                                        stmt = stmt + ", " + str(row[i])
                                        i = i + 1

                                if lottoType in ["Lotto649", "LottoMax", "DailyGrand", "Lottario", "On49"]: # Bonus
                                        stmt = stmt + ", " + str(row[i])
                                        i = i + 1

                                if lottoType in ["Lotto649", "LottoMax", "Lottario"]: # Jackpot
                                        stmt = stmt + ", " + str(row[i])
                                        i = i + 1

                                if lottoType == "LottoMax":  # MD
                                        stmt = stmt + ", " + str(row[i])

                                if lottoType != "OnKeno":
                                        stmt = stmt + " Where NOT EXISTS (SELECT PlayDate FROM Result WHERE PlayDate='" + row[0] + "')"
                                else:
                                        stmt = stmt + " Where NOT EXISTS (SELECT PlayDate FROM Result WHERE PlayDate='" + row[0] + "' and AP='" + row[1] + "')"

                                # print(stmt)
                                sch.databaseSqlExec(lottoType, stmt)
                        # Refresh csv
                        sch.resultExport2CSV(lottoType)

                        schaws.s3_resource.Object("lotto-number", keyName).delete()
                        schaws.s3_resource.Object("lotto-number", keyName).upload_file(Filename=keyName)
                        schaws.s3_resource.Object("lotto-number", lottoType + ".csv").delete()
                        schaws.s3_resource.Object("lotto-number", lottoType + ".csv").upload_file(Filename=lottoType + ".csv")
                else:
                        print("No records to process")

        except Exception as inst:
                print(inst)

# Refresh_AWSFiles("Lotto649")
# Refresh_AWSFiles("LottoMax")
# Refresh_AWSFiles("DailyGrand")
# Refresh_AWSFiles("Lottario")
# Refresh_AWSFiles("On49")
# Refresh_AWSFiles("OnKeno")


