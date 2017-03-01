import pickle
idToCountries = {}
testa = []
testa.append("us")
testa.append("en")
idToCountries["12345"] = testa
testb=[]
testb.append("cn")
testb.append("th")
idToCountries["12346"] = testb
outf=open("test.pkl", "wb")
pickle.dump(idToCountries, outf)