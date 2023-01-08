import json

class cls_readSchema():
    def __init__(self):
        file = 'config/ds10.json'   # je nach Aufruf mit oder ohne ../
        with open(file) as configFile:
            self.dict_data = json.load(configFile)


    def returnSatzarten(self):
        listSatzarten = []
        for sa in self.dict_data.keys():
            listSatzarten.append(sa)
        return listSatzarten

    def returnSatzartenFelder(self, sa):
        listSatzartenFelder = []
        for saKey in self.dict_data[sa]['felder'].keys():
            saValue = self.dict_data[sa]['felder'][saKey]
            if saKey[-5:] == "start":
                saKey = saKey[:-6]
                listSatzartenFelder.append(saKey)
        return listSatzartenFelder

    def returnSatzartenFelderKomplett(self, sa):
        dictSatzartenFelder = {}
        try:
            for saKey, saValue in self.dict_data[sa]['felder'].items():
                if saKey[-5:] == "start":
                    laengeTemp = saValue
                dictSatzartenFelder[saKey] = saValue
                if saKey[-3:] == "end":
                    dictSatzartenFelder[saKey[:-3] + "laenge"] = int(saValue)-int(laengeTemp)
        except:
            dictSatzartenFelder = {}
        return dictSatzartenFelder

    def laengeSa(self, sa):
        laengeSa = list(self.dict_data[sa]['felder'].values())[-1]
        return laengeSa


    def returnFeld(self, sa, position):
        dictFeld = {}
        for saKey, saValue in self.dict_data[sa]['felder'].items():
            if saKey[-5:] == "start" and str(saValue) == str(position):
                dictFeld[saKey[:-6]] = int(self.dict_data[sa]['felder'][saKey[:-6] + "_end"]) - int(self.dict_data[sa]['felder'][saKey])
        return dictFeld




if __name__ == "__main__":
    x = cls_readSchema()