class DataBase:
    def __init__(self, teamSet):
        self.dataBaseMap = {}
        for team in teamSet:
            playersArray = team.getPlayers()
            self.dataBaseMap[team] = playersArray
    def getData(self):
        return self.dataBaseMap
            