class Team:
    def __init__(self, clubName):
        self.clubName = clubName
        self.playersArray  = []
        
    def addPlayer(self, player):
        self.playersArray.append(player)
        
    def getPlayers(self):
        return self.playersArray
    
    def getTeamName(self):
        return self.clubName
    
