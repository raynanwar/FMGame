class Player:
    
    def __init__(self, playerName, playerAge, playerHeight, playerTeam, marketValue, position, nationality) :
        self.playerName = playerName
        self.playerAge = playerAge 
        self.playerheight = playerHeight
        self.playerTeam  = playerTeam
        self.marketValue = marketValue
        self.position = position
        self.nationality = nationality
        
    def setSpeed(self, speed):
        self.speed = speed
    
    def setDribbling(self, dribbling):
        self.dribbling = dribbling
        
    def setShooting(self, shooting):
        self.shooting = shooting
    
    def setPassing(self, passing):
        self.passing = passing
     
    def setPhysical(self, physical):
        self.physical = physical
    
    def getSpeed(self):
        return self.speed
    
    def getDribbing(self):
        return self.dribbling

    def getShooting(self):
        return self.shooting

    def getPassing(self):
        return self.passing

    def getPhysical(self):
        return self.physical
     
    def __str__(self):
        return f"Name: {self.name}, Age: {self.age}, Height: {self.height}, Team: {self.teamName}, Market Value: {self.marketValue}, Position: {self.position}, Nationality: {self.nationality}"