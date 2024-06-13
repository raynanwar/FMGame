import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from Player import Player
from Team import Team
from Database import DataBase

class DataScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()

    def getTeamLinks(self):
        url = 'https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/gb1'
        response = self.session.get(url, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        teamLinks = ['https://www.transfermarkt.co.uk' + td.find('a')['href']
                     for td in soup.find_all('td', class_='hauptlink no-border-links')]
        return teamLinks

    def getTeamPlayerLinks(self, teamLink):
        response = self.session.get(teamLink, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        tableTag = soup.find('table', class_='items')
        if not tableTag:
            return []
        playersLinks = ['https://www.transfermarkt.co.uk' + td.find('a')['href']
                        for td in tableTag.find_all('td', class_='hauptlink')
                        if '€' not in td.get_text()]
        return playersLinks

    def fetchPlayerDetails(self, playerLink):
        response = self.session.get(playerLink, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        name = self.fetchPlayerName(soup)
        age = self.fetchPlayerAge(soup)
        height = self.fetchPlayerHeight(soup)
        nationality = self.fetchPlayerNationality(soup)
        teamName = self.fetchPlayerTeam(soup)
        marketValue = self.fetchPlayerMarketValue(soup)
        position = self.fetchPlayerPosition(soup)
        return Player(name, age, height, teamName, marketValue, position, nationality)

    def fetchPlayerName(self, soup):
        meta_tag = soup.find('meta', attrs={'name': 'keywords'})
        if meta_tag:
            return meta_tag['content'].split(',')[0].strip()
        tempTag = soup.find('span', class_='info-table__content--regular', string='Full name:')
        if tempTag:
            nameTag = tempTag.find_next_sibling('span', class_='info-table__content--bold')
            return nameTag.get_text(strip=True) if nameTag else None
        tempTag = soup.find('span', class_='info-table__content--regular', string='Name in home country:')
        if tempTag:
            nameTag = tempTag.find_next_sibling('span', class_='info-table__content--bold')
            return nameTag.get_text(strip=True) if nameTag else None
        return None

    def fetchPlayerAge(self, soup):
        ageTag = soup.find('span', itemprop='birthDate', class_='data-header__content')
        return ageTag.get_text(strip=True).split('(')[-1].replace(')', '').strip() if ageTag else None

    def fetchPlayerHeight(self, soup):
        heightTag = soup.find('span', itemprop='height', class_='data-header__content')
        return heightTag.get_text(strip=True) if heightTag else None

    def fetchPlayerNationality(self, soup):
        nationalityTag = soup.find('span', itemprop='nationality', class_='data-header__content')
        return nationalityTag.get_text(strip=True) if nationalityTag else None

    def fetchPlayerTeam(self, soup):
        teamTag = soup.find('span', class_='data-header__club', itemprop='affiliation')
        return teamTag.find('a').get_text(strip=True) if teamTag else None

    def fetchPlayerMarketValue(self, soup):
        marketValueTag = soup.find('a', class_='data-header__market-value-wrapper')
        if marketValueTag:
            euroSymbol = marketValueTag.find('span', class_='waehrung').get_text(strip=True)
            marketValue = marketValueTag.find(string=True, recursive=False).strip()
            marketValueUnit = marketValueTag.find_all('span', class_='waehrung')[1].get_text(strip=True)
            return f"{euroSymbol}{marketValue}{marketValueUnit}"
        return None

    def fetchPlayerPosition(self, soup):
        positionTag = soup.find('dd', class_='detail-position__position')
        return positionTag.get_text(strip=True) if positionTag else None

    def createClubs(self, stringSet):
        clubs = set()
        for team_name in stringSet:
            team = Team(team_name)
            clubs.add(team)
        return clubs

    def getTeamNames(self):
        teamSet = set()
        teamLinks = self.getTeamLinks()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_team = {executor.submit(self.processTeamForNames, teamLink): teamLink for teamLink in teamLinks}
            for future in as_completed(future_to_team):
                try:
                    team_names = future.result()
                    teamSet.update(team_names)
                except Exception as e:
                    print(f"Error fetching team names: {str(e)}")

        return teamSet

    def processTeamForNames(self, teamLink):
        playersArray = self.getTeamPlayerLinks(teamLink)
        team_names = set()
        for player in playersArray:
            response = self.session.get(player, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            team = self.fetchPlayerTeam(soup)
            if team and "U21" not in team:
                team_names.add(team)
        return team_names

    def createPlayer(self):
        teamLinks = self.getTeamLinks()
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_team = {executor.submit(self.processTeam, teamLink): teamLink for teamLink in teamLinks}
            for future in as_completed(future_to_team):
                try:
                    future.result()  # Wait for the result to ensure completion
                except Exception as e:
                    print(f"Error processing team: {str(e)}")

    def processTeam(self, teamLink):
        playersLinkArray = self.getTeamPlayerLinks(teamLink)
        teamSet = self.getTeamNames()
        clubSet = self.createClubs(teamSet)

        for playerLink in playersLinkArray:
            response = self.session.get(playerLink, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')

            name = self.fetchPlayerName(soup)
            age = self.fetchPlayerAge(soup)
            height = self.fetchPlayerHeight(soup)
            nationality = self.fetchPlayerNationality(soup)
            teamName = self.fetchPlayerTeam(soup)
            marketValue = self.fetchPlayerMarketValue(soup)
            position = self.fetchPlayerPosition(soup)

            player = Player(name, age, height, teamName, marketValue, position, nationality)
            for club in clubSet:
                if teamName == club.getTeamName():
                    club.addPlayer(player)   
    def run(self):
        self.createPlayer()
        teamSet = self.getTeamNames()
        clubs = self.createClubs(teamSet)
        database = DataBase(clubs)
        dataMap = database.getData()

        for team, players in dataMap.items():
            print(f'Team: {team}')
            for player in players:
                print(player)
            print('-' * 50)