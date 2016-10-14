import scrapy

from neo4j.v1 import GraphDatabase, basic_auth
from time import sleep

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "sitscher"))

class SoccerSpider(scrapy.Spider):
    name = "soccerspider"
    start_urls = ["https://en.wikipedia.org/wiki/Germany_national_football_team"]
    def parse(self, response):
        for link in response.css('a'):
            text = link.css('::text').extract()
            if "FIFA World Cup squad" in next(iter(text), ""):
                url = link.css('::attr(href)').extract_first()
                print url
                yield scrapy.Request(response.urljoin(url),
                                     callback=self.parse_tournament)

    def parse_tournament(self, response):
        session = driver.session()
        for table in response.xpath("//span[contains(@id, 'Germany')]/../following-sibling::table[1]"):
            t = session.begin_transaction()
            for row in table.css('tr'):
                elements = [data.extract()
                            for data in
                            row.css('th>a::text,td>a::text')]
                if len(elements):
                    print elements[1], elements[-1]
                    t.run((u"MERGE (a:Player {{name: '{0}'}})" + 
                        u"-[r:PLAYED]->(b:Club {{name: '{1}'}})")\
                        .format(elements[1], elements[-1]))
            t.commit()
