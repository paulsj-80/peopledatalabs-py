import asyncio
import aiohttp
from aiohttp import ClientSession
import json

class Endpoint:
    def __init__(self, facade):
        self._facade = facade

    async def _enrichment(self, params, stype):
        return await self._facade.safe_get(
            "/%s/enrich" % stype, params)

    async def _cleaner(self, params, ctype):
        return await self._facade.safe_get("/%s/clean" % ctype, params)
        
class Search(Endpoint):
    def __init__(self, facade, search_for):
        super().__init__(facade)
        self.__search_for = search_for

    # TODO: copy params?
    async def _do_search(self, params, stype):
        searchParams = {
            "titlecase": False,
            "dataset": "all",
            "scroll_token": None,
            "size": 10,
            stype: params["searchQuery"],
            "pretty": False
        }
        searchParams = {**searchParams, **params}
        del searchParams["searchQuery"]
        return await self._facade.safe_post(
            "/%s/search" % self.__search_for, searchParams)
    
    async def elastic(self, params):
        return await self._do_search(params, "elastic")
    async def sql(self, params):
        return await self._do_search(params, "sql")

class Person(Endpoint):
    def __init__(self, facade):
        super().__init__(facade)
        # TODO: "person" too often?
        self.search = Search(facade, "person")

    async def enrichment(self, params):
        return await self._enrichment(params, "person")
        
    async def bulk(self, records):
        return await self._facade.safe_post("/person/bulk", records)
        
    async def identify(self, params):
        return await self._facade.safe_get("/person/identify", params)

    async def retrieve(self, pid):
        return await self._facade.safe_get("/person/retrieve/%s" % pid)

class Company(Endpoint):
    def __init__(self, facade):
        super().__init__(facade)
        self.search = Search(facade, "company")
        
    async def enrichment(self, params):
        return await self._enrichment(params, "company")
    
    async def cleaner(self, params):
        return await self._cleaner(params, "company")

class School(Endpoint):
    def __init__(self, facade):
        super().__init__(facade)

    async def cleaner(self, params):
        return await self._cleaner(params, "school")

class Location(Endpoint):
    def __init__(self, facade):
        super().__init__(facade)

    async def cleaner(self, params):
        return await self._cleaner(params, "location")

class Autocomplete(Endpoint):
    def __init__(self, facade):
        super().__init__(facade)

    async def __call__(self, params):
        autocompleteParams = {
            "field": None,
            "text": "",
            "size": 10,
            # TODO: deal with this...
            "pretty": "false"
        }
        autocompleteParams = {**autocompleteParams, **params}
        # TODO: check method and put the key instead of using param
        return await self._facade.safe_get("/autocomplete", autocompleteParams)

class PDLPY:
    def __init__(self, apiKey, basePath=None, version="v5"):
        self.apiKey = apiKey
        self.basePath = basePath or "https://api.peopledatalabs.com/%s" % version
        self.person = Person(self)
        self.company = Company(self)
        self.school = School(self)
        self.location = Location(self)
        self.autocomplete = Autocomplete(self)

    async def safe_request(self, endpoint, apiKeyInHeaders, *args, **kwargs):
        kwargs["url"] = self.basePath + endpoint
        
        if apiKeyInHeaders:
            kwargs["headers"]["X-Api-Key"] = self.apiKey
        else:
            kwargs["params"]["api_key"] = self.apiKey
        kwargs["headers"]["Accept-Encoding"] = "gzip"
        async with ClientSession() as session:
            resp = await session.request(*args, **kwargs)
            # TODO: error handling
            resp.raise_for_status()
            text = await resp.text()
            return json.loads(text)

    async def safe_get(self, endpoint, params={}):
        return await self.safe_request(
            endpoint = endpoint,
            apiKeyInHeaders = False,
            method = "GET",
            headers = {},
            params = params
        )

    async def safe_post(self, endpoint, json):
        return await self.safe_request(
            endpoint = endpoint,
            apiKeyInHeaders = True,
            method = "POST",
            headers = {
                'Content-Type': 'application/json'
            },
            json = json
        )

apiKey = "b67816762d96dbdc1c3d403abdd7a88fed7d2524635af7a5f7dc79a0b6d22aaf"
        
PDLPYClient = PDLPY(apiKey)

phone = '4155688415'

records = {
    "requests": [
        {
            "params": {
                "profile": ['linkedin.com/in/seanthorne22'],
            },
        },
        {
            "params": {
                "profile": ['linkedin.com/in/randrewn22'],
            },
        },
    ],
}

personSQL = "SELECT * FROM person WHERE location_country='mexico' AND job_title_role='health'AND phone_numbers IS NOT NULL;"

personElastic = {
  "query": {
    "bool": {
      "must": [
        { "term": { "location_country": 'mexico' } },
        { "term": { "job_title_role": 'health' } },
        { "exists": { "field": 'phone_numbers' } },
      ],
    },
  },
}
personID = 'qEnOZ5Oh0poWnQ1luFBfVw_0000'

website = 'peopledatalabs.com'

companySQL = "SELECT * FROM company WHERE tags='big data' AND industry='financial services' AND location.country='united states';"

companyElastic = {
  "query": {
    "bool": {
      "must": [
        { "term": { "website": 'peopledatalabs.com' } },
      ],
    },
  },
}

autocomplete = {
  "field": 'skill',
  "text": 'c++',
  "size": 10,
}

company = { "name": 'peopledatalabs' }

location = { "location": '455 Market Street, San Francisco, California 94105, US' }

school = { "name": 'university of oregon' }

def tc(f):
    print(f.__name__)
    f()

def person_enrichment():
    res = asyncio.run(PDLPYClient.person.enrichment({"phone": phone}))
    assert(type(res) is dict)

def person_identify():
    res = asyncio.run(PDLPYClient.person.identify({"phone": phone}))
    assert(type(res) is dict)

def person_bulk():
    res = asyncio.run(PDLPYClient.person.bulk(records))
    assert(type(res) is list)
    assert(len(res) == 2)

def person_search_sql():
    res = asyncio.run(PDLPYClient.person.search.sql({"searchQuery": personSQL, "size": 10}))
    assert(type(res) is dict)

def person_search_elastic():
    res = asyncio.run(PDLPYClient.person.search.elastic({"searchQuery": personElastic, "size": 10}))
    assert(type(res) is dict)

def person_retrieve():
    res = asyncio.run(PDLPYClient.person.retrieve(personID))
    assert(type(res) is dict)

def company_enrichment():
    res = asyncio.run(PDLPYClient.company.enrichment({"website": website}))
    assert(type(res) is dict)

def company_search_sql():
    res = asyncio.run(PDLPYClient.company.search.sql({"searchQuery": companySQL, "size": 10}))
    assert(type(res) is dict)
    
def company_search_elastic():
    res = asyncio.run(PDLPYClient.company.search.elastic({"searchQuery": json.dumps(companyElastic), "size": 10}))
    assert(type(res) is dict)

def test_autocomplete():
    res = asyncio.run(PDLPYClient.autocomplete(autocomplete))
    assert(type(res) is dict)

def company_cleaner():
    res = asyncio.run(PDLPYClient.company.cleaner(company))
    assert(type(res) is dict)

def location_cleaner():
    res = asyncio.run(PDLPYClient.location.cleaner(location))
    assert(type(res) is dict)

def school_cleaner():
    res = asyncio.run(PDLPYClient.school.cleaner(school))
    assert(type(res) is dict)

tc(person_enrichment)
tc(person_identify)
tc(person_bulk)
tc(person_search_sql)

#TODO
#tc(person_search_elastic)
tc(person_retrieve)
tc(company_enrichment)
tc(company_search_sql)

# TODO
#tc(company_search_elastic)
tc(test_autocomplete)

tc(company_cleaner)
tc(location_cleaner)
tc(school_cleaner)

  
