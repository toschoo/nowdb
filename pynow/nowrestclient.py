import requests
import json

#url = "https://test.dm.loka.systems"
url = "http://localhost:50678"
auth = "/api/auth"
query = "/api/query"
u = 'tobias.schoofs@gmx.net'
pw = 'Tobias12345'
wmo='wmo'

# "https://test.dm.loka.systems/auth/login", \

with requests.Session() as s:
    
    r = s.post(url+auth, \
               params={'username':u,'password':pw,'scope':wmo}, \
               verify=False)

    # print "COOKIE : %s" % r.cookies['dm']
    print "SESSION: %s" % s.cookies['nrtok']
    print r.text

    body = """{
              "range" : {"from" : "a", "to" : "z"},
              "interval" : "1s",
              "targets" : [
                 {"refId" : "1", "target" : "station"},
                 {"refId" : "2", "target" : "weather"}
              ],
              "format" : "json",
              "maxDataPoints" : 100,
              "rawQuery" : "select skey from station"
           }"""
    r = s.post(url+query, body)
    if r.status_code != requests.codes.ok:
       print "Status Code: %s" % r.status_code
       print "%s" % r.text
    else:
       print "%s" % r.json()

    """

    dec = json.JSONDecoder()
    o = dec.decode(r.text)

    for c in o['data']:
      print "%04d (%03d) %s, %s" % (c['id'], c['parentId'], c['name'], c['location'])
    """



