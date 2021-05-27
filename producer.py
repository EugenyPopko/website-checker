from kafka import KafkaProducer
import time
import json
import requests
from collections import namedtuple
import urllib
import re

WebsiteStatus = namedtuple('WebsiteStatus', ['status_code', 'response_time', 'regexp_check'])


class SiteChecker:

    def __init__(self, add_http = True):
        self.add_http = add_http

    def check_site(self, site, regexp = None):
    
        if self.add_http==True:
            site = 'http://{}'.format(site)
        try:
            response = requests.get(site)
            status_code = response.status_code
            elapsed = response.elapsed
            
            # try get reg_exp
            reg_check = 'Not checked'
            if regexp and regexp != "":
                req = urllib.request.Request(site)
                try:
                    content = urllib.request.urlopen(req)
                    CHECK_REGEX = re.compile(regexp)
                    if CHECK_REGEX.search(content.read().decode('utf-8')):
                        reg_check = 'regex found'
                    else:
                        reg_check = 'regex not found'
                except urllib.error.HTTPError as e:
                    pass
            
        except requests.exceptions.ConnectionError:
            status_code = '000'
            #elapsed = response.elapsed
            
        website_status = WebsiteStatus(status_code, elapsed, reg_check)
        print("SITE: " + site + "; "+ str(website_status.status_code) +"; "+ str(website_status.response_time) + "; " + str(website_status.regexp_check))
        return website_status 


class Producer:

    def __init__(self, config):
        self.server = config['kafka']['server'],
        self.topic = config['kafka']['topic'],
        self.period = config['period']
        self.sites_dict = config['sites']
        
        self.checker = SiteChecker()

        try:
            self.producer = KafkaProducer(
                    bootstrap_servers=self.server,
                    security_protocol="SSL",
                    ssl_cafile="auth/ca.pem",
                    ssl_certfile="auth/service.cert",
                    ssl_keyfile="auth/service.key"
            )
        except Exception as e:
            print(f'Error connecting to Kafka broker: {e}')
            raise e

    def produce(self):
    
        while True:
            start = time.time()
            for key in self.sites_dict:
                site_status = self.checker.check_site(key, self.sites_dict[key])
                t = time.localtime()
                current_time = time.strftime("%H:%M:%S", t)
                message = {
                    'site': key,
                    'status_code': site_status.status_code,
                    'response_time': site_status.response_time,
                    'regexp_check': site_status.regexp_check,
                    'time': current_time
                }
                self.producer.send(self.topic, json.dumps(message).encode('utf-8'))
            end_time = time.time() - start
            wait = self.period - end_time
            if wait<0:
                wait = 0
            time.sleep(wait)