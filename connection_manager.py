# -*- coding: utf-8 -*-
"""
@author: nabizade
"""
# Only works on Linux Ubuntu

import warnings
import traceback
import urllib
import time
from user_agent import generate_user_agent
from stem import Signal
from stem.control import Controller
warnings.filterwarnings('ignore')

class ConnectionManager:
    def __init__(self):
        self.new_ip = "0.0.0.0"
        self.old_ip = "0.0.0.0"
        self.new_identity()

    @classmethod
    # classmethod1(private)
    def _get_connection(self):
        """
        TOR new connection
        """
        with Controller.from_port(port=9051) as controller:
            controller.authenticate(password="bedel")
            controller.signal(Signal.NEWNYM)
            controller.close()

    @classmethod
    # classmethod2 (private)
    def request(self, url):
        """
        TOR communication through local proxy
        :param url: web page to parser
        :return: request
        """
        try:
            proxy_support = urllib.request.ProxyHandler({"http" : "127.0.0.1:8118"})

            opener = urllib.request.build_opener(proxy_support)

            opener.addheaders = [('User-agent',
                generate_user_agent(device_type="desktop",
                    os=('mac', 'linux')))]

            urllib.request.install_opener(opener)

            request =opener.open(url)
            return request

        except urllib.request.HTTPError as e:
            return e.message

    def new_identity(self):
        """
        new connection with new IP
        """
        # First Connection
        if self.new_ip == "0.0.0.0":
            self._get_connection()
            self.new_ip = str(self.request("http://icanhazip.com/").read())\
            .split("b'")[1].split('\\')[0]

        else:
            self.old_ip = self.new_ip
            self._get_connection()
            self.new_ip = str(self.request("http://icanhazip.com/").read())\
            .split("b'")[1].split('\\')[0]

        seg = 0

        # If we get the same ip, we'll wait 5 seconds to request a new IP
        while self.old_ip == self.new_ip:
            time.sleep(5)
            seg += 5
            print ("Waiting to obtain new IP: %s Seconds" % seg)
            self.new_ip = str(self.request("http://icanhazip.com/").read())\
            .split("b'")[1].split('\\')[0]
            # If waiting time is more than 45 seconds, we'll move on with previous IP
            if seg > 45:
                break

        print ("New connection with IP: %s" % self.new_ip)
