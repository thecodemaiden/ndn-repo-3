#!/usr/bin/python
# -*- Mode:python; c-file-style:"gnu"; indent-tabs-mode:nil -*- */
#
# Copyright (C) 2014 Regents of the University of California.
# Author: Adeola Bannis <thecodemaiden@gmail.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# A copy of the GNU General Public License is in the file COPYING.


from pyndn import Name, ThreadsafeFace, Data, Interest
from pyndn.encoding import ProtobufTlv
from repo_command_pb2 import RepoCommandParameterMessage
from repo_response_pb2 import RepoCommandResponseMessage
from pyndn.security import KeyChain
import trollius as asyncio
from trollius import From
import logging
class NdnRepoPing(object):
    """
     A repo is supposed to be notified of new data by the publisher. Since the 
     publisher doesn't know about this repo, I will 'manually' poke the repo to      insert data points.
    """
    def __init__(self, repoPrefix=None):
        super(NdnRepoPing, self).__init__()
        if repoPrefix is not None:
            self.repoPrefix = repoPrefix
        else:
            self.repoPrefix = Name('/test/repo')
        self.repoWatchNames = []
        self.log = logging.getLogger(str(self.__class__))
        h = logging.FileHandler('repo_ping.log')
        h.setFormatter(logging.Formatter(
            '%(asctime)-15s %(levelname)-8s %(funcName)s\n\t%(message)s'))
        self.log.addHandler(h)
        self.log.setLevel(logging.DEBUG)
        self.isStopped = True
        logging.getLogger('trollius').addHandler(h)

    def onDataReceived(self, interest, data):
        self.log.debug('Response to {}'.format(interest.getName()))
        responseMessage = RepoCommandResponseMessage()
        ProtobufTlv.decode(responseMessage, data.getContent())
        self.log.debug('Status code: {}'.format(
            responseMessage.response.status_code))

    def onTimeout(self, interest):
        self.log.info('Timed out on {}'.format(interest.getName()))

    def sendRepoInsertCommand(self, dataName):
        self.log.debug('Sending insert command for {}'.format(dataName))
        commandMessage = RepoCommandParameterMessage()
        command = commandMessage.command
        for component in dataName:
            command.name.components.append(str(component.getValue()))
        command.start_block_id = command.end_block_id = 0
        commandComponent = ProtobufTlv.encode(commandMessage)

        interestName = Name(self.repoPrefix).append('insert')
        interestName.append(commandComponent)
        interest = Interest(interestName)
        interest.setInterestLifetimeMilliseconds(4000)
        self.face.makeCommandInterest(interest)
        
        self.face.expressInterest(interest, self.onDataReceived, self.onTimeout)

    @asyncio.coroutine
    def sendNextInsertRequest(self):
        sleepTime = 60*15
        while True:
            for name in self.repoWatchNames:
                self.sendRepoInsertCommand(name)
            yield From(asyncio.sleep(sleepTime))

    def start(self):
        self.isStopped = False
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.face = ThreadsafeFace(self.loop, '')
        k = KeyChain()
        self.face.setCommandSigningInfo(k, k.getDefaultCertificateName())
        self.face.stopWhen(lambda:self.isStopped)
        try:
            self.loop.run_until_complete(self.sendNextInsertRequest())
        finally:
            self.face.shutdown()

    def stop(self):
        self.isStopped = True

    def addWatchName(self, newName):
        if newName not in self.repoWatchNames:
            self.repoWatchNames.append(Name(newName))

def assembleDataName():
    schemaStr = ('/ndn/ucla.edu/bms/{building}/data/{room}/electrical/panel/{panel_name}/{quantity}/{data_type}')
    keyNames = ['building', 'room', 'panel_name', 'quantity', 'data_type']
    valueDict = {}
    for k in keyNames:
        valueDict[k] = raw_input('{}: '.format(k))
    dataName = schemaStr.format(**valueDict)
    return dataName


def main():
    import threading
    import sys
    import time
    p = NdnRepoPing()

    pingThread = threading.Thread(target=p.start)
    pingThread.daemon = True
    pingThread.start()

    try:
        inputFile = sys.argv[1]
        with open(inputFile, 'r') as nameList:
            for line in nameList:
                name = line.strip()
                if len(name) > 0:
                    newName = Name(name)
                    p.loop.call_soon_threadsafe(p.addWatchName, newName)
    except IndexError:
        inputFile = None

    try:
        while True:
            if inputFile is None:
                print 'Insert repo watch name'
                newName = assembleDataName()
                print 'Adding {} to watchlist'.format(newName)
                p.loop.call_soon_threadsafe(p.addWatchName, newName)
            else:
                time.sleep(10)
    except (KeyboardInterrupt):
        pass
    except Exception as e:
        print e

if __name__ == '__main__':
    main()
