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

from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256

import trollius as asyncio
from trollius import From
import logging
from bson import Binary, BSON
import json
import string

class NdnRepoClient(object):
    def __init__(self, repoPrefix=None):
        super(NdnRepoClient, self).__init__()
        if repoPrefix is not None:
            self.repoPrefix = Name(repoPrefix)
        else:
            self.repoPrefix = Name('/test/repo')
        self.loadKey()
        self.isStopped = False
        self.dataReady = asyncio.Event()
        self.resultQueue = asyncio.Queue()

        self.log = logging.getLogger(str(self.__class__))
        h = logging.FileHandler('repo_client.log')
        s = logging.StreamHandler()
        logFormatter = logging.Formatter(
            '%(asctime)-15s %(levelname)-8s %(funcName)s\n\t%(message)s')
        s.setFormatter(logFormatter)
        h.setFormatter(logFormatter)
        self.log.addHandler(h)
        self.log.addHandler(s)
        s.setLevel(logging.WARN)
        self.log.setLevel(logging.DEBUG)
        self.isStopped = True
        logging.getLogger('trollius').addHandler(h)

    def loadKey(self, keyFile='bms_key.pri'):
        self.keyId = '\xa2\xeb9\xbcGo$\xad\xbf\xe9?k\xb2\xb8|\xa8 E\x96\x13\x1e\xb9\x97\x91Z\xf6\xda\xd1]\xa1lD'
        with open(keyFile, 'r') as keyFile:
            binDer = keyFile.read()
            self.privateKey = RSA.importKey(binDer)



    def _decryptAndPrintRecord(self, recordData, keyName, parentDoc):

        def cleanString(dirty):
            return ''.join(filter(string.printable.__contains__, str(dirty)))

        def onKeyDataReceived(keyInterest, keyData):
            self.log.debug('Got key for {}'.format(keyInterest.getName()))
            cipherText = str(keyData.getContent())
            symKeyRaw = self.privateKey.decrypt(cipherText)
            symKey = symKeyRaw[-64:].decode('hex')

            msg = recordData[8:]
            iv = msg[:16]
            cipherText = msg[16:]

            cipher = AES.new(key=symKey, IV=iv, mode=AES.MODE_CBC)
            decData = cleanString(cipher.decrypt(cipherText))

            fromJson = json.loads(decData)
            fromJson.update(parentDoc)

            #self.resultQueue.put_nowait(fromJson)
            print '---------'
            print str(fromJson)

        def onKeyTimeout(keyInterest):
            self.log.error('Could not get decryption key for {}'.format(data.getName()))
            #self.resultQueue.put_nowait({})

        i = Interest(keyName)
        i.setMustBeFresh(False)
        i.setInterestLifetimeMilliseconds(2000)
        self.keyFace.expressInterest(i, onKeyDataReceived, onKeyTimeout)

    @asyncio.coroutine
    def collectResults(self, allData):
        self.dataReady.set()
        for record in allData:
            parentDoc = {k:v for (k,v) in record.items() if k not in [u'_id', u'value']} 
            aDataVal = str(record[u'value'])
            keyTs = aDataVal[:8]
            keyDataName = Name('/ndn/ucla.edu/bms/melnitz/kds').append(keyTs).append(self.keyId)
            self._decryptAndPrintRecord(aDataVal, keyDataName, parentDoc)
        receivedVals = []
        #for i in range(len(allData)):
        #v = yield From(self.resultQueue.get())
        #receivedVals.append(v)
        print receivedVals

    def onDataReceived(self, interest, data):
        # now we have to retrieve the key
        dataContent = str(data.getContent())
        dataContent = BSON(dataContent)
        dataDict = dataContent.decode()

        dataCount = dataDict['count']
        print '---------'
        print 'Got {} result(s)'.format(dataCount)

        allData = dataDict['results']
        asyncio.async(self.collectResults(allData))

    def onDataTimeout(self, interest):
        self.log.warn("Timed out on {}".format(interest.toUri()))
        self.dataReady.set()

    def sendDataRequestCommand(self, dataName):
        interest = Interest(dataName)
        interest.setInterestLifetimeMilliseconds(4000)
        self.face.expressInterest(interest, self.onDataReceived, self.onDataTimeout)
        return interest

    def stop(self):
        self.isStopped = True

    def start(self):
        self.isStopped = False
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        self.face = ThreadsafeFace(self.loop, '')
        self.keyFace = ThreadsafeFace(self.loop, 'borges.metwi.ucla.edu')
        self.face.stopWhen(lambda:self.isStopped)
        self.keyFace.stopWhen(lambda:self.isStopped)

        k = KeyChain()
        self.face.setCommandSigningInfo(k, k.getDefaultCertificateName())
        try:
            self.loop.run_forever()
        finally:
            self.face.shutdown()
        

def main():
    import threading
    import time

    client = NdnRepoClient()
    def assembleDataName():
        schemaStr = ('/ndn/ucla.edu/bms/{building}/data/{room}/electrical/panel/{panel_name}/{quantity}/{data_type}')
        keyNames = ['building', 'room', 'panel_name', 'quantity', 'data_type']
        valueDict = {}
        for k in keyNames:
            value  = raw_input('{}: '.format(k)).strip()
            valueDict[k] = value if len(value)>0 else '_'
        dataName = schemaStr.format(**valueDict)
        return Name(dataName)

    @asyncio.coroutine
    def parseDataRequest():
        while True:
            client.dataReady.clear()
            dataName = assembleDataName()
            client.loop.call_soon_threadsafe(client.sendDataRequestCommand,
                    dataName)
            yield From(client.dataReady.wait())

    clientThread = threading.Thread(target=client.start)
    clientThread.daemon = True
    clientThread.start()
    
    try:
        asyncio.get_event_loop().run_until_complete(parseDataRequest())
    except (EOFError, KeyboardInterrupt):
        pass
    except Exception as e:
        print e
    finally:
        client.stop()


   #while True:
   #    try:
   #        pass
   #    except (EOFError, KeyboardInterrupt):
   #        break
   #    except Exception as e:
   #        print e

if __name__ == '__main__':
    main()
