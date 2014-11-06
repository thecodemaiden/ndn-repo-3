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
import logging
from bson import Binary, BSON

class NdnRepoClient(object):
    def __init__(self, repoPrefix=None):
        super(NdnRepoClient, self).__init__()
        if repoPrefix is not None:
            self.repoPrefix = Name(repoPrefix)
        else:
            self.repoPrefix = Name('/test/repo')
        self.loadKey()
        self.isStopped = False

        self.log = logging.getLogger(str(self.__class__))
        h = logging.FileHandler('repo_client.log')
        h.setFormatter(logging.Formatter(
            '%(asctime)-15s %(levelname)-8s %(funcName)s\n\t%(message)s'))
        self.log.addHandler(h)
        self.log.setLevel(logging.DEBUG)
        self.isStopped = True
        logging.getLogger('trollius').addHandler(h)

    def loadKey(self, keyFile='bms_key.pri'):
        self.keyId = '\xa2\xeb9\xbcGo$\xad\xbf\xe9?k\xb2\xb8|\xa8 E\x96\x13\x1e\xb9\x97\x91Z\xf6\xda\xd1]\xa1lD'
        with open(keyFile, 'r') as keyFile:
            binDer = keyFile.read()
            self.privateKey = RSA.importKey(binDer)

    def onDataReceived(self, interest, data):
        # now we have to retrieve the key
        dataContent = str(data.getContent())
        dataContent = BSON(dataContent)
        dataDict = dataContent.decode()
        dataVal = str(dataDict[u'value'])
        keyTs = dataVal[:8]
        keyDataName = Name('/ndn/ucla.edu/bms/melnitz/kds').append(keyTs).append(self.keyId)
        i = Interest(keyDataName)
        i.setMustBeFresh(False)
        i.setInterestLifetimeMilliseconds(2000)

        def onKeyDataReceived(keyInterest, keyData):
            print 'Got key for {}'.format(keyInterest.getName())
            cipherText = str(keyData.getContent())
            symKeyRaw = self.privateKey.decrypt(cipherText)
            symKey = symKeyRaw[-64:].decode('hex')

            msg = dataVal[8:]
            iv = msg[:16]
            cipherText = msg[16:]

            cipher = AES.new(key=symKey, IV=iv, mode=AES.MODE_CBC)
            decData = cipher.decrypt(cipherText)

            print '---------'
            print data.getName()
            print '---------'
            print decData
            print '--------\n'

        def onKeyTimeout(keyInterest):
            print 'Could not get decryption key for {}'.format(data.getName())

        self.keyFace.expressInterest(i, onKeyDataReceived, onKeyTimeout)

    def onDataTimeout(self, interest):
        print "Timed out on {}".format(interest.toUri())

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

    def assembleDataName():
        schemaStr = ('/ndn/ucla.edu/bms/{building}/data/{room}/electrical/panel/{panel_name}/{quantity}/{data_type}')
        keyNames = ['building', 'room', 'panel_name', 'quantity', 'data_type']
        valueDict = {}
        for k in keyNames:
            value  = raw_input('{}: '.format(k)).strip()
            valueDict[k] = value if len(value)>0 else '_'
        dataName = schemaStr.format(**valueDict)
        return Name(dataName)

    l = logging.getLogger('trollius')
    l.addHandler(logging.StreamHandler())
    client = NdnRepoClient()
    clientThread = threading.Thread(target=client.start)
    clientThread.daemon = True
    try:
        clientThread.start()
        while True:
            dataName = assembleDataName()
            client.loop.call_soon_threadsafe(client.sendDataRequestCommand,
                    dataName)
            time.sleep(4)
    except (EOFError, KeyboardInterrupt):
        pass
    except Exception as e:
        print e
    finally:
        client.stop()
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
