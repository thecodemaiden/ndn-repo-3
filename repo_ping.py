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


from pyndn import Name, Face, Data, Interest
from pyndn.encoding import ProtobufTlv
from repo_command_pb2 import RepoCommandParameterMessage
from repo_response_pb2 import RepoCommandResponseMessage
from pyndn.security import KeyChain
from collections import defaultdict
repoPrefix = Name('/test/repo')
gotResponse = False

def onDataReceived(interest, data):
    global gotResponse
    responseMessage = RepoCommandResponseMessage()
    ProtobufTlv.decode(responseMessage, data.getContent())
    print 'Status code: {}'.format(responseMessage.response.status_code)
    gotResponse = True

def onTimeout(interest):
    global gotResponse
    print "Timed out on {}".format(interest.toUri())
    gotResponse = True

def makeRepoInsertCommand():
    dataName = Name(assembleDataName())
    print dataName
    commandMessage = RepoCommandParameterMessage()
    command = commandMessage.command
    for component in dataName:
        command.name.components.append(str(component.getValue()))
    command.start_block_id = command.end_block_id = 0
    commandComponent = ProtobufTlv.encode(commandMessage)

    interestName = Name(repoPrefix).append('insert')
    interestName.append(commandComponent)
    interest = Interest(interestName)
    interest.setInterestLifetimeMilliseconds(4000)
    return interest

def assembleDataName():
    schemaStr = ('/ndn/ucla.edu/bms/{building}/data/{room}/electrical/panel/{panel_name}/{quantity}/{data_type}')
    keyNames = ['building', 'room', 'panel_name', 'quantity', 'data_type']
    valueDict = {}
    for k in keyNames:
        valueDict[k] = raw_input('{}: '.format(k))
    dataName = schemaStr.format(**valueDict)
    return dataName


def main():
    global gotResponse
    f = Face()
    k = KeyChain()
    f.setCommandSigningInfo(k, k.getDefaultCertificateName())
    while True:
        try:
            i = makeRepoInsertCommand()
            f.makeCommandInterest(i)
            gotResponse = False
            f.expressInterest(i, onDataReceived, onTimeout)
            while not gotResponse:
                f.processEvents()
            print
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print e

if __name__ == '__main__':
    main()
