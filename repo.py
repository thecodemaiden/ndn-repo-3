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

"""
This is a repo storing hierarchical data in SQL, using recursive queries to
retrieve data. It uses a similar protocol as repo-ng, except that message parts which are naturally part of an Interest packet are not repeated in the RepoCommandParameterMessage
"""

#TODO:
# - creating tables from schema (updating if necessary)
# - respond to insert command by inserting into right places
# - respond to interests for data (register prefix before first substitution key
# - allow multiple schemata
# - use config file to set signing key

import pymongo as mongo
from repo_command_pb2 import RepoCommandParameterMessage
from repo_response_pb2 import RepoCommandResponseMessage

from pyndn import Name, Data, Interest, ThreadsafeFace
from pyndn.security import KeyChain
from pyndn.encoding import ProtobufTlv
from bson.binary import Binary
import trollius as asyncio
import logging
import struct

class NdnSchema(object):
    #TODO: move schemata into a class
    # so we can have additional properties (units, etc)
    pass

class NdnHierarchicalRepo(object):
    def __init__(self, nameSchema, repoPrefix=None):
        self._extractKeysFromNameSchema(nameSchema)
        tableNames = self.fieldMappings.keys()
        if len(tableNames) == 0:
            raise ValueError('Name schema has no keys')
        firstKeyIdx = sorted(self.fieldMappings.values())[0]
        self.dataPrefix = nameSchema[:firstKeyIdx]
        if repoPrefix is None:
            self.repoPrefix = Name('/test/repo')
        else:
            self.repoPrefix = Name(repoPrefix)
        self.keyChain = KeyChain()
        self.currentProcessId = 0

        self.pendingProcesses = {}
        self.log = logging.getLogger(str(self.__class__))
        s = logging.StreamHandler()
        formatter = logging.Formatter(
                '%(asctime)-15s %(funcName)s\n\t %(message)s')
        s.setFormatter(formatter)
        s.setLevel(logging.WARNING)
        self.log.addHandler(s)

    def initializeDatabase(self):
        self.dbClient = mongo.MongoClient()
        self.db = self.dbClient['bms']

    def _extractKeysFromNameSchema(self, nameSchema):
        self.fieldMappings = {}
        self.prototype  = []
        for i in range(len(nameSchema)):
            component = str(nameSchema[i].getValue())
            if component[0] == '<' and component[-1]=='>':
                fieldName = component.strip('<>')
                self.fieldMappings[fieldName] = i
                self.prototype.append('')
            else:
                self.prototype.append(component)

    def _registerFailed(self, prefix):
        self.log.info('Registration failure')
        if prefix == self.dataPrefix:
            callback = self.handleDataInterests
        elif prefix == self.repoPrefix:
            callback = self.handleCommandInterests

        self.loop.call_later(5, self._register, prefix, callback)

    def _register(self, prefix, callback):
        self.face.registerPrefix(prefix, callback, self._registerFailed) 

    def matchNameToSchema(self, name):
        """
         Find the mapping between the name components and the 
         mappings in our schema
        """
        foundDict = {}
        if len(name) > len(self.prototype):
            self.log.warn("Warning, data name is longer than schema")
        if not self.dataPrefix.match(name):
            self.log.error("Name is not under db prefix")
        else:
            try:
                for (k,v) in self.fieldMappings.items():
                    foundDict[k] = str(name[v].getValue())
            except IndexError:
                self.log.error("Name is too short")
                return {}
        return foundDict
        
    def insertData(self, data):
        dataName = data.getName()
        self.log.info('Inserting {}'.format(dataName))
        dataFields = self.matchNameToSchema(dataName)
        if len(dataFields) == 0:
            return
   
        dataValue = Binary(str(data.getContent()))
        tsComponent = str(dataName[-1].getValue())
        dataTs = struct.unpack("!Q", tsComponent)[0]
        dataFields.update({'value':dataValue, 'ts':dataTs})
        dataCollection = self.db['data']
        new_id = dataCollection.insert(dataFields)
        #NOTE: do i need to do something with this id?

    def start(self):
        self.initializeDatabase()
        self.loop = asyncio.get_event_loop()
        self.face = ThreadsafeFace(self.loop, '')
        self.face.setCommandSigningInfo(self.keyChain, 
                self.keyChain.getDefaultCertificateName())
        self._register(self.dataPrefix, self.handleDataInterests)
        self._register(self.repoPrefix, self.handleCommandInterests)


        #TODO: figure out why nfdc doesn't forward to borges
        self._insertFace = ThreadsafeFace(self.loop, 'borges.metwi.ucla.edu')

        try:
            self.loop.run_forever()
        except Exception as e:
            self.log.exception(e, exc_info=True)
            self.face.shutdown()

    def handleDataInterests(self, prefix, interest, transport, prefixId):
        # TODO: verification
        pass

    def _onInsertionDataReceived(self, interest, data):
        # TODO: update status in processingList
        self.log.debug("Got {} in response to {}".format(data.getName(), 
                interest.getName()))
        self.insertData(data)
        

    def _onInstertionDataTimeout(self, interest):
        self.log.warn("Timeout on {}".format(interest.getName()))

    def handleCommandInterests(self, prefix, interest, transport, prefixId):
        # TODO: verification
        interestName = interest.getName()
        if len(interestName) <= len(prefix)+4:
            self.log.info("Bad command interest")
        commandName = str(interestName[len(prefix)].getValue())
        responseMessage =  RepoCommandResponseMessage()
        if commandName == 'insert':
            commandParams = interestName[len(prefix)+1].getValue()
            commandMessage = RepoCommandParameterMessage()
            ProtobufTlv.decode(commandMessage, commandParams)
            dataName = Name()
            for component in commandMessage.command.name.components:
                dataName.append(component)
            self.log.info("Insert request for {}".format(dataName))
            responseMessage.response.status_code = 100
            processId = self.currentProcessId
            self.currentProcessId += 1
            responseMessage.response.process_id = processId
        else:
            responseMessage.response.status_code = 403
        responseData = Data(interestName)
        responseData.setContent(ProtobufTlv.encode(responseMessage))
        transport.send(responseData.wireEncode().buf())

        # now send the interest out to the publisher
        # TODO: pendingProcesses becomes list of all processes as objects
        try:
            self.pendingProcesses[processId] = (dataName, 100)
        except NameError:
            pass # wasn't valid insert request
        else:
            self._insertFace.expressInterest(dataName, 
                    self._onInsertionDataReceived,
                    self._onInstertionDataTimeout)
            
        


if __name__ == '__main__':
    logging.getLogger('trollius').addHandler(logging.StreamHandler())
    # TODO: should we explicitly state ts in schema name?
    # TODO: won't work until schema becomes an object
    schemaStr = ('/ndn/ucla.edu/bms/<building>/data/<room>/electrical/panel/<panel_name>/<quantity>/')
    schema = Name(schemaStr)
    print "Schema: {}".format(schemaStr)
    repo = NdnHierarchicalRepo(schema)
    repo.start()
