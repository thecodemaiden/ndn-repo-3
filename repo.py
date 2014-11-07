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
# - allow multiple schemata
# - use config file to set signing key
# - support deletion

import pymongo as mongo
from repo_command_pb2 import RepoCommandParameterMessage
from repo_response_pb2 import RepoCommandResponseMessage

from pyndn import Name, Data, Interest, ThreadsafeFace
from pyndn.security import KeyChain
from pyndn.encoding import ProtobufTlv
from bson.binary import Binary
from bson import BSON
import trollius as asyncio
import logging
import struct

from datetime import datetime
class NdnSchema(object):
    #TODO: how do we include annotations, e.g units?
    # are these separate from data points (I think so...)
    SCHEMA_STR = 'string'
    SCHEMA_INT = 'int'
    SCHEMA_BINARY = 'blob'
    SCHEMA_TIMESTAMP = 'time'

    def __init__(self, nameSchema, schemaProps=None):
        super(NdnSchema, self).__init__()
        self._extractKeysFromNameSchema(nameSchema)
        tableNames = self.fieldMappings.keys()
        if len(tableNames) == 0:
            raise ValueError('Name schema has no keys')
        firstKeyIdx = sorted(self.fieldMappings.values())[0]
        self.dbPrefix = nameSchema[:firstKeyIdx]

    def setSchemaTypes(self, **properties):
        """
        Given key=type arguments, save the types so data can be formatted before
        inserting into the database.
        """
        #TODO: what to do with bad keys?
        for k in self.fieldTypes:
            try:
                self.fieldTypes[k] = properties[k]
            except KeyError:
                pass
    
    def addField(self, fieldName, fieldType=None):
        if fieldType is None:
            fieldType = self.SCHEMA_STR
        if fieldName in self.fieldMappings:
            #error ? warning ?
            pass
        else:
            self.fieldTypes[fieldName] = fieldType

    def _extractKeysFromNameSchema(self, nameSchema):
        self.fieldMappings = {}
        self.prototype  = []
        self.fieldTypes = {}
        for i in range(len(nameSchema)):
            component = str(nameSchema[i].getValue())
            if component[0] == '<' and component[-1]=='>':
                fieldName = component.strip('<>')
                self.fieldMappings[fieldName] = i
                self.fieldTypes[fieldName] = self.SCHEMA_STR
                self.prototype.append('')
            else:
                self.prototype.append(component)

    def matchNameToSchema(self, name):
        """
         Find the mapping between the name components and the 
         mappings in our schema
         :raise: ValueError if the name doesn't match the schema's prefix
        """
        foundDict = {}
        if len(name) > len(self.prototype):
            self.log.warn("Warning, data name is longer than schema")
        if not self.dbPrefix.match(name):
            raise ValueError("Name is not under db prefix")
        else:
            for (k,v) in self.fieldMappings.items():
                try:
                    value = str(name[v].getValue()).strip()
                    if value != '_':
                        foundDict[k] = str(value)
                except IndexError:
                    pass
        return foundDict

    def sanitizeData(self, dataDict):
        """
        Convert each value to the schema type defined for it.
        :raise: struct.error if a SCHEMA_INT value is invalid
        :raise: ValueError if a SCHEMA_TIMESTAMP is invalid
        """
        outputDict = {}
        for k in dataDict:
            try:
                fieldType = self.fieldTypes[k]
            except KeyError:
                outputDict[k] = dataDict[k]# don't know this data type
            else:
                if fieldType == self.SCHEMA_STR:
                    outputDict[k] = str(dataDict[k])
                elif fieldType == self.SCHEMA_INT:
                    try:
                        outputDict[k] = int(dataDict[k])
                    except ValueError:
                        # raises struct.error if this doesn't work
                        outputDict[k] = struct.unpack("!Q", dataDict[k])
                elif fieldType == self.SCHEMA_BINARY:
                    outputDict[k] = Binary(str(dataDict[k]))
                elif fieldType == self.SCHEMA_TIMESTAMP:
                    try:
                        timeVal = float(dataDict[k])
                    except ValueError:
                        # raise struct.error if this doesn't work
                        timeVal = struct.unpack("!Q", dataDict[k])[0]
                    outputDict[k] = datetime.fromtimestamp(timeVal)
                else:
                    raise ValueError('Bad schema type: {}'.format(fieldType))
                    
        return outputDict


class NdnHierarchicalRepo(object):
    def __init__(self, repoPrefix=None):
        super(NdnHierarchicalRepo, self).__init__()
        self.schemaList = []
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
        s.setLevel(logging.INFO)
        self.log.addHandler(s)
        self.log.setLevel(logging.INFO)

    def addSchema(self, schema):
        if not isinstance(schema, NdnSchema):
            schema = NdnSchema(schema)
            schema.addField('value', NdnSchema.SCHEMA_BINARY)
            schema.addField('ts', NdnSchema.SCHEMA_TIMESTAMP)
        schema.log = self.log
        self.schemaList.append(schema)
        self.dataPrefix = schema.dbPrefix

    def initializeDatabase(self):
        self.dbClient = mongo.MongoClient()
        # TODO: different DB name for different schemata?
        self.db = self.dbClient['bms']

    def _registerFailed(self, prefix):
        self.log.info('Registration failure')
        if prefix == self.dataPrefix:
            callback = self.handleDataInterests
        elif prefix == self.repoPrefix:
            callback = self.handleCommandInterests

        self.loop.call_later(5, self._register, prefix, callback)

    def _register(self, prefix, callback):
        self.face.registerPrefix(prefix, callback, self._registerFailed) 
        
    def insertData(self, data):
        dataName = data.getName()
        self.log.info('Inserting {}'.format(dataName))
        useSchema = (s for s in self.schemaList 
                if s.dbPrefix.match(dataName)).next()
        
        #exclude timestamp...
        dataFields = useSchema.matchNameToSchema(dataName[:-1])
        if len(dataFields) == 0:
            self.log.error('Invalid data name for schema')
   
        dataValue = Binary(str(data.getContent()))
        tsComponent = str(dataName[-1].getValue())
        tsConverted = float(struct.unpack("!Q", tsComponent)[0])/1000
        dataFields.update({'value':dataValue, 'ts':tsConverted})
        dataFields = useSchema.sanitizeData(dataFields)
        dataCollection = self.db['data']
        new_id = dataCollection.update(dataFields, dataFields, upsert=True)
        self.log.debug('Inserted object {}'.format(new_id))

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
        # TODO: bulk data return?
        
        # we match the components to the name, and any '_' components
        # are discarded. Then we run a MongoDB query and append the
        # object id to allow for excludes

        chosenSchema = (s for s in self.schemaList 
                if s.dbPrefix.match(prefix)).next()

        interestName = interest.getName()
        responseName = Name(interestName)
        nameFields = chosenSchema.matchNameToSchema(interestName)
        self.log.info("Data requested with params:\n\t{}".format(nameFields))
        allResults = []
        
        for result in self.db['data'].find(nameFields):
            dataId = result[u'_id']
            self.log.debug("Found object {}".format(dataId))
            allResults.append(result)

        #responseName.append(str(dataId))
        responseObject = {'count':len(allResults), 'results':allResults}
        responseData = Data(responseName)
        resultEncoded = BSON.encode(responseObject)
        responseData.setContent(resultEncoded)
        transport.send(responseData.wireEncode().buf())


    def _onInsertionDataReceived(self, interest, data):
        # TODO: update status in processingList
        self.log.debug("Got {} in response to {}".format(data.getName(), 
                interest.getName()))
        self.insertData(data)
        

    def _onInsertionDataTimeout(self, interest):
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
            fullSchemaName = Name()
            for component in commandMessage.command.name.components:
                fullSchemaName.append(component)
                if component == '_':
                    continue
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
        i = Interest(dataName)
        i.setChildSelector(1)
        i.setInterestLifetimeMilliseconds(4000)
        try:
            self.pendingProcesses[processId] = (dataName, 100)
        except NameError:
            pass # wasn't valid insert request
        else:
            self._insertFace.expressInterest(i, 
                    self._onInsertionDataReceived,
                    self._onInsertionDataTimeout)

if __name__ == '__main__':
    logging.getLogger('trollius').addHandler(logging.StreamHandler())
    # TODO: explicitly including ts in schema name leads to errors with short names
    schemaStr = ('/ndn/ucla.edu/bms/<building>/data/<room>/electrical/panel/<panel_name>/<quantity>/<quantity_type>/')
    schema = Name(schemaStr)
    print "Schema: {}".format(schemaStr)
    repo = NdnHierarchicalRepo()
    repo.addSchema(schema)
    repo.start()
