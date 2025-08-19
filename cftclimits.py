'''
Id: "$Id: cftclimits.py,v 1.18 2025/07/23 16:01:32 Exp $"
Description:
Test: 'qz.remoterisk.tests.unittests.cftc.limits.cftclimits'
'''
import sandra
import qztable
import urllib.parse
from qz.tools.gov.lib import logging
from qz.core import bobfns
from qz.data.qztable_utils import vMerge
from qz.core.rester.restutil import Rest
from qz.rester.lib.authentication import PKI
from qz.core.rester.restutil import tableFromResterJson
from qzsix.moves.urllib.error import HTTPError
from qz.rester.lib import authentication
from qz.ui.utils import isQzwin
from qz.core.dates import subtractBusinessDays
from qz.remoterisk.cftc.utils.config import CFTCConf
from qz.data.qztable_utils import tableFromListOfDicts
from qz.remoterisk.cftc.limits.cftcemail import sendLMSExceptionMail, sendLMSLimitEmail
from qz.remoterisk.cftc.limits.utils import Timestamp, getRunDate

logger = logging.getLogger(name)

SCHEMANAME = 'limitsapi'
DIRECTORY = '/Risk/Market/Mrq/Domain'

_LIMIT_COL_TYPE = [
'string', 'string', 'string', 'string', 'double', 'string', 'string', 'string', 'string', 'string',
'string', 'string', 'string', 'string','string','string'
]
_LMS_LIMIT_COL_NAME = [
'Volcker Business Area', 'Volcker Trading Desk', 'Limit Code', 'Name', 'CurrLimit', 'Currency',
'Limit Status', 'Maturity Name', 'Product Type', 'Risk Factor Name', 'Significant Rating', 'Shift Name',
'Measure', 'Measure Unit', 'entity_id', 'entity_name'
]
_LIMIT_COL_NAME = [
'Volcker Business Area', 'Volcker Trading Desk', 'Limit Code', 'Limit Name', 'Limit Value', 'Currency',
'Status', 'Maturity Name', 'Product Type', 'Risk Factor Name', 'Significant Rating', 'Shift Name',
'Measure', 'Measure Unit', 'entity_id', 'entity_name'
]

#CFTC limit schema
LIMIT_SCHEMA = qztable.Schema(_LIMIT_COL_NAME, _LIMIT_COL_TYPE)

#LMS limit schema
LMS_LIMIT_SCHEMA = qztable.Schema(_LMS_LIMIT_COL_NAME, _LIMIT_COL_TYPE)

class CFTCLimits():
def __init__(self, config):
    '''Snapshots the limit value data(in Sandra) from LMS API at regular intervals

    :param str isProd : Flag for prod/non-prod
    '''
    self.limitDict = {}
    self.cftc = CFTCConf(config)
    self.dbName = self.cftc['db']
    self.dbPath = self.cftc['risk_limits_path']
    self.saveDB = sandra.connect(self.dbName)
    self.currentTime = Timestamp.now('UTC')
    self.batchDate = getRunDate(bobfns.batchDate)
    self.lmitDBPath = sandra.db.join(self.dbPath, self.currentTime.runDate)
    self.limitsDbName = self.cftc['limits_dbname']  
    self.authType = authentication.KERBEROS if isQzwin() else authentication.PKI
    self.envServiceLayer = self.cftc['limits_env_service_layer'] if isQzwin() else self.cftc['pki_limits_env_service_layer']
            
def start(self):
    '''
    Fetch limits data from LMS API, stores it sandra and send email in exceptional scenarios
    '''
    limitsTable = None
    limitsTable = self.fetchLMSApiData()
       
    if limitsTable:
        #Important pls read ######## dont update and append the same limit in two places, do either updation or appending
        #append
        limitsTable.append(['GLOBAL FINANCING & FUTURES', 'SHORT END TRADING', 'INTRA_FICC_s', 'INTRA_FICC_SHORT END TRADING_USD_RepoSpreadDelta', 5000000.0, 'USD', 'Active', ' ', ' ', ' ', ' ', '1', 'Repo Spread Delta', 'b.p.', '', ''])
        limitsTable.append(['COUNTERPARTY PORTFOLIO MANAGEMENT', 'GLPM', 'INTRA_FICC_GLMPShort', 'INTRA_FICC_GLPM_SHORT_CS01', 10000000 , '', 'Active', ' ', ' ', ' ', ' ', '', 'ShortCS01', '', '', ''])            
        limitsTable.append(['COUNTERPARTY PORTFOLIO MANAGEMENT', 'GLPM', 'INTRA_FICC_GLMPLong', 'INTRA_FICC_GLPM_LONG_CS01', 100000 , '', 'Active', ' ', ' ', ' ', ' ', '', 'LongCS01', '', '', ''])  
        limitsTable.append(['GLOBAL RATES', 'AMRS LINEAR RATES', 'INTRA_FICC_AMRS_LR_a', 'INTRA - GLOBAL RATES - AMRS LINEAR RATES - Sov Spread Delta', 5000000.0, 'USD', 'Active', ' ', ' ', ' ', ' ', '1', 'Sov Spread Delta', '', '', ''])
        
        ## pls remove the below limits once the limits are creted in LMS UAT
        limitsTable.append(['MORTGAGE PRODUCTS', 'AMERICAS ABS', 'INTRA_FICC_29hc', 'INTRA_FICC_AMERICAS ABS_CS01+1bp', 850000.000, '', 'Active', '', '', '', '', ' ', 'CS01', '', '', ''])
        limitsTable.append(['MORTGAGE PRODUCTS', 'AMERICAS ABS', 'INTRA_FICC_30hc', 'INTRA_FICC_AMERICAS ABS_IR01+1bp', 200000.000, '', 'Active', '', '', '', '', ' ', 'IR01', '', '', ''])
        
        lcs = limitsTable.colToList('Limit Code')
        
        #updation
        
        #if 'INTRA_FICC_237' in lcs:
            #idx = lcs.index('INTRA_FICC_237')
            #logger.info("Updating row %s" % idx)
            #limitsTable[idx]=('MUNICIPAL BANKING AND MARKETS', 'MBAM SECONDARY', 'INTRA_FICC_237', 'INTRA_FICC_MUNI SECONDARY_USD_CS01+1bp', 2000000.000, 'USD', 'Active', '', '', 'Credit Spread', '', '-1', 'CS01', 'b.p.', '8009245', 'MUNICIPAL BANKING AND MARKETS')

        if 'RATES0175' in lcs:
              idx = lcs.index('RATES0175')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'APAC LINEAR RATES', 'RATES0175', 'INTRA - MLI - GLOBAL RATES - APAC LINEAR - IR Delta - P1', 800, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '<None>', 'IR01', 'b.p.', '8022251', 'APAC LINEAR RATES')
        '''
        if 'INTRA_FICC_COMM403' in lcs:
              idx = lcs.index('INTRA_FICC_COMM403')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COMMODITIES', 'GLOBAL METALS', 'INTRA_FICC_COMM403', 'INTRA_FICC - GLOBAL METALS - COMMODITY VEGA - BASE METALS', 200000, 'USD', 'Active', '', '<None>', 'Commodity', '', '<None>', 'Vega', 'b.p.', '8026631', 'BASE METALS')
        if 'INTRA_FICC_COMM525' in lcs:
              idx = lcs.index('INTRA_FICC_COMM525')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COMMODITIES', 'GLOBAL METALS', 'INTRA_FICC_COMM525', 'INTRA_FICC - GLOBAL METALS - COMMODITY DELTA - BASE METALS_New', 25000.0, 'USD', 'Active', '', '', 'Commodity', '', 'shift', 'Delta', 'b.p.', '8026631', 'BASE METALS')
        if 'INTRA_FICC_97' in lcs:
              idx = lcs.index('INTRA_FICC_97')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'INTRA_FICC_97', 'INTRA - GLOBAL RATES - AMRS LINEAR - IR Delta - P1', 2750000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', ' ', '1', 'IR01', 'b.p.', '8022229', 'AMRS LINEAR RATES')
        if 'INTRA_FICC_98' in lcs:
              idx = lcs.index('INTRA_FICC_98')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'INTRA_FICC_98', 'INTRA - GLOBAL RATES - AMRS LINEAR - IR Vega - P1% rel.', 700000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', ' ', '1', 'Vega', '%', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0159' in lcs:
              idx = lcs.index('RATES0159')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0159', 'INTRA - BANA - GLOBAL RATES - AMRS LINEAR - IR Delta - P1', 2750000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'IR01', 'b.p.', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0160' in lcs:
              idx = lcs.index('RATES0160')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0160', 'INTRA - BANA - GLOBAL RATES - AMRS LINEAR - IR Vega - P1% rel.', 700000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'Vega', '%', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0171' in lcs:
              idx = lcs.index('RATES0171')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0171', 'INTRA - MLI - GLOBAL RATES - AMRS LINEAR - IR Delta - P1', 2750000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'IR01', 'b.p.', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0172' in lcs:
              idx = lcs.index('RATES0172')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0172', 'INTRA - MLI - GLOBAL RATES - AMRS LINEAR - IR Vega - P1% rel.', 700000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'Vega', '%', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0183' in lcs:
              idx = lcs.index('RATES0183')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0183', 'INTRA - BofAS - GLOBAL RATES - AMRS LINEAR - IR Delta - P1', 2750000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'IR01', 'b.p.', '8022229', 'AMRS LINEAR RATES')
        if 'RATES0184' in lcs:
              idx = lcs.index('RATES0184')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('GLOBAL RATES', 'AMRS LINEAR RATES', 'RATES0184', 'INTRA - BofAS - GLOBAL RATES - AMRS LINEAR - IR Vega - P1% rel.', 700000.0, '', 'Active', '<None>', '<None>', 'Interest Rate', '', '', 'Vega', '%', '8022229', 'AMRS LINEAR RATES')
        if 'INTRA_FICC_COMM523' in lcs:
            idx = lcs.index('INTRA_FICC_COMM523')
            logger.info("Updating row %s" % idx)
            limitsTable[idx]=('COMMODITIES', 'GLOBAL METALS', 'INTRA_FICC_COMM523', 'INTRA_FICC - GLOBAL METALS - COMMODITY DELTA - PRECIOUS METALS', 20900.0, 'USD', 'Active', '<None>', '', 'Commodity', '', '1', 'Delta', 'b.p.', '8026611', 'COMMODITIES METALS')
        
        if 'INTRA_FICC_157' in lcs:
              idx = lcs.index('INTRA_FICC_157')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COUNTERPARTY PORTFOLIO MANAGEMENT', 'XVA END-USER', 'INTRA_FICC_157', 'INTRA - CPM - XVA-EU - IR Delta - P1 - EUR', 300000.0, 'EUR', 'Retired', '<None>', '<None>', '<None>', ' ', '1', 'IR01', 'b.p.', '8022859', 'XVA END-USER')
        if 'INTRA_FICC_159' in lcs:
              idx = lcs.index('INTRA_FICC_159')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COUNTERPARTY PORTFOLIO MANAGEMENT', 'XVA END-USER', 'INTRA_FICC_159', 'INTRA - CPM - XVA-EU - IR Delta - P1 - GBP', 200000.0, 'GBP', 'Retired', '<None>', '<None>', '<None>', ' ', '1', 'IR01', 'b.p.', '8022859', 'XVA END-USER')
        if 'INTRA_FICC_161' in lcs:
              idx = lcs.index('INTRA_FICC_161')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COUNTERPARTY PORTFOLIO MANAGEMENT', 'XVA END-USER', 'INTRA_FICC_161', 'INTRA - CPM - XVA-EU - IR Delta - P1 - JPY', 100000.0, 'JPY', 'Retired', '<None>', '<None>', '<None>', ' ', '1', 'IR01', 'b.p.', '8022859', 'XVA END-USER')
        if 'INTRA_FICC_164' in lcs:
              idx = lcs.index('INTRA_FICC_164')
              logger.info("Updating row %s" % idx)
              limitsTable[idx]=('COUNTERPARTY PORTFOLIO MANAGEMENT', 'XVA END-USER', 'INTRA_FICC_164', 'INTRA - CPM - XVA-EU - IR Delta - P1 - USD', 50000.0, 'USD', 'Retired', '<None>', '<None>', '<None>', ' ', '1', 'IR01', 'b.p.', '8022859', 'XVA END-USER')
       
        '''
        # #Dont delete this - start - by sangeetha since cs01 & IR01 are not going live this year updating vtd name as americas abs1
        # if 'INTRA_FICC_29' in lcs:
        #    idx = lcs.index('INTRA_FICC_29')
        #    logger.info("Updating row %s" % idx)
        #    limitsTable[idx]=('MORTGAGE PRODUCTS', 'AMERICAS ABS', 'INTRA_FICC_29', 'INTRA_FICC_AMERICAS ABS_CS01+1bp', 850000.000, '', 'Active', '', '', '', '', ' ', 'CS01', '', '', '')

        # if 'INTRA_FICC_30' in lcs:
        #    idx = lcs.index('INTRA_FICC_30')
        #    logger.info("Updating row %s" % idx)
        #    limitsTable[idx]=('MORTGAGE PRODUCTS', 'AMERICAS ABS', 'INTRA_FICC_30', 'INTRA_FICC_AMERICAS ABS_IR01+1bp', 200000.000, '', 'Active', '', '', '', '', ' ', 'IR01', '', '', '')
        # #Dont delete this - end- by sangeetha since cs01 & IR01 are not going live this year updating vtd name as americas abs1            

        limitPaths = self.fetchPreviousSnap(self.currentTime.runDate)
        if not limitPaths:
            yesterday = subtractBusinessDays(self.currentTime, 1)
            yesterday = getRunDate(yesterday)
            limitPaths =self.fetchPreviousSnap(yesterday)  
        if limitPaths:
            limitPaths = limitPaths.ls()
            limitPaths.sort(reverse=True)
            limitObj = self.saveDB.readobj(limitPaths[0])
            prevContents = limitObj.contents
            limitDiff, newCols, removedCols  = self.limitDifference(prevContents, limitsTable)
            newCols = ", ".join(newCols)
            removedCols = ", ".join(removedCols)
            if limitDiff or removedCols or newCols:
                sendLMSLimitEmail(self.currentTime.cobDate, limitDiff.inMemCopy(), newCols, removedCols, self.cftc)
                logger.info("Difference in Snaps:%s",limitDiff)
        self.writeLimits(limitsTable)
        return

def fetchLMSApiData(self):
    '''
    Makes a call to LMS API, and converts the response to QZTable
    
    :return: qztable.Table
    '''
    args = {
        "cob_start" : self.batchDate,
        "cob_end": self.batchDate,
        "Source Feed Name" : 'LMS_FICC_FO',
        "Limit Status" : 'Active, Retired',
        "show_attributes" : ','.join([','.join(_LMS_LIMIT_COL_NAME),'Source Feed Name']),
     }
    
    try:
        url = self.envServiceLayer +'/catalog/limits?' + urllib.parse.urlencode(args)
        logger.info("Limits URL:%s",url)
        limitsResponse = Rest.request(url, authentication_type=self.authType, get_method=lambda: 'GET')
        limitsTable = tableFromListOfDicts(limitsResponse)
        if limitsTable.nRows()>1: # Check data availability from new LMS api
            limitsTable = self.mapLimitsSchema(limitsTable, LMS_LIMIT_SCHEMA)
        logger.info("Limits Data returned from New LMS Limits API")
        return limitsTable
    except Exception as e:
        sendLMSExceptionMail(self.currentTime.cobDate, self.cftc, 'NewLMSAPIFailure')
        logger.info('Exception occured while trying to fetch Limits.')
        logger.error(e)
        return
    
    
def mapLimitsSchema(self, limitsTable, sourceLimitsSchema):
    '''
    Performs data massaging for the limits table
    
    :param qztable limitsTable : LMS Limits Table
    :return: qztable.Table
    '''
    limitsTable = limitsTable.mapToSchema(sourceLimitsSchema).inMemCopy()
    limitsTable = limitsTable.convertSchema(LIMIT_SCHEMA).inMemCopy()
    return limitsTable

def fetchPreviousSnap(self,snapDate):
    '''
    To fetch previous snap sandra path
    
    :param str snapDate : Current Snap Date 
    '''
    prevSnapPath = sandra.db.join(self.dbPath, snapDate)
    limitPaths =self.saveDB.readobj(prevSnapPath)
    return limitPaths        
    
def addLimitDiffCategories(self, prevDiff, currDiff):
    
    '''
    Compares the limits values data from previous snap and current snap version stored in sandra
    
    :param qztable prevDiff  : Previous snap difference limits contents
    :param qztable currDiff  : Current snap difference limits contents
    :return qztable limitsTable : limitsDifference data
    '''
    limitDiffColNames = list(prevDiff.columnNames())
    limitDiffColTypes = list(prevDiff.columnTypes())
    limitDiff = qztable.Table(qztable.Schema(limitDiffColNames+['Column Update Status','ModifiedCols'],limitDiffColTypes+['string','string']))
    # Iterating over Previous snap difference, to highlight limit column changes and limits which are being removed
    for row in prevDiff.toList():
        lc = row[prevDiff.columnIndex('Limit Code')]
        
        if lc in currDiff.colToList('Limit Code'):
            currDiffrow = currDiff[currDiff['Limit Code']==lc][0]
            currDiffrow = list(currDiffrow)
            diffIdx = []
            for i in range(len(row)):
                if row[i] != currDiffrow[i]:
                    diffIdx.append(str(i))
            #Adding prev diff row and current diff row to create limitDiff table                        
            limitDiff.append(row+['Modified',','.join(diffIdx)])
            limitDiff.append(currDiffrow+['Modified',','.join(diffIdx)])
            
        else :
             limitDiff.append(row+['Limit Removed',''])
    # Iterating over current snap difference, to highlight new limit addition
    for row in currDiff.toList():
       lc = row[currDiff.columnIndex('Limit Code')]
       if lc not in prevDiff.colToList('Limit Code'):
           limitDiff.append(row+['New Limit',''])
           
    return limitDiff     
           
def limitDifference(self,prevContents,limitsTable):
    '''
    Compares the limits values data from LMS API with the version stored in sandra
    
    :param dict prevContents  : Previous snap time limits contents
    :param qztable limitsTable : Current snap time limits data
    '''
    prevLimits = prevContents.get('Limits')
    newCols = list(set(limitsTable.columnNames()) - set(prevLimits.columnNames()))
    removedCols = list(set(prevLimits.columnNames())- set(limitsTable.columnNames()))
    
    prevLimits = prevLimits.mapToSchema(qztable.Schema(list(limitsTable.columnNames()),list(limitsTable.columnTypes())))
    prevCmpLimits = prevLimits.extendConst(prevContents.get('COBDate'), 'COBDate', 'string')
    prevCmpLimits = prevCmpLimits.extendConst(prevContents.get('SnapTime'), 'SnapTime', 'string')
    currCmpLimits = limitsTable.extendConst(self.currentTime.cobDate, 'COBDate', 'string')
    currCmpLimits = currCmpLimits.extendConst(self.currentTime.snapTime, 'SnapTime', 'string')
    
    prevDiff = prevCmpLimits.antiJoin(currCmpLimits, limitsTable.columnNames())
    currDiff = currCmpLimits.antiJoin(prevCmpLimits, limitsTable.columnNames())
    limitDiff = self.addLimitDiffCategories(prevDiff, currDiff)      
    return limitDiff, newCols, removedCols

def writeLimits(self,limitsTable):
    '''
    Write limits data from LMS API to sandra
    
    :param qztable limitsTable : Current snap time limits data
    '''
    self.limitDict['COBDate'] = self.currentTime.cobDate
    self.limitDict['SnapTime'] = self.currentTime.snapTime
    self.limitDict['Limits'] = limitsTable
    self.saveDB.mkdir(self.lmitDBPath)
    self.path = self.lmitDBPath+ '/'+ self.currentTime.sandraRunHour
    obj = self.saveDB.read_or_new("Container", self.path)
    obj.contents = self.limitDict
    obj.write()
    logger.info('Limits Written to sandra')
def run(config='uat_common'):
'''
Entry point to store limit data.
:param str config : yaml config name 
'''
cftc = CFTCLimits(config)
cftc.start()
def main():
logging.compliance(name, "Bob Run", action=logging.Action.ENTRYPOINT)
bobfns.run(run)