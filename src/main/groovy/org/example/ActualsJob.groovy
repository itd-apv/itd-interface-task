import de.itdesign.apv.ActualsImportFromCSV
import de.itdesign.clarity.logging.CommonLogger
import de.itdesign.interfaces.cmn.dto.enums.InterfaceStatusEnum
import de.itdesign.interfaces.shared.actuals.ActualsInterfaceProcessor
import de.itdesign.interfaces.utils.rest.CommonRestUtils

import groovy.transform.Field

import java.sql.Connection

if (!['connection', 'configInstanceCode'].collect { assertInput(it) }.every()) {
    throw new Exception("Input Parameter is missing!")
}

@Field CommonLogger cmnLog = new CommonLogger(this)
cmnLog.setFailJobOnError(true)

boolean assertInput(String input) {
    binding.variables.containsKey(input) && binding.variables.get(input)
}

def runScript() {
    cmnLog.info "Execution Starts at ${new Date()}"

    ActualsInterfaceProcessor actualsInterfaceProcessor
    try {
        connection.autoCommit = true

        CommonRestUtils restUtils = new CommonRestUtils("admin", cmnLog)
        ActualsImportFromCSV actualsImport = new ActualsImportFromCSV(
                binding.variables.get('connection') as Connection
                , cmnLog
                , restUtils
                , binding.variables.get('configInstanceCode') as String
        )
        actualsInterfaceProcessor = actualsImport.actualsInterfaceProcessor

        List<Integer> masterInstanceDbIds = actualsImport.importSrcDataIntoStg()
        masterInstanceDbIds.each { Integer masterInstanceDbId ->
            actualsInterfaceProcessor.ctx.masterInstanceDbId = masterInstanceDbId
            try {
                actualsImport.validateAndTransform()
                actualsImport.writeToTarget()
                actualsInterfaceProcessor.ctx.stagingObjectService.setMasterObjectStatus(InterfaceStatusEnum.PROCESSED)
            } catch (Exception exception) {
                actualsInterfaceProcessor.ctx.stagingObjectService.setMasterObjectStatusAndMessage(InterfaceStatusEnum.FAILED, exception.asString())
                cmnLog.error exception.asString()
            }
        }

        cmnLog.info "Execution Finishes at ${new Date()}"
    } catch (Exception exception) {
        cmnLog.error exception.asString()
    } finally {
        actualsInterfaceProcessor?.close()
    }
}

runScript()
